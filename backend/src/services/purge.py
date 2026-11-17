import logging
import redis
from fastapi import HTTPException
from sqlalchemy import update
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.dataset import Dataset
from src.models.trace import Trace
from src.models.purge import PurgeJob
from src.services.audit import log
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
PURGED_SNAPSHOT={'purged':True}
TERMINATED_ERROR='数据集已彻底清除，执行已终止'
def _masked_trace(row):
 data=record(row)
 # 受理清除后立即屏蔽历史执行的数据、预览与导出，不等物理清理完成
 if row.purge_job_id is not None:
  data['snapshot']=PURGED_SNAPSHOT
  data['result']=None
 return data
def mask_dataset(row):
 data=record(row)
 if row.purge_status!='active':
  # 名称、描述与关联计划保留供追溯；表数据在接口层立即不可见
  data['sensors']=[]
  data['readings']=[]
 return data
def list_traces():
 with Session() as s:return [_masked_trace(r) for r in s.query(Trace).order_by(Trace.id.desc()).limit(100).all()]
def get_trace(item_id):
 with Session() as s:
  row=s.get(Trace,item_id)
  if not row:raise HTTPException(404,'未找到执行轨迹')
  return _masked_trace(row)
def list_datasets():
 with Session() as s:return [mask_dataset(r) for r in s.query(Dataset).order_by(Dataset.id.desc()).all()]
def ensure_editable(s,item_id):
 row=s.get(Dataset,item_id)
 if not row:raise HTTPException(404,'未找到记录')
 if row.purge_status!='active':raise HTTPException(409,'数据集已屏蔽，拒绝编辑；仅保留名称与关联记录供追溯')
 return row
def request_purge(dataset_id,actor):
 with Session() as s:
  row=s.query(Dataset).filter_by(id=dataset_id).with_for_update().first()
  if not row:raise HTTPException(404,'未找到数据集')
  if row.purge_status=='purging':raise HTTPException(409,'数据集正在彻底清除中，请勿重复提交')
  if row.purge_status=='purged':raise HTTPException(409,'数据集已彻底清除')
  if row.purge_status=='purge_failed':
   # 复用原任务：中断/失败重试不重复建任务
   job=s.query(PurgeJob).filter_by(dataset_id=dataset_id).order_by(PurgeJob.id.desc()).first()
   if not job:raise HTTPException(409,'清除任务缺失，无法重试')
   job.status='queued';job.error=None
   row.purge_status='purging';row.purge_error=None
   action='dataset.purge.retry'
  else:
   job=PurgeJob(dataset_id=row.id,status='queued',attempts=0,created_by=actor)
   s.add(job);s.flush()
   # 受理即屏蔽全部历史执行（含排队/运行中），worker 凭此标记拒绝晚到写回
   s.execute(update(Trace).where(Trace.dataset_id==row.id).values(purge_job_id=job.id))
   row.purge_status='purging';row.purge_error=None
   action='dataset.purge.request'
  s.commit();s.refresh(job);payload=record(job);job_id=job.id
 try:
  queue.rpush('purge-jobs',job_id)
 except redis.RedisError:
  with Session() as s:
   j=s.get(PurgeJob,job_id)
   if j and j.status=='queued':
    j.status='failed';j.error='队列不可用，请重试清除'
    d=s.get(Dataset,dataset_id);d.purge_status='purge_failed';d.purge_error=j.error;s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 log(actor,action,str(job_id))
 return payload
def _claim(job_id):
 # 抢占任务并记录一次尝试；崩溃中断后重试可再次抢占，attempts 如实累计
 with Session() as s:
  job=s.query(PurgeJob).filter_by(id=job_id).with_for_update().first()
  if not job or job.status!='queued':return None
  job.status='running';job.attempts=PurgeJob.attempts+1;job.error=None
  dataset_id=job.dataset_id;s.commit()
 return dataset_id
def _cleanup(dataset_id,job_id):
 # 每一步独立事务提交，worker 中断后重试可从已提交进度继续，且全部操作幂等
 with Session() as s:
  dataset=s.get(Dataset,dataset_id)
  # 1. 清空原表（JSON 列置空数组），保留名称、描述
  dataset.sensors=[];dataset.readings=[]
  # 2. 清空执行快照与结果；排队/运行中的轨迹一并终止，杜绝晚到写回残留
  traces=s.query(Trace).filter_by(dataset_id=dataset_id).all()
  for t in traces:
   t.snapshot=PURGED_SNAPSHOT;t.result=None
   if t.status in ('queued','running'):t.status='failed';t.error=TERMINATED_ERROR
  s.commit()
 with Session() as s:
  # 3. 全部清空核验通过才标记已清除；任一不符都抛错进入 failed 且保持屏蔽
  dataset=s.get(Dataset,dataset_id)
  if dataset.sensors or dataset.readings:raise RuntimeError('原表清空校验未通过')
  traces=s.query(Trace).filter_by(dataset_id=dataset_id).all()
  leftovers=[t for t in traces if t.result is not None or t.snapshot!=PURGED_SNAPSHOT]
  if leftovers:raise RuntimeError('执行快照或结果清空校验未通过')
  job=s.get(PurgeJob,job_id);job.status='succeeded';job.error=None
  dataset.purge_status='purged';dataset.purge_error=None
  s.commit()
def run_purge_job(job_id):
 try:
  dataset_id=_claim(job_id)
  if dataset_id is None:return False
  _cleanup(dataset_id,job_id)
  log('worker','dataset.purge.completed',str(job_id))
  return True
 except Exception as exc:
  logging.exception('purge job failed')
  with Session() as s:
   job=s.get(PurgeJob,job_id)
   if job and job.status!='succeeded':
    job.status='failed';job.error=str(exc)[:1000]
    dataset=s.get(Dataset,job.dataset_id)
    # 失败保持屏蔽，允许重试
    dataset.purge_status='purge_failed';dataset.purge_error=job.error
    s.commit()
  log('worker','dataset.purge.failed',str(job_id))
  return False
def recover_orphans(client=None):
 # worker 启动恢复：崩溃中断的 running 任务重新排队，丢失消息的 queued 任务重新入队，不新建任务
 client=client or queue
 with Session() as s:
  jobs=s.query(PurgeJob).filter(PurgeJob.status.in_(('queued','running'))).all()
  for job in jobs:
   if job.status=='running':job.status='queued';job.error=None
  s.commit();ids=[j.id for j in jobs]
 for job_id in ids:client.rpush('purge-jobs',job_id)
 return ids
