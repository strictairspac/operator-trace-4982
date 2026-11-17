import pytest
from src.config.database import Session
from src.models.dataset import Dataset
from src.models.trace import Trace
from src.models.purge import PurgeJob
from src.services import purge as purge_service
import src.worker as worker_mod
PURGED={'purged':True}
def purge_queue_ids():return [int(x) for x in purge_service.queue.lrange('purge-jobs',0,-1)]
def trace_queue_ids():return [int(x) for x in purge_service.queue.lrange('operator-traces',0,-1)]
def flush_queues():purge_service.queue.flushall()
def get_dataset(did):
 with Session() as s:return s.get(Dataset,did)
def get_trace(tid):
 with Session() as s:return s.get(Trace,tid)
def get_job(jid):
 with Session() as s:return s.get(PurgeJob,jid)
def run_all_purge_jobs():
 ids=purge_queue_ids()
 for jid in ids:worker_mod.process_purge(jid)
def test_viewer_cannot_purge(client,auth,make_dataset):
 did=make_dataset()
 r=client.post(f'/datasets/{did}/purge',headers=auth('viewer'))
 assert r.status_code==403
 d=get_dataset(did)
 assert d.purge_status=='active' and d.sensors and d.readings
def test_accept_masks_immediately_and_other_datasets_unaffected(client,auth,make_dataset,make_plan,make_trace):
 flush_queues()
 d1=make_dataset('待清除集')
 d2=make_dataset('正常集')
 p1=make_plan(d1);p2=make_plan(d2)
 t1=make_trace(p1);t2=make_trace(p2)
 r=client.post(f'/datasets/{d1}/purge',headers=auth('operator'))
 assert r.status_code==200
 job=r.json();assert job['status']=='queued' and job['attempts']==0
 assert purge_queue_ids()==[job['id']]
 # 受理后立即屏蔽：列表层原表不可见，但名称保留
 listed={x['id']:x for x in client.get('/datasets',headers=auth('viewer')).json()}
 assert listed[d1]['sensors']==[] and listed[d1]['readings']==[] and listed[d1]['name']=='待清除集'
 assert listed[d1]['purge_status']=='purging'
 assert listed[d2]['sensors'] and listed[d2]['readings'] and listed[d2]['purge_status']=='active'
 # 历史执行的数据/预览/导出立即屏蔽
 tr=client.get(f'/traces/{t1}',headers=auth('viewer')).json()
 assert tr['snapshot']==PURGED and tr['result'] is None
 exp=client.get(f'/traces/{t1}/export',headers=auth('viewer'))
 assert exp.status_code==200 and '"purged": true' in exp.text and 'readings' not in exp.text
 # 同数据集另一条历史执行同样屏蔽；其他数据集照常
 other=client.get(f'/traces/{t2}',headers=auth('viewer')).json()
 assert other['snapshot']!=PURGED and other['snapshot'].get('dataset')
 # 拒绝编辑与新执行
 r=client.put(f'/datasets/{d1}',json={'name':'待清除集','description':'x','sensors':[],'readings':[]},headers=auth('operator'))
 assert r.status_code==409
 r=client.post('/traces',json={'name':'新执行','plan_id':p1},headers=auth('operator'))
 assert r.status_code==409
 r=client.post('/plans',json={'name':'屏蔽集新计划','dataset_id':d1,'steps':[]},headers=auth('operator'))
 assert r.status_code==422 or r.status_code==409
 # 其他数据集照常工作
 ok=client.post('/traces',json={'name':'正常集新执行','plan_id':p2},headers=auth('operator'))
 assert ok.status_code==200
def test_queued_trace_terminated_when_worker_picks_it(client,auth,make_dataset,make_plan,make_trace):
 flush_queues()
 did=make_dataset();pid=make_plan(did);tid=make_trace(pid)
 client.post(f'/datasets/{did}/purge',headers=auth('admin'))
 # 排队中的旧执行被 worker 取到时直接终止，不执行算子
 worker_mod.process_trace(tid)
 t=get_trace(tid)
 assert t.status=='failed' and t.purge_job_id is not None
 assert t.result is None and '已彻底清除' in (t.error or '')
 run_all_purge_jobs()
 t=get_trace(tid);d=get_dataset(did)
 assert t.status=='failed' and '已彻底清除' in t.error
 assert t.snapshot==PURGED and t.result is None
 assert d.purge_status=='purged' and d.sensors==[] and d.readings==[]
def test_running_trace_late_writeback_rejected(client,auth,make_dataset,make_plan,make_trace):
 flush_queues()
 did=make_dataset();pid=make_plan(did);tid=make_trace(pid)
 # 模拟 worker 已将任务置为 running（快照在内存中），此时清除才受理
 with Session() as s:
  t=s.get(Trace,tid);t.status='running';s.commit()
 client.post(f'/datasets/{did}/purge',headers=auth('operator'))
 # 旧执行晚完成：写回必须被拒绝
 worker_mod.process_trace(tid)
 t=get_trace(tid)
 assert t.result is None and t.purge_job_id is not None
 # 异常路径同样不得写回
 worker_mod.process_trace(tid)
 assert get_trace(tid).result is None
 run_all_purge_jobs()
 assert get_dataset(did).purge_status=='purged'
def test_full_purge_success_marks_purged_and_keeps_name_and_plans(client,auth,make_dataset,make_plan,make_trace):
 flush_queues()
 did=make_dataset('保留名称集');pid=make_plan(did);tid=make_trace(pid)
 worker_mod.process_trace(tid)
 assert get_trace(tid).status=='completed' and get_trace(tid).result is not None
 r=client.post(f'/datasets/{did}/purge',headers=auth('admin'))
 jid=r.json()['id']
 run_all_purge_jobs()
 d=get_dataset(did);t=get_trace(tid);job=get_job(jid)
 assert d.purge_status=='purged' and d.purge_error is None
 assert d.name=='保留名称集' and d.description=='测试数据集'
 assert d.sensors==[] and d.readings==[]
 assert t.snapshot==PURGED and t.result is None
 assert job.status=='succeeded' and job.attempts==1
 # 关联计划仍在
 plans=client.get('/plans',headers=auth('viewer')).json()
 assert any(p['id']==pid for p in plans)
 # 已清除后再次请求返回 409，不新建任务
 again=client.post(f'/datasets/{did}/purge',headers=auth('operator'))
 assert again.status_code==409
 with Session() as s:assert s.query(PurgeJob).filter_by(dataset_id=did).count()==1
def test_purge_failure_keeps_masked_and_retry_succeeds_without_new_job(client,auth,make_dataset,make_plan,make_trace,monkeypatch):
 flush_queues()
 did=make_dataset();pid=make_plan(did);tid=make_trace(pid)
 r=client.post(f'/datasets/{did}/purge',headers=auth('operator'))
 jid=r.json()['id']
 # 注入失败：模拟清理中断（进程崩溃发生在清空之后、标记成功之前）
 from src.services import purge as ps
 def boom(dataset_id,job_id):raise RuntimeError('模拟清理中断')
 monkeypatch.setattr(ps,'_cleanup',boom)
 worker_mod.process_purge(jid)
 d=get_dataset(did);job=get_job(jid)
 assert d.purge_status=='purge_failed' and d.purge_error and '模拟清理中断' in d.purge_error
 assert job.status=='failed' and job.attempts==1
 # 失败期间仍屏蔽
 listed={x['id']:x for x in client.get('/datasets',headers=auth('viewer')).json()}
 assert listed[did]['sensors']==[] and listed[did]['purge_status']=='purge_failed'
 assert client.put(f'/datasets/{did}',json={'name':'xx','description':'x','sensors':[],'readings':[]},headers=auth('operator')).status_code==409
 monkeypatch.undo()
 # 重试：复用同一任务，不重复建；先模拟中断（running 卡住），再走重试接口
 retry=client.post(f'/datasets/{did}/purge',headers=auth('operator'))
 assert retry.status_code==200 and retry.json()['id']==jid
 with Session() as s:assert s.query(PurgeJob).filter_by(dataset_id=did).count()==1
 run_all_purge_jobs()
 job=get_job(jid);d=get_dataset(did);t=get_trace(tid)
 assert job.status=='succeeded' and job.attempts==2
 assert d.purge_status=='purged'
 assert t.snapshot==PURGED and t.result is None
def test_orphan_recovery_requeues_interrupted_running_job(make_dataset):
 flush_queues()
 did=make_dataset()
 with Session() as s:
  job=PurgeJob(dataset_id=did,status='running',attempts=1,created_by='worker')
  s.add(job);s.commit();jid=job.id
  d=s.get(Dataset,did);d.purge_status='purging';s.commit()
 ids=purge_service.recover_orphans()
 assert jid in ids and purge_queue_ids()==[jid]
 job=get_job(jid)
 assert job.status=='queued'
 worker_mod.process_purge(jid)
 assert get_job(jid).status=='succeeded'
 assert get_dataset(did).purge_status=='purged'
def test_duplicate_request_while_purging_returns_conflict_and_single_job(client,auth,make_dataset):
 flush_queues()
 did=make_dataset()
 first=client.post(f'/datasets/{did}/purge',headers=auth('operator'))
 assert first.status_code==200
 second=client.post(f'/datasets/{did}/purge',headers=auth('admin'))
 assert second.status_code==409
 with Session() as s:assert s.query(PurgeJob).filter_by(dataset_id=did).count()==1
 assert purge_queue_ids()==[first.json()['id']]
def test_queue_unavailable_fails_closed_and_allows_retry(client,auth,make_dataset,monkeypatch):
 flush_queues()
 did=make_dataset()
 class DeadRedis:
  def rpush(self,*a):import redis;raise redis.RedisError('down')
 monkeypatch.setattr(purge_service,'queue',DeadRedis())
 r=client.post(f'/datasets/{did}/purge',headers=auth('operator'))
 assert r.status_code==503
 d=get_dataset(did)
 assert d.purge_status=='purge_failed'
 with Session() as s:
  job=s.query(PurgeJob).filter_by(dataset_id=did).one()
  assert job.status=='failed'
 monkeypatch.undo()
 # 恢复后重试同一任务成功
 retry=client.post(f'/datasets/{did}/purge',headers=auth('operator'))
 assert retry.status_code==200
 run_all_purge_jobs()
 assert get_dataset(did).purge_status=='purged'
def test_worker_main_loop_dispatches_both_queues(make_dataset,make_plan,make_trace,monkeypatch):
 flush_queues()
 did=make_dataset();pid=make_plan(did);tid=make_trace(pid)
 calls=[]
 monkeypatch.setattr(worker_mod.q,'blpop',lambda keys,timeout:None)
 # 依次弹出一个执行任务、一个清除任务后空转
 seq=[(b'operator-traces',str(tid).encode()),(b'purge-jobs',b'0'),None]
 def fake_blpop(keys,timeout):
  item=seq.pop(0)
  return item
 monkeypatch.setattr(worker_mod.q,'blpop',fake_blpop)
 monkeypatch.setattr(worker_mod,'process_trace',lambda i:calls.append(('trace',i)))
 monkeypatch.setattr(worker_mod,'process_purge',lambda i:calls.append(('purge',i)))
 monkeypatch.setattr(worker_mod.time,'sleep',lambda s:None)
 from src.services import purge as ps
 monkeypatch.setattr(ps,'recover_orphans',lambda:[])
 try:
  # main 循环在第三次 blpop 返回 None 后继续；用有限调用模拟：直接跑一次迭代体
  item=worker_mod.q.blpop(['operator-traces','purge-jobs'],2)
  key=item[0].decode();worker_mod.process_trace(int(item[1])) if key!='purge-jobs' else worker_mod.process_purge(int(item[1]))
  item=worker_mod.q.blpop(['operator-traces','purge-jobs'],2)
  key=item[0].decode();worker_mod.process_purge(int(item[1])) if key=='purge-jobs' else worker_mod.process_trace(int(item[1]))
 except Exception as e:
  pytest.fail('dispatch failed: %r'%e)
 assert calls==[('trace',tid),('purge',0)]
def test_operator_can_purge_and_audit_logged(client,auth,make_dataset):
 flush_queues()
 did=make_dataset()
 r=client.post(f'/datasets/{did}/purge',headers=auth('operator'))
 assert r.status_code==200
 from src.models.identity import Audit
 with Session() as s:
  entries=s.query(Audit).filter(Audit.action.like('dataset.purge%')).all()
  assert any(e.action=='dataset.purge.request' and e.detail==str(r.json()['id']) for e in entries)
 run_all_purge_jobs()
 with Session() as s:
  assert any(e.action=='dataset.purge.completed' for e in s.query(Audit).all())
def test_retry_reuses_job_when_worker_crashed_mid_cleanup(client,auth,make_dataset,make_plan,make_trace):
 flush_queues()
 did=make_dataset();pid=make_plan(did);tid=make_trace(pid)
 jid=client.post(f'/datasets/{did}/purge',headers=auth('admin')).json()['id']
 # 抢占成功（attempts=1，状态 running）后模拟进程崩溃，未做任何清理
 assert worker_mod.purge._claim(jid)==did
 assert get_job(jid).status=='running' and get_job(jid).attempts==1
 # worker 重启恢复：running 回 queued 并入队，不新建任务（at-least-once，重复消息由 claim 幂等拦截）
 ids=purge_service.recover_orphans()
 assert ids==[jid] and jid in purge_queue_ids()
 # worker 重启恢复：running 回 queued 并入队，不新建任务
 worker_mod.process_purge(jid)
 # 队列中重复消息在任务 succeeded 后被 claim 丢弃，不会重复清理
 for stale in purge_queue_ids():worker_mod.process_purge(stale)
 job=get_job(jid)
 assert job.status=='succeeded' and job.attempts==2
 assert get_dataset(did).purge_status=='purged'
 with Session() as s:assert s.query(PurgeJob).filter_by(dataset_id=did).count()==1
