import time,logging,redis
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.dataset import Dataset
from src.models.trace import Trace
from src.services.operators import execute
from src.services import purge
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
def process_trace(run_id):
 with Session() as s:
  row=s.get(Trace,run_id)
  if not row or row.status!='queued':return
  # 清除受理后排队中的旧执行直接终止，不得启动
  if row.purge_job_id is not None:
   row.status='failed';row.error=purge.TERMINATED_ERROR;s.commit();return
  row.status='running';snapshot=row.snapshot;s.commit()
 try:
  result=execute(snapshot['dataset'],snapshot['plan']['steps'])
  with Session() as s:
   r=s.get(Trace,run_id)
   # 并发运行的旧执行即使晚完成也不能写回：清除标记或数据集已屏蔽即丢弃结果
   if r is None or r.purge_job_id is not None or s.get(Dataset,r.dataset_id).purge_status!='active':
    logging.warning('trace %s writeback rejected by purge guard',run_id);return
   r.result=result;r.status='completed';s.commit()
  log('worker','trace.completed',str(run_id))
 except Exception as exc:
  logging.exception('trace failed')
  with Session() as s:
   r=s.get(Trace,run_id)
   if r is None or r.purge_job_id is not None:return
   r.status='failed';r.error=str(exc)[:1000];s.commit()
def process_purge(job_id):purge.run_purge_job(job_id)
def main():
 try:purge.recover_orphans(q)
 except Exception:logging.exception('purge recovery on startup failed')
 while True:
  Path('/tmp/worker-heartbeat').touch()
  try:
   item=q.blpop(['operator-traces','purge-jobs'],timeout=2)
   if not item:continue
   key,payload=item[0].decode() if isinstance(item[0],bytes) else item[0],item[1]
   item_id=int(payload)
   if key=='purge-jobs':process_purge(item_id)
   else:process_trace(item_id)
  except Exception:logging.exception('worker loop');time.sleep(2)
if __name__=='__main__':main()
