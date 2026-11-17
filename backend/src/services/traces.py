import redis
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.services import purge
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
def list_items():return purge.list_traces()
def detail(item_id):return purge.get_trace(item_id)
def create(data):
 with Session() as s:
  p=s.get(Plan,data.plan_id)
  if not p:raise HTTPException(404,'未找到算子计划')
  d=s.get(Dataset,p.dataset_id)
  if d.purge_status!='active':raise HTTPException(409,'数据集已屏蔽，拒绝新执行')
  row=Trace(name=data.name,plan_id=p.id,dataset_id=d.id,status='queued',snapshot={'dataset':record(d),'plan':record(p)})
  s.add(row);s.commit();s.refresh(row);result=record(row)
 try:queue.rpush('operator-traces',result['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(Trace,result['id']);r.status='failed';r.error='队列不可用，请创建新任务';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 return result
