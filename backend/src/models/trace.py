from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey
from src.config.database import Base
class Trace(Base):
 __tablename__='traces'
 id=Column(Integer,primary_key=True)
 name=Column(String(120),unique=True,nullable=False)
 dataset_id=Column(Integer,ForeignKey('datasets.id'),nullable=False)
 plan_id=Column(Integer,ForeignKey('plans.id'),nullable=False)
 status=Column(String(20),default='queued',nullable=False)
 result=Column(JSON,nullable=True)
 error=Column(String(1000),nullable=True)
 snapshot=Column(JSON,nullable=False)
 # 关联的清除任务；非空表示该轨迹受数据集清除屏蔽，worker 不得再写回结果
 purge_job_id=Column(Integer,ForeignKey('purge_jobs.id'),nullable=True)
 created_at=Column(DateTime,server_default=func.now())
