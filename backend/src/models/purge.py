from sqlalchemy import Column,Integer,String,DateTime,func,ForeignKey
from src.config.database import Base
class PurgeJob(Base):
 __tablename__='purge_jobs'
 id=Column(Integer,primary_key=True)
 dataset_id=Column(Integer,ForeignKey('datasets.id'),nullable=False)
 status=Column(String(20),nullable=False,default='queued')
 # attempts 记录实际清理尝试次数，中断重试不重复建任务
 attempts=Column(Integer,nullable=False,default=0,server_default='0')
 error=Column(String(1000),nullable=True)
 created_by=Column(String(120),nullable=False)
 created_at=Column(DateTime,server_default=func.now())
 updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())
