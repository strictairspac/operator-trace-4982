from sqlalchemy import Column,Integer,String,JSON
from src.config.database import Base
class Dataset(Base):
 __tablename__='datasets'
 id=Column(Integer,primary_key=True)
 name=Column(String(120),unique=True,nullable=False)
 description=Column(String(500),nullable=False,default='')
 sensors=Column(JSON,nullable=False)
 readings=Column(JSON,nullable=False)
 # 清除状态机：active/purging/purge_failed/purged；清除后 sensors/readings 置空，仅保留名称与描述供追溯
 purge_status=Column(String(20),nullable=False,default='active',server_default='active')
 purge_error=Column(String(1000),nullable=True)
