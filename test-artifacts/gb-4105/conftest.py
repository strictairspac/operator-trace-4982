import os,pathlib,sys
# 独立环境：SQLite + fake Redis，不依赖 Docker/PostgreSQL/Redis
DB_PATH=pathlib.Path('/tmp/gb-4105/test.db')
DB_PATH.parent.mkdir(parents=True,exist_ok=True)
if DB_PATH.exists():DB_PATH.unlink()
os.environ.setdefault('DATABASE_URL','sqlite:///'+str(DB_PATH))
os.environ.setdefault('REDIS_URL','redis://localhost:6379/0')
os.environ.setdefault('JWT_SECRET','gb-4105-test-secret')
BACKEND=pathlib.Path(__file__).resolve().parents[2]/'backend'
sys.path.insert(0,str(BACKEND))
import fakeredis
import pytest
from fastapi.testclient import TestClient
from src.config.database import Base,engine,Session
from src.models.identity import User
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.models.purge import PurgeJob
from src.utils.security import hash_password
from src.services import traces as traces_service
from src.services import purge as purge_service
import src.worker as worker_mod
@pytest.fixture(scope='session',autouse=True)
def setup_env():
 fake=fakeredis.FakeStrictRedis()
 traces_service.queue=fake
 purge_service.queue=fake
 worker_mod.q=fake
 Base.metadata.create_all(engine)
 with Session() as s:
  for role in ('admin','operator','viewer'):
   s.add(User(email=role+'@operator.local',role=role,password=hash_password('Trace2026!')))
  s.commit()
 yield
 engine.dispose()
@pytest.fixture(autouse=True)
def isolate_purge_state():
 # 每个用例独立：清空清除相关表与队列，避免跨用例恢复任务串扰
 from src.services import purge as purge_service
 purge_service.queue.flushall()
 with Session() as s:
  s.query(Trace).delete();s.query(PurgeJob).delete();s.query(Plan).delete();s.query(Dataset).delete();s.commit()
 yield
 purge_service.queue.flushall()
@pytest.fixture
def client():
 from src.main import app
 return TestClient(app)
@pytest.fixture
def auth():
 def _h(role):
  with Session() as s:u=s.query(User).filter_by(email=role+'@operator.local').one()
  from src.utils.security import token
  return {'Authorization':'Bearer '+token(u)}
 return _h
SENSORS=[{'sensor_id':1,'zone':'北场','device_type':'pump'},{'sensor_id':2,'zone':'南场','device_type':'motor'}]
READINGS=[{'sensor_id':1,'value':80,'recorded_at':'2026-11-17T09:00:00'},{'sensor_id':2,'value':40,'recorded_at':'2026-11-17T09:05:00'}]
STEPS=[{'op':'scan','source':'readings'},{'op':'filter','field':'value','compare':'gte','value':50},{'op':'hash_join','right':'sensors','left_key':'sensor_id','right_key':'sensor_id'},{'op':'group','keys':['zone'],'measure':'value','aggregate':'avg'}]
@pytest.fixture
def make_dataset():
 def _make(name='数据集-'+os.urandom(3).hex(),sensors=None,readings=None):
  with Session() as s:
   d=Dataset(name=name,description='测试数据集',sensors=sensors if sensors is not None else[dict(x) for x in SENSORS],readings=readings if readings is not None else[dict(x) for x in READINGS])
   s.add(d);s.commit();s.refresh(d);return d.id
 return _make
@pytest.fixture
def make_plan():
 def _make(dataset_id,name=None):
  with Session() as s:
   p=Plan(name=name or '计划-'+os.urandom(3).hex(),dataset_id=dataset_id,steps=[dict(x) for x in STEPS],note='测试计划')
   s.add(p);s.commit();s.refresh(p);return p.id
 return _make
@pytest.fixture
def make_trace():
 def _make(plan_id,name=None):
  with Session() as s:
   p=s.get(Plan,plan_id);d=s.get(Dataset,p.dataset_id)
   from src.utils.serialization import record
   t=Trace(name=name or '执行-'+os.urandom(3).hex(),plan_id=plan_id,dataset_id=d.id,status='queued',snapshot={'dataset':record(d),'plan':record(p)})
   s.add(t);s.commit();s.refresh(t);return t.id
 return _make
