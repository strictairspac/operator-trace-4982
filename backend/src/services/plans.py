from fastapi import HTTPException
from src.config.database import Session
from src.models.plan import Plan
from src.utils.serialization import record
from src.models.dataset import Dataset
from src.services.purge import ensure_editable

def list_items():
 with Session() as s:return [record(r) for r in s.query(Plan).order_by(Plan.id.desc()).all()]
def save(data,item_id=None):
 with Session() as s:
  ensure_editable(s,data.dataset_id)
  row=s.get(Plan,item_id) if item_id else Plan()
  if not row:raise HTTPException(404,'未找到记录')
  if item_id:
   current=s.get(Dataset,row.dataset_id)
   ensure_editable(s,current.id)
  for k,v in data.model_dump().items():setattr(row,k,v)
  s.add(row);s.commit();s.refresh(row);return record(row)
