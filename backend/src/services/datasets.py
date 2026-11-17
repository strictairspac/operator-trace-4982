from fastapi import HTTPException
from src.config.database import Session
from src.models.dataset import Dataset
from src.services.purge import mask_dataset,ensure_editable
def list_items():
 with Session() as s:return [mask_dataset(r) for r in s.query(Dataset).order_by(Dataset.id.desc()).all()]
def save(data,item_id=None):
 with Session() as s:
  if item_id:
   row=ensure_editable(s,item_id)
  else:
   row=Dataset()
  for k,v in data.model_dump().items():setattr(row,k,v)
  s.add(row);s.commit();s.refresh(row);return mask_dataset(row)
