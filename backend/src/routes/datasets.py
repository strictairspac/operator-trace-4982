from fastapi import APIRouter,Depends
from src.middlewares.auth import current,writer
from src.schemas.business import DatasetInput
from src.services import datasets,purge
from src.controllers.business import save_dataset
router=APIRouter()
@router.get('/datasets')
def listing(u=Depends(current)):return datasets.list_items()
@router.post('/datasets')
def create(data:DatasetInput,u=Depends(writer)):return save_dataset(data,u)
@router.put('/datasets/{item_id}')
def update(item_id:int,data:DatasetInput,u=Depends(writer)):return save_dataset(data,u,item_id)
@router.post('/datasets/{item_id}/purge')
def purge_dataset(item_id:int,u=Depends(writer)):return purge.request_purge(item_id,u.email)
