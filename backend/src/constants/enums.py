ROLES=('admin','operator','viewer')
STATUSES=('queued','running','completed','failed')
OPERATORS=('scan','filter','hash_join','group')
# 数据集彻底清除状态机：active 正常；purging 已受理屏蔽、清理中；purge_failed 清理失败仍屏蔽、可重试；purged 全部清空
PURGE_STATUSES=('active','purging','purge_failed','purged')
PURGE_JOB_STATUSES=('queued','running','succeeded','failed')
