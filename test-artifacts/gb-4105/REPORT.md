# gb-4105 数据集彻底清除 — 验证报告

环境无 Docker/PostgreSQL/Redis，后端用 SQLite + fakeredis 等价验证（应用代码仍面向 PostgreSQL/Redis，迁移 SQL 见 backend/migrations/002_purge.sql）。

## 运行方式
```sh
PYTHONPATH=/workspace/backend python3 -m pytest -q   # 12 用例
python3 /tmp/e2e_smoke.py                            # FastAPI 全链路冒烟（真实 JWT/路由/审计）
node /tmp/fe_check2.mjs                              # 前端四态渲染检查
```

## 需求覆盖
| 需求 | 用例 |
|---|---|
| 受理即屏蔽数据集与历史执行的数据/预览/导出 | test_accept_masks_immediately_and_other_datasets_unaffected |
| 拒绝编辑与新执行，其他数据集照常 | 同上（PUT/POST 409，正常集 200） |
| 排队中撤回（worker 取到即终止） | test_queued_trace_terminated_when_worker_picks_it |
| 运行中撤回、旧执行晚完成不能写回 | test_running_trace_late_writeback_rejected（含异常路径） |
| 清理覆盖原表/快照/结果，全部清空才标已清除，保留名称与关联计划 | test_full_purge_success_marks_purged_and_keeps_name_and_plans |
| 中断/失败保持屏蔽可重试，不重复建任务 | test_purge_failure_keeps_masked_and_retry_succeeds_without_new_job、test_retry_reuses_job_when_worker_crashed_mid_cleanup |
| worker 重启恢复中断任务（running→queued 重新入队，幂等 claim） | test_orphan_recovery_requeues_interrupted_running_job |
| 清理中重复提交冲突、任务唯一 | test_duplicate_request_while_purging_returns_conflict_and_single_job |
| 队列不可用失败封闭（503、保持屏蔽、可重试） | test_queue_unavailable_fails_closed_and_allows_retry |
| 只读账号拒绝清除 | test_viewer_cannot_purge（403） |
| 审计贯穿 | test_operator_can_purge_and_audit_logged |
| 双队列 worker 分发 | test_worker_main_loop_dispatches_both_queues |

结果：12 passed；E2E SMOKE OK；前端 10 项渲染检查全 PASS。
