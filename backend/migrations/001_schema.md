# Schema v1
SQLAlchemy metadata creates users, audits, datasets, plans, traces. Natural names are unique; foreign keys reference parent entities. Each trace stores immutable JSON input snapshots. PostgreSQL persists metadata and teaching datasets; the bounded Python operators execute the plan.

# Schema v2
datasets gains purge_status (active/purging/purge_failed/purged, default active) and purge_error. traces gains purge_job_id referencing purge_jobs. New purge_jobs table (id, dataset_id, status queued/running/succeeded/failed, attempts, error, created_by, created_at, updated_at). After a purge is accepted the dataset is masked immediately; on success sensors/readings become empty arrays and every trace snapshot/result is replaced by {"purged": true}. Dataset name, description and linked plans remain for traceability. Idempotent PostgreSQL DDL: see 002_purge.sql.
