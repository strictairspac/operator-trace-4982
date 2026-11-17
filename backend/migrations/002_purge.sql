-- Schema v2: 数据集彻底清除
-- 幂等 PostgreSQL 迁移；全新库由 SQLAlchemy metadata 直接建出，已存在的库执行本脚本补齐。
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS purge_status VARCHAR(20) NOT NULL DEFAULT 'active';
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS purge_error VARCHAR(1000);
CREATE TABLE IF NOT EXISTS purge_jobs (
 id SERIAL PRIMARY KEY,
 dataset_id INTEGER NOT NULL REFERENCES datasets(id),
 status VARCHAR(20) NOT NULL DEFAULT 'queued',
 attempts INTEGER NOT NULL DEFAULT 0,
 error VARCHAR(1000),
 created_by VARCHAR(120) NOT NULL,
 created_at TIMESTAMP DEFAULT now(),
 updated_at TIMESTAMP DEFAULT now()
);
ALTER TABLE traces ADD COLUMN IF NOT EXISTS purge_job_id INTEGER REFERENCES purge_jobs(id);
