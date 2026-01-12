-- Fix pipeline_metadata table - add missing ended_at column if not exists
ALTER TABLE pipeline_metadata ADD COLUMN IF NOT EXISTS ended_at TIMESTAMP WITH TIME ZONE NULL;
