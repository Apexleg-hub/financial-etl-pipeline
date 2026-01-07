-- ============================================================================
-- Financial ETL Pipeline - Pipeline Execution Metadata Table
-- ============================================================================
-- Purpose: Track all ETL pipeline runs for monitoring, debugging, and auditing
-- Table Name: public.pipeline_metadata
-- Created: 2026-01-07
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.pipeline_metadata (
    -- ========================================================================
    -- Primary Key & Run Identifiers
    -- ========================================================================
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id VARCHAR(100) NOT NULL UNIQUE COMMENT 'Unique identifier for this ETL run',
    pipeline_name VARCHAR(100) NOT NULL COMMENT 'Name of the pipeline (stock, weather, forex, etc)',
    
    -- ========================================================================
    -- Execution Timestamps
    -- ========================================================================
    started_at TIMESTAMP WITH TIME ZONE NOT NULL COMMENT 'When the ETL run started',
    ended_at TIMESTAMP WITH TIME ZONE COMMENT 'When the ETL run ended (NULL if running)',
    
    -- ========================================================================
    -- Execution Status & Results
    -- ========================================================================
    status VARCHAR(50) NOT NULL DEFAULT 'running' COMMENT 'Status: running, success, partial_success, failed, cancelled',
    success_count INTEGER DEFAULT 0 COMMENT 'Number of records successfully processed',
    failure_count INTEGER DEFAULT 0 COMMENT 'Number of records that failed processing',
    warning_count INTEGER DEFAULT 0 COMMENT 'Number of warnings (non-fatal issues)',
    error_message TEXT COMMENT 'Error message if status is "failed"',
    
    -- ========================================================================
    -- Processing Details
    -- ========================================================================
    records_extracted INTEGER DEFAULT 0 COMMENT 'Total records extracted from source',
    records_transformed INTEGER DEFAULT 0 COMMENT 'Total records after transformation',
    records_loaded INTEGER DEFAULT 0 COMMENT 'Total records successfully loaded to database',
    
    -- ========================================================================
    -- Data Parameters
    -- ========================================================================
    parameters JSONB COMMENT 'JSON object with pipeline parameters (symbols, cities, pairs, etc)',
    
    -- ========================================================================
    -- Performance Metrics
    -- ========================================================================
    extract_duration_seconds NUMERIC(10, 2) COMMENT 'Time spent on extraction phase',
    transform_duration_seconds NUMERIC(10, 2) COMMENT 'Time spent on transformation phase',
    load_duration_seconds NUMERIC(10, 2) COMMENT 'Time spent on loading phase',
    total_duration_seconds NUMERIC(10, 2) COMMENT 'Total execution time',
    
    -- ========================================================================
    -- Data Lineage & Source Info
    -- ========================================================================
    data_source VARCHAR(100) COMMENT 'External data source (alpha_vantage, openweather, fred, etc)',
    source_record_count INTEGER COMMENT 'Total records available from source',
    source_last_update TIMESTAMP WITH TIME ZONE COMMENT 'Last update time reported by source',
    
    -- ========================================================================
    -- Execution Environment
    -- ========================================================================
    environment VARCHAR(50) COMMENT 'Execution environment: development, staging, production',
    executor VARCHAR(100) COMMENT 'Who/what triggered the run (cron, airflow, manual, etc)',
    python_version VARCHAR(20) COMMENT 'Python version used (3.9, 3.10, 3.11, 3.12)',
    
    -- ========================================================================
    -- Data Quality Flags
    -- ========================================================================
    has_anomalies BOOLEAN DEFAULT FALSE COMMENT 'TRUE if data quality issues were detected',
    anomaly_details JSONB COMMENT 'JSON array with details of detected anomalies',
    data_validation_passed BOOLEAN COMMENT 'TRUE if all data validation checks passed',
    
    -- ========================================================================
    -- Dependency & Trigger Information
    -- ========================================================================
    triggered_by_run_id UUID COMMENT 'Parent run_id if this was triggered by another pipeline',
    depends_on_run_ids UUID[] COMMENT 'Array of run_ids that must complete before this one',
    retry_count INTEGER DEFAULT 0 COMMENT 'Number of times this run was retried',
    
    -- ========================================================================
    -- Auditing
    -- ========================================================================
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- ========================================================================
    -- Constraints
    -- ========================================================================
    CONSTRAINT chk_status_valid CHECK (
        status IN ('running', 'success', 'partial_success', 'failed', 'cancelled')
    ),
    CONSTRAINT chk_counts_non_negative CHECK (
        success_count >= 0 AND failure_count >= 0 AND warning_count >= 0
    ),
    CONSTRAINT chk_durations_positive CHECK (
        extract_duration_seconds IS NULL OR extract_duration_seconds > 0
    ),
    CONSTRAINT chk_ended_after_started CHECK (
        ended_at IS NULL OR ended_at >= started_at
    ),
    CONSTRAINT chk_retry_count_non_negative CHECK (
        retry_count >= 0
    ),
    CONSTRAINT chk_total_duration CHECK (
        total_duration_seconds IS NULL OR 
        total_duration_seconds = COALESCE(extract_duration_seconds, 0) + 
                                 COALESCE(transform_duration_seconds, 0) + 
                                 COALESCE(load_duration_seconds, 0)
    )
);

-- ============================================================================
-- INDEXES: Query Optimization
-- ============================================================================

-- Index 1: Run ID lookup (direct access to specific runs)
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_metadata_run_id 
    ON public.pipeline_metadata(run_id);

-- Index 2: Pipeline name (all runs of a specific pipeline)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_pipeline_name 
    ON public.pipeline_metadata(pipeline_name);

-- Index 3: Status-based queries (find all failed runs, running runs, etc)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_status 
    ON public.pipeline_metadata(status);

-- Index 4: Time-based queries (recent runs, historical analysis)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_started_at 
    ON public.pipeline_metadata(started_at DESC);

-- Index 5: Composite for pipeline + status (e.g., "find failed stock ETL runs")
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_pipeline_status 
    ON public.pipeline_metadata(pipeline_name, status, started_at DESC);

-- Index 6: End time (for completed run queries)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_ended_at 
    ON public.pipeline_metadata(ended_at DESC) 
    WHERE ended_at IS NOT NULL;

-- Index 7: Anomaly detection (flag data quality issues)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_anomalies 
    ON public.pipeline_metadata(has_anomalies) 
    WHERE has_anomalies = TRUE;

-- Index 8: Execution environment (separate production from dev)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_environment 
    ON public.pipeline_metadata(environment);

-- Index 9: Data source tracking (which external sources are failing)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_data_source 
    ON public.pipeline_metadata(data_source);

-- ============================================================================
-- TRIGGER: Automatic Timestamp Management
-- ============================================================================

-- Create trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_pipeline_metadata_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop trigger if exists (to avoid conflicts)
DROP TRIGGER IF EXISTS trg_pipeline_metadata_update_timestamp ON public.pipeline_metadata;

-- Create trigger
CREATE TRIGGER trg_pipeline_metadata_update_timestamp
BEFORE UPDATE ON public.pipeline_metadata
FOR EACH ROW
EXECUTE FUNCTION update_pipeline_metadata_timestamp();

-- ============================================================================
-- PROCEDURE: Mark run as complete
-- ============================================================================

CREATE OR REPLACE FUNCTION mark_pipeline_run_complete(
    p_run_id VARCHAR(100),
    p_status VARCHAR(50),
    p_success_count INTEGER DEFAULT 0,
    p_failure_count INTEGER DEFAULT 0,
    p_warning_count INTEGER DEFAULT 0
)
RETURNS TABLE(success BOOLEAN, message TEXT) AS $$
DECLARE
    v_started_at TIMESTAMP WITH TIME ZONE;
    v_duration NUMERIC;
BEGIN
    -- Get start time
    SELECT started_at INTO v_started_at 
    FROM public.pipeline_metadata 
    WHERE run_id = p_run_id;
    
    IF v_started_at IS NULL THEN
        RETURN QUERY SELECT FALSE, 'Run ID not found: ' || p_run_id;
        RETURN;
    END IF;
    
    -- Calculate duration
    v_duration := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_started_at)) / 60.0;
    
    -- Update run record
    UPDATE public.pipeline_metadata
    SET 
        status = p_status,
        success_count = p_success_count,
        failure_count = p_failure_count,
        warning_count = p_warning_count,
        records_loaded = p_success_count,
        total_duration_seconds = v_duration,
        ended_at = CURRENT_TIMESTAMP
    WHERE run_id = p_run_id;
    
    RETURN QUERY SELECT TRUE, 'Pipeline run ' || p_run_id || ' marked as ' || p_status;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS: Self-documenting Schema
-- ============================================================================

COMMENT ON TABLE public.pipeline_metadata IS 
'Audit trail and monitoring table for all ETL pipeline executions.
Tracks execution status, performance metrics, data quality issues, and error details.
Essential for operational monitoring, debugging failed runs, and performance optimization.
Also used for calculating SLAs, identifying bottlenecks, and trending infrastructure needs.
Python ETL automatically inserts a record at the start of each run and updates it at completion.
Indexes optimized for operational dashboards and historical analysis queries.';

COMMENT ON COLUMN public.pipeline_metadata.id IS
'Unique identifier (UUID). Primary key for record-level operations.';

COMMENT ON COLUMN public.pipeline_metadata.run_id IS
'Unique identifier for this specific ETL execution. Format: pipeline_name_timestamp_random.
Examples: "stock_20260107_143025_abc123", "weather_20260107_150000_def456".
Used to correlate logs, alerts, and dependent pipeline runs.';

COMMENT ON COLUMN public.pipeline_metadata.pipeline_name IS
'Name of the pipeline: stock, weather, forex, fred, finnhub, etc.
Allows filtering and grouping by pipeline type.';

COMMENT ON COLUMN public.pipeline_metadata.started_at IS
'Exact timestamp when the ETL run started. Used to calculate duration and correlate with external events.
Format: TIMESTAMP WITH TIME ZONE for accurate timezone handling.';

COMMENT ON COLUMN public.pipeline_metadata.ended_at IS
'Exact timestamp when the ETL run completed (success or failure).
NULL if run is still in progress. Used to calculate total_duration_seconds.';

COMMENT ON COLUMN public.pipeline_metadata.status IS
'Final status of the run:
- "running": Execution in progress
- "success": All records processed successfully
- "partial_success": Some records processed, some failed
- "failed": Critical failure, no records loaded
- "cancelled": Run was manually cancelled
Used to query successful vs failed runs.';

COMMENT ON COLUMN public.pipeline_metadata.success_count IS
'Number of records successfully processed and loaded.
Useful for data volume trending and identifying reduced data days.';

COMMENT ON COLUMN public.pipeline_metadata.failure_count IS
'Number of records that failed validation or loading.
High failure counts indicate data quality issues or API problems.';

COMMENT ON COLUMN public.pipeline_metadata.warning_count IS
'Number of non-fatal issues (warnings) encountered.
Example: Missing optional fields, anomalous values that were corrected.
Helps identify data quality trends without blocking the load.';

COMMENT ON COLUMN public.pipeline_metadata.records_extracted IS
'Total number of records extracted from the external data source.
Useful for detecting API changes or data source availability issues.
If much higher than records_transformed, indicates high data quality issues.';

COMMENT ON COLUMN public.pipeline_metadata.records_transformed IS
'Number of records after transformation (cleaning, validation, enrichment).
Usually close to records_extracted; lower values indicate data quality issues.';

COMMENT ON COLUMN public.pipeline_metadata.records_loaded IS
'Number of records successfully persisted to the database.
Should equal records_transformed in successful runs.
Lower values indicate loading errors or database constraints.';

COMMENT ON COLUMN public.pipeline_metadata.parameters IS
'JSON object containing the parameters used for this run.
Example: {"symbols": ["AAPL", "MSFT"], "cities": ["New York", "London"]}.
Useful for understanding exactly what data was requested for failed runs.';

COMMENT ON COLUMN public.pipeline_metadata.extract_duration_seconds IS
'Time spent on the extraction phase (API calls, data retrieval).
Large values indicate slow APIs or network issues.
Useful for identifying API rate limit waits and network problems.';

COMMENT ON COLUMN public.pipeline_metadata.transform_duration_seconds IS
'Time spent on data transformation (cleaning, validation, feature engineering).
Large values indicate complex transformations or high data volumes.';

COMMENT ON COLUMN public.pipeline_metadata.load_duration_seconds IS
'Time spent on database loading (inserts, updates, upserts).
Large values indicate database performance issues or lock contention.
Useful for identifying database optimization opportunities.';

COMMENT ON COLUMN public.pipeline_metadata.total_duration_seconds IS
'Total end-to-end execution time. Sum of extract + transform + load times.
Useful for SLA tracking and identifying performance degradation trends.
Must equal sum of phase durations (enforced by constraint).';

COMMENT ON COLUMN public.pipeline_metadata.data_source IS
'External data provider: "alpha_vantage", "openweather", "fred", "finnhub", etc.
Allows identifying which external services are causing issues.';

COMMENT ON COLUMN public.pipeline_metadata.source_record_count IS
'Total records available from the external source (if reported).
Allows detecting API quota changes or data availability issues.';

COMMENT ON COLUMN public.pipeline_metadata.source_last_update IS
'Last update timestamp reported by the external data source.
Useful for detecting stale data (source hasn''t updated in days).';

COMMENT ON COLUMN public.pipeline_metadata.environment IS
'Deployment environment: "development", "staging", "production".
Allows separate monitoring/alerting for each environment.';

COMMENT ON COLUMN public.pipeline_metadata.executor IS
'What triggered this run: "airflow", "cron", "manual", "test", "slack_command", etc.
Useful for tracing user actions and correlating with manual changes.';

COMMENT ON COLUMN public.pipeline_metadata.python_version IS
'Python interpreter version used. Helps correlate with version-specific bugs.
Examples: "3.9.12", "3.10.8", "3.11.5", "3.12.1".';

COMMENT ON COLUMN public.pipeline_metadata.has_anomalies IS
'TRUE if data quality issues were detected (outliers, null values, invalid data).
Quick flag for identifying data quality trends without detailed analysis.';

COMMENT ON COLUMN public.pipeline_metadata.anomaly_details IS
'JSON array with detailed anomaly information for root cause analysis.
Example: [{"type": "outlier", "field": "volume", "value": 999999999, "z_score": 5.2}].';

COMMENT ON COLUMN public.pipeline_metadata.data_validation_passed IS
'TRUE if all data validation checks passed (schema compliance, value ranges, etc).
FALSE indicates data that may need investigation or retry.';

COMMENT ON COLUMN public.pipeline_metadata.triggered_by_run_id IS
'Parent run_id if this pipeline was triggered as a dependent of another.
Example: FRED ETL might trigger after stock ETL completes.
Used to trace dependencies and find the root cause of cascading failures.';

COMMENT ON COLUMN public.pipeline_metadata.depends_on_run_ids IS
'Array of run_ids that must complete before this one.
Used to enforce execution order and identify blocking dependencies.';

COMMENT ON COLUMN public.pipeline_metadata.retry_count IS
'Number of automatic retries performed for this run.
High retry counts may indicate transient issues (network, rate limiting).';

COMMENT ON COLUMN public.pipeline_metadata.created_at IS
'Timestamp when this metadata record was created (at pipeline start).
Set once, never modified. Used for audit trail.';

COMMENT ON COLUMN public.pipeline_metadata.updated_at IS
'Timestamp of last modification (when run completed).
Automatically updated by trigger. Used to identify recently completed runs.';

-- ============================================================================
-- EXAMPLE DATA
-- ============================================================================

-- Example 1: Insert a running pipeline
-- INSERT INTO public.pipeline_metadata (
--     run_id, pipeline_name, started_at, status, parameters
-- ) VALUES (
--     'stock_20260107_120000_abc123',
--     'stock',
--     CURRENT_TIMESTAMP,
--     'running',
--     jsonb_build_object('symbols', ARRAY['AAPL', 'MSFT', 'GOOGL'])
-- );

-- Example 2: Mark run as completed (success)
-- SELECT mark_pipeline_run_complete(
--     'stock_20260107_120000_abc123',
--     'success',
--     1250,  -- success_count
--     5,     -- failure_count (some validation errors)
--     12     -- warning_count (anomalies detected but allowed)
-- );

-- Example 3: Query recent failed runs
-- SELECT run_id, pipeline_name, started_at, error_message
-- FROM public.pipeline_metadata
-- WHERE status = 'failed' AND started_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
-- ORDER BY started_at DESC;

-- Example 4: Pipeline performance trends
-- SELECT 
--     DATE(started_at) as run_date,
--     pipeline_name,
--     COUNT(*) as runs,
--     SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
--     AVG(total_duration_seconds) as avg_duration_sec,
--     MAX(total_duration_seconds) as max_duration_sec,
--     SUM(records_loaded) as total_records_loaded
-- FROM public.pipeline_metadata
-- WHERE started_at >= CURRENT_DATE - INTERVAL '30 days'
-- GROUP BY DATE(started_at), pipeline_name
-- ORDER BY run_date DESC, pipeline_name;

-- ============================================================================
-- COMMON QUERIES
-- ============================================================================

-- Query 1: Latest run for each pipeline
-- SELECT DISTINCT ON (pipeline_name)
--     pipeline_name, run_id, started_at, status, total_duration_seconds
-- FROM public.pipeline_metadata
-- ORDER BY pipeline_name, started_at DESC;

-- Query 2: Failed runs requiring investigation
-- SELECT run_id, pipeline_name, started_at, error_message, failure_count
-- FROM public.pipeline_metadata
-- WHERE status = 'failed' AND started_at >= CURRENT_DATE - INTERVAL '7 days'
-- ORDER BY started_at DESC;

-- Query 3: Performance degradation detection (slow runs)
-- SELECT pipeline_name, started_at, total_duration_seconds,
--   AVG(total_duration_seconds) OVER (PARTITION BY pipeline_name ORDER BY started_at
--     ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as moving_avg_7d
-- FROM public.pipeline_metadata
-- WHERE started_at >= CURRENT_DATE - INTERVAL '30 days'
-- ORDER BY pipeline_name, started_at DESC;

-- Query 4: Data quality trends
-- SELECT DATE(started_at) as run_date, 
--     pipeline_name,
--     SUM(CASE WHEN has_anomalies THEN 1 ELSE 0 END) as runs_with_anomalies,
--     SUM(anomaly_details::text LIKE '%' ? 1 : 0) as anomaly_count
-- FROM public.pipeline_metadata
-- WHERE started_at >= CURRENT_DATE - INTERVAL '30 days'
-- GROUP BY DATE(started_at), pipeline_name
-- ORDER BY run_date DESC;
