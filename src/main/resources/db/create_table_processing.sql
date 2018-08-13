
-- This DB table is used to enable many batch-worker-containers in a cluster
--   to share their working status, to get work in small batches
--   and to shut down gracefully when work completed.

create table processing (

    id                 varchar(100) PRIMARY KEY,

    -- Add mission specific columns here ...

    timestamp          timestamp,     -- when status changed
    status             varchar(50),   -- according to DbStatus enum class
    failure_reason     varchar(100),  -- brief error message
    failure_exception  text           -- detailed error message or full stack trace for future analysis
);