-- create paritions for the stg_pages and stg_tracks (creating monthly partitions)
-- 1. rename the existing table
ALTER TABLE dbt.stg_pages RENAME TO stg_pages_old;

-- 2. create the schema of the stg_pages now 

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'stg_pages_old';

CREATE TABLE dbt.stg_pages (
    event_id text,
    session_id text,
    anonymous_id text,
    shopify_client_id text,
    user_id text,
    event text,
    event_text text,
    category text,
    is_pixel_event boolean,
    init_cart_id text,
    init_cart_currency text,
    init_cart_quantity bigint,
    init_cart_value numeric,
    init_cart_attributes jsonb,
    init_cart_products jsonb,
    "timestamp" timestamptz,
    uuid_ts timestamptz NOT NULL,
    shopify_store_event_timestamp timestamp,
    page_referrer text,
    page_title text,
    page_url text,
    page_search text,
    page_pathname text,
    screen_width integer,
    screen_height integer,
    device_language text,
    context_ip text,
    user_agent text,
    campaign_source text,
    campaign_medium text,
    campaign_name text,
    campaign_content text,
    campaign_term text,
    context_page_initial_referrer text,
    context_screen_height bigint,
    context_screen_width bigint,
    context_locale text,
    context_user_agent text,
    context_page_url text,
    context_page_search text,
    shopify_pixel_event_details jsonb
)
PARTITION BY RANGE (uuid_ts);


-- 3. create monthly partitions
-- 2024-12
CREATE TABLE dbt.stg_pages_2024_12
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

-- 2025-01
CREATE TABLE dbt.stg_pages_2025_01
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- 2025-02
CREATE TABLE dbt.stg_pages_2025_02
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

-- 2025-03
CREATE TABLE dbt.stg_pages_2025_03
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

-- 2025-04
CREATE TABLE dbt.stg_pages_2025_04
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

-- 2025-05
CREATE TABLE dbt.stg_pages_2025_05
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');

-- 2025-06
CREATE TABLE dbt.stg_pages_2025_06
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');

-- 2025-07
CREATE TABLE dbt.stg_pages_2025_07
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

-- 2025-08
CREATE TABLE dbt.stg_pages_2025_08
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

-- 2025-09
CREATE TABLE dbt.stg_pages_2025_09
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

-- 2025-10
CREATE TABLE dbt.stg_pages_2025_10
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

-- 2025-11
CREATE TABLE dbt.stg_pages_2025_11
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

-- 2025-12
CREATE TABLE dbt.stg_pages_2025_12
    PARTITION OF dbt.stg_pages
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');


-- transfer the data into the partitions
INSERT INTO dbt.stg_pages
SELECT * FROM dbt.stg_pages_old 


-- now create the indexes on the partitions
CREATE INDEX stg_pages_uuid_ts_idx ON dbt.stg_pages (uuid_ts);
CREATE INDEX stg_pages_session_id_idx ON dbt.stg_pages (session_id);

SELECT 
    indexname AS index_name,
    indexdef AS index_definition
FROM 
    pg_indexes
WHERE 
    schemaname = 'dbt' 
    AND tablename = 'stg_pages';


-- check if the partitions have the index
SELECT tab.relname AS partition,
       idx.relname AS index_name
FROM pg_class tab
LEFT JOIN pg_index i ON i.indrelid = tab.oid
LEFT JOIN pg_class idx ON idx.oid = i.indexrelid
WHERE tab.oid IN (
    SELECT inhrelid
    FROM pg_inherits
    WHERE inhparent = 'dbt.stg_pages'::regclass
)
ORDER BY tab.relname;


-- remove the table
ALTER TABLE dbt.stg_pages_old SET UNLOGGED;
DROP TABLE dbt.stg_pages_old CASCADE;