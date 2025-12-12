-- ==============================================================================
-- COMPLETE TABLE PARTITIONING SCRIPT
-- ==============================================================================
-- This script converts an existing table into a partitioned table
-- Usage: Update the configuration variables and run the script
-- ==============================================================================

DO $$
DECLARE
    -- ========== CONFIGURATION VARIABLES (MODIFY THESE) ==========
    v_schema_name TEXT := 'dbt';
    v_table_name TEXT := 'stg_pages';
    v_partition_column TEXT := 'uuid_ts';
    v_start_date DATE := '2024-12-01';
    v_end_date DATE := '2026-01-01';
    
    -- Index definitions (array of index definitions)
    v_indexes TEXT[] := ARRAY[
        'CREATE INDEX IF NOT EXISTS stg_pages_uuid_ts_idx ON dbt.stg_pages (uuid_ts)',
        'CREATE INDEX IF NOT EXISTS stg_pages_session_id_idx ON dbt.stg_pages (session_id)'
    ];
    
    -- ========== INTERNAL VARIABLES ==========
    v_old_table_name TEXT;
    v_full_table_name TEXT;
    v_full_old_table_name TEXT;
    v_partition_date DATE;
    v_next_partition_date DATE;
    v_partition_name TEXT;
    v_year TEXT;
    v_month TEXT;
    v_sql TEXT;
    v_row_count BIGINT;
    v_index_def TEXT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := CLOCK_TIMESTAMP();
    v_old_table_name := v_table_name || '_old';
    v_full_table_name := v_schema_name || '.' || v_table_name;
    v_full_old_table_name := v_schema_name || '.' || v_old_table_name;
    
    RAISE NOTICE '==============================================================================';
    RAISE NOTICE 'STARTING TABLE PARTITIONING PROCESS';
    RAISE NOTICE '==============================================================================';
    RAISE NOTICE 'Table: %', v_full_table_name;
    RAISE NOTICE 'Partition Column: %', v_partition_column;
    RAISE NOTICE 'Date Range: % to %', v_start_date, v_end_date;
    RAISE NOTICE '==============================================================================';
    
    -- Step 1: Rename existing table
    RAISE NOTICE '';
    RAISE NOTICE '[STEP 1/6] Renaming table % to %...', v_full_table_name, v_full_old_table_name;
    EXECUTE FORMAT('ALTER TABLE %I.%I RENAME TO %I', v_schema_name, v_table_name, v_old_table_name);
    RAISE NOTICE '✓ Table renamed successfully';
    
    -- Step 2: Create partitioned table structure
    RAISE NOTICE '';
    RAISE NOTICE '[STEP 2/6] Creating partitioned table structure...';
    EXECUTE FORMAT($DDL$
        CREATE TABLE %I.%I (
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
        PARTITION BY RANGE (%I)
    $DDL$, v_schema_name, v_table_name, v_partition_column);
    RAISE NOTICE '✓ Partitioned table created successfully';
    
    -- Step 3: Create monthly partitions
    RAISE NOTICE '';
    RAISE NOTICE '[STEP 3/6] Creating monthly partitions...';
    v_partition_date := DATE_TRUNC('month', v_start_date);
    
    WHILE v_partition_date < v_end_date LOOP
        v_next_partition_date := v_partition_date + INTERVAL '1 month';
        v_year := TO_CHAR(v_partition_date, 'YYYY');
        v_month := TO_CHAR(v_partition_date, 'MM');
        v_partition_name := v_schema_name || '.' || v_table_name || '_' || v_year || '_' || v_month;
        
        EXECUTE FORMAT(
            'CREATE TABLE IF NOT EXISTS %s PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
            v_partition_name,
            v_schema_name,
            v_table_name,
            v_partition_date,
            v_next_partition_date
        );
        
        RAISE NOTICE '  ✓ Created partition: % [%, %)', v_partition_name, v_partition_date, v_next_partition_date;
        v_partition_date := v_next_partition_date;
    END LOOP;
    RAISE NOTICE '✓ All partitions created successfully';
    
    -- Step 4: Transfer data from old table to partitioned table
    RAISE NOTICE '';
    RAISE NOTICE '[STEP 4/6] Transferring data from % to %...', v_full_old_table_name, v_full_table_name;
    RAISE NOTICE '  This may take a while depending on data volume...';
    
    EXECUTE FORMAT('INSERT INTO %I.%I SELECT * FROM %I.%I', 
        v_schema_name, v_table_name, v_schema_name, v_old_table_name);
    
    EXECUTE FORMAT('SELECT COUNT(*) FROM %I.%I', v_schema_name, v_table_name) INTO v_row_count;
    RAISE NOTICE '✓ Data transfer complete. Rows transferred: %', v_row_count;
    
    -- Step 5: Drop old table
    RAISE NOTICE '';
    RAISE NOTICE '[STEP 5/6] Dropping old table %...', v_full_old_table_name;
    EXECUTE FORMAT('ALTER TABLE %I.%I SET UNLOGGED', v_schema_name, v_old_table_name);
    EXECUTE FORMAT('DROP TABLE %I.%I CASCADE', v_schema_name, v_old_table_name);
    RAISE NOTICE '✓ Old table dropped successfully';
    
    -- Step 6: Create indexes
    RAISE NOTICE '';
    RAISE NOTICE '[STEP 6/6] Creating indexes...';
    FOREACH v_index_def IN ARRAY v_indexes LOOP
        EXECUTE v_index_def;
        RAISE NOTICE '  ✓ Index created: %', SUBSTRING(v_index_def FROM 'CREATE INDEX[^)]*\s+(\w+)\s+ON');
    END LOOP;
    RAISE NOTICE '✓ All indexes created successfully';
    
    -- Summary
    v_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '';
    RAISE NOTICE '==============================================================================';
    RAISE NOTICE 'PARTITIONING COMPLETE';
    RAISE NOTICE '==============================================================================';
    RAISE NOTICE 'Table: %', v_full_table_name;
    RAISE NOTICE 'Total Rows: %', v_row_count;
    RAISE NOTICE 'Duration: %', v_end_time - v_start_time;
    RAISE NOTICE '==============================================================================';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '';
        RAISE NOTICE '==============================================================================';
        RAISE NOTICE 'ERROR OCCURRED DURING PARTITIONING';
        RAISE NOTICE '==============================================================================';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Detail: %', SQLSTATE;
        RAISE NOTICE '';
        RAISE NOTICE 'ROLLBACK INSTRUCTIONS:';
        RAISE NOTICE '1. If the new partitioned table was created, drop it:';
        RAISE NOTICE '   DROP TABLE IF EXISTS %.% CASCADE;', v_schema_name, v_table_name;
        RAISE NOTICE '2. Rename the old table back:';
        RAISE NOTICE '   ALTER TABLE %.% RENAME TO %;', v_schema_name, v_old_table_name, v_table_name;
        RAISE NOTICE '==============================================================================';
        RAISE;
END;
$$;