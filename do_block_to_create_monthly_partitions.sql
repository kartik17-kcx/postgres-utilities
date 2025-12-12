-- Alternative: Create partitions using a simple DO block (one-time use)
DO $$
DECLARE
    v_partition_date DATE;
    v_next_partition_date DATE;
    v_partition_name TEXT;
    v_year TEXT;
    v_month TEXT;
    v_sql TEXT;
    
    -- Configuration variables
    v_table_name TEXT := 'stg_pages';
    v_schema_name TEXT := 'dbt';
    v_start_date DATE := '2024-12-01';
    v_end_date DATE := '2026-01-01';
BEGIN
    v_partition_date := DATE_TRUNC('month', v_start_date);
    
    WHILE v_partition_date < v_end_date LOOP
        v_next_partition_date := v_partition_date + INTERVAL '1 month';
        v_year := TO_CHAR(v_partition_date, 'YYYY');
        v_month := TO_CHAR(v_partition_date, 'MM');
        v_partition_name := v_schema_name || '.' || v_table_name || '_' || v_year || '_' || v_month;
        
        v_sql := FORMAT(
            'CREATE TABLE IF NOT EXISTS %s PARTITION OF %s.%s FOR VALUES FROM (%L) TO (%L)',
            v_partition_name,
            v_schema_name,
            v_table_name,
            v_partition_date,
            v_next_partition_date
        );
        
        EXECUTE v_sql;
        RAISE NOTICE 'Created partition: %', v_partition_name;
        
        v_partition_date := v_next_partition_date;
    END LOOP;
END;
$$;