-- PL/pgSQL function to create monthly partitions dynamically
CREATE OR REPLACE FUNCTION dbt.create_monthly_partitions(
    p_table_name TEXT,           -- Parent table name (e.g., 'stg_pages')
    p_schema_name TEXT,           -- Schema name (e.g., 'dbt')
    p_start_date DATE,            -- Start date
    p_end_date DATE               -- End date
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_date DATE;
    v_next_partition_date DATE;
    v_partition_name TEXT;
    v_year TEXT;
    v_month TEXT;
    v_sql TEXT;
    v_partitions_created INT := 0;
BEGIN
    -- Initialize the partition date to the first day of the start month
    v_partition_date := DATE_TRUNC('month', p_start_date);
    
    -- Loop through each month until we reach or exceed the end date
    WHILE v_partition_date < p_end_date LOOP
        -- Calculate the next partition date (first day of next month)
        v_next_partition_date := v_partition_date + INTERVAL '1 month';
        
        -- Format year and month for partition name
        v_year := TO_CHAR(v_partition_date, 'YYYY');
        v_month := TO_CHAR(v_partition_date, 'MM');
        
        -- Construct partition table name
        v_partition_name := p_schema_name || '.' || p_table_name || '_' || v_year || '_' || v_month;
        
        -- Build CREATE TABLE statement
        v_sql := FORMAT(
            'CREATE TABLE IF NOT EXISTS %s PARTITION OF %s.%s FOR VALUES FROM (%L) TO (%L)',
            v_partition_name,
            p_schema_name,
            p_table_name,
            v_partition_date,
            v_next_partition_date
        );
        
        -- Execute the CREATE TABLE statement
        EXECUTE v_sql;
        
        -- Log the partition creation
        RAISE NOTICE 'Created partition: % for range [%, %)', 
            v_partition_name, v_partition_date, v_next_partition_date;
        
        v_partitions_created := v_partitions_created + 1;
        
        -- Move to next month
        v_partition_date := v_next_partition_date;
    END LOOP;
    
    RETURN FORMAT('Successfully created %s partitions', v_partitions_created);
END;
$$;


-- Example usage:
-- SELECT dbt.create_monthly_partitions('stg_pages', 'dbt', '2024-12-01', '2026-01-01');
