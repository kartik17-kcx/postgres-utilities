SELECT
    c.relname AS table_name,
    CASE 
        WHEN p.partstrat = 'r' THEN 'RANGE'
        WHEN p.partstrat = 'l' THEN 'LIST'
        WHEN p.partstrat = 'h' THEN 'HASH'
        ELSE 'NOT PARTITIONED'
    END AS partition_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_partitioned_table p ON p.partrelid = c.oid
WHERE n.nspname = 'attryb'
  AND c.relname = 'meta_data';

