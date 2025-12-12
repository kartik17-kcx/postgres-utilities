WITH tbl AS (
    SELECT 
        c.oid,
        c.relname AS table_name,
        pg_total_relation_size(c.oid) AS total_table_size,
        pg_relation_size(c.oid) AS table_data_size
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = '<your_schema>'
      AND c.relname = '<your_table>'
)
SELECT 
    t.table_name,
    pg_size_pretty(t.table_data_size) AS table_size,
    i.relname AS index_name,
    pg_size_pretty(pg_relation_size(i.oid)) AS index_size,
TO_CHAR(
    (pg_relation_size(i.oid)::numeric / NULLIF(t.table_data_size, 0)) * 100,
    'FM999990.00"%"'
) AS index_pct_of_total_size

FROM tbl t
LEFT JOIN pg_index ix ON ix.indrelid = t.oid
LEFT JOIN pg_class i ON i.oid = ix.indexrelid
ORDER BY pg_relation_size(i.oid) DESC NULLS LAST;
