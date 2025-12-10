
-- check if your table is logged or not
SELECT 
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.relpersistence,
    CASE c.relpersistence
        WHEN 'p' THEN 'logged'
        WHEN 'u' THEN 'unlogged'
        WHEN 't' THEN 'temporary'
    END AS persistence_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'dbt'
  AND c.relname = 'stg_tracks';
