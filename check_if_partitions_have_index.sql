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