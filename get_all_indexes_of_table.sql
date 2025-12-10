
--------------------------------------
-- get the indexes
-------------------------------------
SELECT 
    indexname AS index_name,
    indexdef AS index_definition
FROM 
    pg_indexes
WHERE 
    schemaname = 'public' 
    AND tablename = 'id_stitching';
