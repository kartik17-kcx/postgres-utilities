--------------------------------------
-- get the column names
-------------------------------------
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = '<>' AND table_name = 'users';