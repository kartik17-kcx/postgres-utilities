	-- DB OBJECTS
	`CREATE OR REPLACE FUNCTION attryb.get_segment_results(
    anonymous_id TEXT,
    user_id TEXT,
    segment_rules TEXT
) RETURNS SETOF RECORD AS $$  -- Use SETOF RECORD for dynamic columns
DECLARE
    dynamic_query TEXT;
    stitching_exists BOOLEAN;
BEGIN 
    IF user_id IS NOT NULL AND user_id <> '' THEN
        -- Query customer meta data when user_id is present
        dynamic_query := format($q$
            SELECT %s
            FROM dbt.customer_session_meta cmd
            LEFT JOIN dbt.dim_customers dim ON dim.customer_id::text = cmd.distinct_id::text
            WHERE cmd.distinct_id = $1
        $q$, segment_rules);

        RETURN QUERY EXECUTE dynamic_query USING user_id;

    ELSE
        -- Check if anonymous_id exists in id_stitching
        SELECT EXISTS (
            SELECT 1 FROM dbt.id_stitching WHERE distinct_id = $1
        ) INTO stitching_exists;

        IF stitching_exists THEN 
            -- Query anonymous meta data
            dynamic_query := format($q$
                SELECT %s
                FROM dbt.anonymous_session_meta amd 
                LEFT JOIN dbt.dim_anonymous dim ON dim.anonymous_id = amd.distinct_id
                WHERE amd.distinct_id = $1
            $q$, segment_rules);

            RETURN QUERY EXECUTE dynamic_query USING anonymous_id;

        ELSE
            -- Query customer meta data using anonymous_id
            dynamic_query := format($q$
                SELECT %s
                FROM dbt.customer_session_meta cmd
                LEFT JOIN dbt.dim_customers dim ON dim.customer_id = cmd.distinct_id
                WHERE cmd.anonymous_id = $1
            $q$, segment_rules);

            RETURN QUERY EXECUTE dynamic_query USING anonymous_id;
        END IF;
    END IF;

END;
$$ LANGUAGE plpgsql STABLE;