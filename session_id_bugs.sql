-- CHECKING FOR THE PIXEL SESSIONS ONLY
WITH
constants AS(
	SELECT 
		'2025-12-01'::DATE as start_date,
		'2025-12-31'::DATE as end_date
),
-- we need to find the cases for customers where we have multiple session ids generated before the 30 minutes inactivity and check the pattern
-- lets check for the customer_session_meta
customer_sessions AS(
	SELECT 
	COUNT(*) OVER(PARTITION BY anonymous_id) as session_count,
	ROW_NUMBER() OVER(PARTITION BY anonymous_id ORDER BY session_first_event_timestamp) as session_sequence,
	anonymous_id,
	session_id,
	session_date,
	session_first_event_timestamp,
	session_last_event_timestamp,
	session_duration,
	pageviews
	FROM dbt.session_meta csm 
	WHERE csm.session_date BETWEEN (select start_date from constants) and (select end_date from constants)
	ORDER BY anonymous_id, session_first_event_timestamp
)
-- select * from customer_sessions where distinct_id='6111700713538'
,
session_analysis AS (
  SELECT 
    session_count,
    session_sequence,
    anonymous_id,
    session_id,
    session_date,
    session_first_event_timestamp,
    session_last_event_timestamp,
    session_duration,
    pageviews,
    LAG(session_id) OVER (
      PARTITION BY anonymous_id 
      ORDER BY session_sequence
    ) AS prev_session_id,
    LAG(session_last_event_timestamp) OVER (
      PARTITION BY anonymous_id 
      ORDER BY session_sequence
    ) AS prev_session_end,
    session_first_event_timestamp - LAG(session_last_event_timestamp) OVER (
      PARTITION BY anonymous_id 
      ORDER BY session_sequence
    ) AS time_gap
  FROM customer_sessions
-- 	where distinct_id='6111700713538'
),
final_data AS(
SELECT 
  session_count,
  session_sequence,
  anonymous_id,
  session_id,
  session_date,
  session_first_event_timestamp,
  session_last_event_timestamp,
  session_duration,
  pageviews,
  prev_session_end,
  time_gap,
  CASE 
    WHEN time_gap IS NULL THEN 'valid'
    WHEN time_gap < INTERVAL '30 minutes' THEN 'duplicate_session_bug'
    ELSE 'valid'
  END AS session_flag,
  CASE 
    WHEN time_gap IS NULL THEN session_id  -- First session uses its own ID
    WHEN time_gap < INTERVAL '30 minutes' THEN prev_session_id  -- Use previous session's ID
    ELSE session_id  -- Valid session uses its own ID
  END AS correct_session_id
FROM session_analysis
ORDER BY anonymous_id, session_sequence
)
select count(distinct correct_session_id) from final_data 