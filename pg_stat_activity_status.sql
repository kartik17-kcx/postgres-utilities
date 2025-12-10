	SELECT
		pid,
		usename AS user_name,
		datname AS database_name,
		state,
		client_addr,
		now() - query_start AS query_duration,
		wait_event_type,
		wait_event,
		backend_type,
		query AS query_text
	FROM pg_stat_activity
	ORDER BY query_duration DESC;