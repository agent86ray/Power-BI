
/*

	Recalculate average job duration for the entire history in the 
	msdb.dbo.sysjobhistory table. Calculate at the job and step level.
	This will provide 2 metrics for the estimated completion time for
	jobs that are currently running:
	
	1 - Add the average job duration to the job start time  
	2 - Add the sum of the average duration of the remaining job steps
	    to the current time

	Notes from msdb.dbo.sysjobhistory

	When a job ends, a row is written to the table with the step_id = 0
	and step_name = (Job outcome). The run_date and run_time of the first 
	step executed in the job will equal the run_date and run_time in the 
	Job outcome row.
	
	 
*/


/*
-- gather all data for specified job
SELECT
	[step_id]
,	[step_name]
,	msdb.dbo.agent_datetime(run_date, run_time) AS [start_time]
,	DATEADD(
		second
	,	[run_duration] / 10000 * 3600 +			-- convert hours to seconds
		([run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
		([run_duration] % 10000) % 100			-- get seconds
	,	msdb.dbo.agent_datetime(run_date, run_time)) [end_time]
	,	[run_duration] / 10000 * 3600 +			-- convert hours to seconds
		([run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
		([run_duration] % 10000) % 100			-- get seconds
			AS [duration_seconds]
,	[run_status]
,	[retries_attempted]
FROM msdb.dbo.sysjobhistory
WHERE [job_id] = '8D554A8D-58F6-47C6-BAF3-D92C461060C9'
*/



-- Get the current number of steps in each job
;WITH CTE_JOB_STEPS_COUNT AS (
	SELECT
		j.[job_id]
	,	j.[name]
	,	COUNT(*)	AS [job_step_count]
	FROM msdb.dbo.sysjobsteps s
	JOIN msdb.dbo.sysjobs j
	ON j.[job_id] = s.[job_id]
	GROUP BY j.[job_id], j.[name]
)

, CTE_JOB_OUTCOME AS (
	SELECT
		h.[job_id]
	,	msdb.dbo.agent_datetime(run_date, run_time) AS [start_time]
	,	DATEADD(
			second
		,	[run_duration] / 10000 * 3600 +			-- convert hours to seconds
			([run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
			([run_duration] % 10000) % 100			-- get seconds
		,	msdb.dbo.agent_datetime(run_date, run_time)) [end_time]
		,	[run_duration] / 10000 * 3600 +			-- convert hours to seconds
			([run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
			([run_duration] % 10000) % 100			-- get seconds
				AS [duration_seconds]
	,	[run_status]
	,	[retries_attempted]
	,	s.[job_step_count]
	FROM msdb.dbo.sysjobhistory h
	JOIN CTE_JOB_STEPS_COUNT s
	ON s.[job_id] = h.[job_id]
	WHERE [step_id] = 0
	AND [run_status] = 1
)

/*
SELECT * FROM CTE_JOB_OUTCOME
WHERE [job_id] = '8D554A8D-58F6-47C6-BAF3-D92C461060C9'
ORDER BY [job_id], [start_time] DESC
*/

, CTE_JOB_OUTCOME_INSTANCE AS (
	SELECT
		[job_id]
	,	ROW_NUMBER() OVER (
			PARTITION BY [job_id]
			ORDER BY [job_id], [start_time]
		)	AS [job_instance]
	,	[start_time]
	,	[end_time]
	,	[duration_seconds]
	,	[run_status]
	,	[retries_attempted]
	,	[job_step_count]
	FROM CTE_JOB_OUTCOME
)

/*

SELECT * FROM CTE_JOB_OUTCOME_INSTANCE
WHERE [job_id] = '8D554A8D-58F6-47C6-BAF3-D92C461060C9'
ORDER BY [job_id], [start_time] DESC

*/

, CTE_JOB_STEP_OUTCOME AS (
	SELECT
		[job_id]
	,	[step_id]
	,	msdb.dbo.agent_datetime(run_date, run_time) AS [start_time]
	,	DATEADD(
			second
		,	[run_duration] / 10000 * 3600 +			-- convert hours to seconds
			([run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
			([run_duration] % 10000) % 100			-- get seconds
		,	msdb.dbo.agent_datetime(run_date, run_time)) [end_time]
		,	[run_duration] / 10000 * 3600 +			-- convert hours to seconds
			([run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
			([run_duration] % 10000) % 100			-- get seconds
				AS [duration_seconds]
	,	[run_status]
	,	[retries_attempted]
	FROM msdb.dbo.sysjobhistory
	WHERE [step_id] > 0
	AND [run_status] = 1
)

/*

SELECT * FROM CTE_JOB_STEP_OUTCOME
WHERE [job_id] = '8D554A8D-58F6-47C6-BAF3-D92C461060C9'
ORDER BY [job_id], [start_time] DESC

*/

, CTE_JOB_STEP_OUTCOME_INSTANCE AS (
	SELECT
		s.[job_id]
	,	j.[job_instance]
	,	s.[step_id]
	,	s.[start_time]
	,	s.[end_time]
	,	s.[duration_seconds]
	,	s.[run_status]
	,	s.[retries_attempted]
	FROM CTE_JOB_STEP_OUTCOME s
	JOIN CTE_JOB_OUTCOME_INSTANCE j
	ON j.[job_id] = s.[job_id] 
	WHERE s.[start_time] BETWEEN j.[start_time] AND j.[end_time]
)

/*

SELECT * FROM CTE_JOB_STEP_OUTCOME_INSTANCE
WHERE [job_id] = '8D554A8D-58F6-47C6-BAF3-D92C461060C9'
ORDER BY [job_id], [start_time] DESC

*/

, CTE_JOB_STEP_DURATION_EXECUTED_STEPS AS (
	SELECT
		j.[job_id]
	,	j.[job_instance]
	,	COUNT(*)					AS [executed_steps]
	,	SUM(s.[duration_seconds])	AS [duration_seconds]
	FROM CTE_JOB_STEP_OUTCOME_INSTANCE s
	JOIN CTE_JOB_OUTCOME_INSTANCE j
	ON j.[job_id] = s.[job_id]
	WHERE s.[start_time] BETWEEN j.[start_time] AND j.[end_time]
	GROUP BY j.[job_id], j.[job_instance]
)

, CTE_JOB_STEP_JOB_STEP_COUNT AS (
	SELECT 
		e.[job_id]
	 ,	s.[name]
	,	e.[job_instance]
	,	s.[job_step_count]
	,	e.[executed_steps]
	,	e.[duration_seconds]
	FROM CTE_JOB_STEP_DURATION_EXECUTED_STEPS  e
	JOIN CTE_JOB_STEPS_COUNT s ON s.[job_id] = e.[job_id]
)

/*

SELECT *
FROM CTE_JOB_STEP_JOB_STEP_COUNT
WHERE [job_id] = '8D554A8D-58F6-47C6-BAF3-D92C461060C9'
ORDER BY [job_id], [job_instance] DESC

*/

, CTE_JOBS_ALL_STEPS_SUCCEEDED AS (
	SELECT
		[job_id]
	,	[job_instance]
	FROM CTE_JOB_STEP_JOB_STEP_COUNT c
	WHERE c.[job_step_count] = c.[executed_steps]
)

SELECT *
FROM CTE_JOBS_ALL_STEPS_SUCCEEDED
WHERE [job_id] = '8D554A8D-58F6-47C6-BAF3-D92C461060C9'
ORDER BY [job_id], [job_instance];
