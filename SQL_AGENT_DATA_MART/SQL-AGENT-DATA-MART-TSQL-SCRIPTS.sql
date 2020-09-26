/*

	SQL_AGENT_DATA_MART T-SQL scripts

*/

USE [SQL_AGENT_DATA_MART]
GO


-- Job table
IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'dbo'
	AND TABLE_NAME = 'Job'
)
BEGIN
	CREATE TABLE [dbo].[Job] (
		[job_id]		UNIQUEIDENTIFIER NOT NULL
			CONSTRAINT PK_Job
				PRIMARY KEY CLUSTERED
	,	[name]			NVARCHAR(128) NOT NULL
	,	[step_count]	SMALLINT NOT NULL
	)
END
GO


CREATE OR ALTER PROCEDURE [dbo].[LoadJob]
AS
BEGIN
	TRUNCATE TABLE [dbo].[Job];

	INSERT [dbo].[Job] (
		[job_id]
	,	[name]
	,	[step_count]
	)
	SELECT
		j.[job_id]
	,	j.[name]
	,	COUNT(*)	AS [step_count]
	FROM msdb.dbo.sysjobsteps s
	JOIN msdb.dbo.sysjobs j
	ON j.[job_id] = s.[job_id]
	GROUP BY j.[job_id], j.[name]
END
GO


-- JobInstance table
IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'dbo'
	AND TABLE_NAME = 'JobInstance'
)
BEGIN
	CREATE TABLE [dbo].[JobInstance] (
		[job_id]			UNIQUEIDENTIFIER	NOT NULL
	,	[job_instance]		BIGINT				NOT NULL
	,	[start_time]		DATETIME			NOT NULL
	,	[end_time]			DATETIME			NOT NULL
	,	[duration_seconds]	INT					NOT NULL
	,	[run_status]		INT					NOT NULL
	,	[retries_attempted]	INT					NOT NULL
	,	[step_count]		SMALLINT			NOT NULL
		CONSTRAINT PK_JobInstance
			PRIMARY KEY CLUSTERED ([job_id], [job_instance])
	)	
END
GO


CREATE OR ALTER PROCEDURE [dbo].[LoadJobInstance]
AS
BEGIN
	TRUNCATE TABLE [dbo].[JobInstance];

	;WITH CTE_JOB_OUTCOME AS (
		-- Get jobs that ran successfully. Get the start_time, end_time and duration in seconds.
		-- ?? Do we really need the step_count from the Job table ??
		-- We don't know if every step in the job ran.
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
		,	j.[step_count]
		FROM msdb.dbo.sysjobhistory h
		JOIN [dbo].[Job] j
		ON j.[job_id] = h.[job_id]
		WHERE [step_id] = 0
		AND [run_status] = 1
	)

	, CTE_JOB_OUTCOME_INSTANCE AS (
		-- Assign a job instanceusing ROW_NUMBER()
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
		,	[step_count]
		FROM CTE_JOB_OUTCOME
	)

	INSERT [dbo].[JobInstance] (
		[job_id]			
	,	[job_instance]		
	,	[start_time]		
	,	[end_time]			
	,	[duration_seconds]	
	,	[run_status]		
	,	[retries_attempted]	
	,	[step_count]		
	)
	SELECT
		[job_id]			
	,	[job_instance]		
	,	[start_time]		
	,	[end_time]			
	,	[duration_seconds]	
	,	[run_status]
	,	[retries_attempted]	
	,	[step_count]		
	FROM CTE_JOB_OUTCOME_INSTANCE;
END
GO


-- JobStepInstance Table
IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'dbo'
	AND TABLE_NAME = 'JobStepInstance'
)
BEGIN
	CREATE TABLE [dbo].[JobStepInstance] (
		[job_id]			UNIQUEIDENTIFIER	NOT NULL
	,	[job_instance]		BIGINT				NOT NULL
	,	[step_id]			INT					NOT NULL
	,	[start_time]		DATETIME			NOT NULL
	,	[end_time]			DATETIME			NOT NULL
	,	[duration_seconds]	INT					NOT NULL
	,	[run_status]		INT					NOT NULL
	,	[retries_attempted]	INT					NOT NULL
	,	[step_count]		SMALLINT			NOT NULL
		CONSTRAINT PK_JobStepInstance
			PRIMARY KEY CLUSTERED ([job_id], [job_instance], [step_id], [start_time])
	)	
END
GO


CREATE OR ALTER PROCEDURE [dbo].[LoadJobStepInstance]
AS 
BEGIN
	TRUNCATE TABLE [dbo].[JobStepInstance];

	;WITH CTE_JOB_STEP_OUTCOME AS (
		-- Get every job step that ran successfully. Get the start_time, end_time and duration in seconds.
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

	, CTE_JOB_STEP_OUTCOME_INSTANCE AS (
		-- Assign the job step to the job instance based on the job step start_time
		-- being between the start_time and end_time of the job.
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
		JOIN [dbo].[JobInstance] j
		ON j.[job_id] = s.[job_id] 
		WHERE s.[start_time] BETWEEN j.[start_time] AND j.[end_time]
	)

	INSERT [dbo].[JobStepInstance] (
		[job_id]
	,	[job_instance]
	,	[step_id]
	,	[start_time]
	,	[end_time]
	,	[duration_seconds]
	,	[run_status]
	,	[retries_attempted]
	)
	SELECT
		[job_id]
	,	[job_instance]
	,	[step_id]
	,	[start_time]
	,	[end_time]
	,	[duration_seconds]
	,	[run_status]
	,	[retries_attempted]
	FROM CTE_JOB_STEP_OUTCOME_INSTANCE;
END
GO


-- JobStepAverageDuration
IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'dbo'
	AND TABLE_NAME = 'JobStepAverageDuration'
)
BEGIN
	CREATE TABLE [dbo].[JobStepAverageDuration] (
		[job_id]				UNIQUEIDENTIFIER	NOT NULL
	,	[step_id]				INT					NOT NULL
	,	[count]					INT					NOT NULL
	,	[avg_duration_seconds]	INT					NOT NULL
		CONSTRAINT PK_JobStepAverageDuration
			PRIMARY KEY CLUSTERED ([job_id], [step_id])
	)	
END
GO


CREATE OR ALTER PROCEDURE [dbo].[CalculateJobStepAvgDuration]
AS
BEGIN
	TRUNCATE TABLE [dbo].[JobStepAverageDuration];

	;WITH CTE_JOB_STEP_DURATION_EXECUTED_STEPS AS (
		SELECT
			j.[job_id]
		,	j.[job_instance]
		,	COUNT(*)					AS [executed_steps]
		,	SUM(s.[duration_seconds])	AS [duration_seconds]
		FROM [dbo].[JobStepInstance] s
		JOIN [dbo].[JobInstance] j
		ON j.[job_id] = s.[job_id]
		WHERE s.[start_time] BETWEEN j.[start_time] AND j.[end_time]
		GROUP BY j.[job_id], j.[job_instance]
	)

	, CTE_JOB_STEP_JOB_STEP_COUNT AS (
		SELECT 
			e.[job_id]
		 ,	j.[name]
		,	e.[job_instance]
		,	j.[step_count]
		,	e.[executed_steps]
		,	e.[duration_seconds]
		FROM CTE_JOB_STEP_DURATION_EXECUTED_STEPS  e
		JOIN [dbo].[Job] j ON j.[job_id] = e.[job_id]
	)

	, CTE_JOBS_ALL_STEPS_SUCCEEDED AS (
		SELECT
			[job_id]
		,	[job_instance]
		FROM CTE_JOB_STEP_JOB_STEP_COUNT c
		WHERE c.[step_count] = c.[executed_steps]
	)

	, CTE_CALCULATE_AVG_STEP_DURATION AS (
		SELECT
			s.[job_id]
		,	s.[step_id]
		,	COUNT(*)					AS [count]
		,	AVG(s.[duration_seconds])	AS [avg_duration_seconds]
		FROM CTE_JOBS_ALL_STEPS_SUCCEEDED j
		JOIN [dbo].[JobStepInstance] s
		ON s.[job_id] = j.[job_id] AND s.[job_instance] = j.[job_instance]
		GROUP BY s.[job_id], s.[step_id]
	) 

	INSERT [dbo].[JobStepAverageDuration] (
		[job_id]				
	,	[step_id]				
	,	[count]					
	,	[avg_duration_seconds]	
	)
	SELECT 
		[job_id]				
	,	[step_id]				
	,	[count]					
	,	[avg_duration_seconds]	
	FROM CTE_CALCULATE_AVG_STEP_DURATION; 
END
GO


-- JobAverageDuration table
IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'dbo'
	AND TABLE_NAME = 'JobAverageDuration'
)
BEGIN
	CREATE TABLE [dbo].[JobAverageDuration] (
		[job_id]					UNIQUEIDENTIFIER	NOT NULL
	,	[execution_count]			INT					NOT NULL
	,	[average_duration_seconds]	INT					NOT NULL
	,	[average_retries_attempted]	INT					NOT NULL
		CONSTRAINT PK_JobAverageDuration
			PRIMARY KEY CLUSTERED ([job_id])
	)	
END
GO


CREATE OR ALTER PROCEDURE [dbo].[CalculateJobAverageDuration]
AS
BEGIN
	TRUNCATE TABLE [dbo].[JobAverageDuration];

	INSERT [dbo].[JobAverageDuration] (
		[job_id]					
	,	[execution_count]			
	,	[average_duration_seconds]	
	,	[average_retries_attempted]
	)
	SELECT
		[job_id]
	,	COUNT(*)
	,	AVG([duration_seconds])
	,	AVG([retries_attempted])
	FROM [dbo].[JobInstance]
	GROUP BY [job_id];
END
GO





