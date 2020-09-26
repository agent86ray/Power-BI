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







	




