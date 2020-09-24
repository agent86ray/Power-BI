/*

	SQL_AGENT_ACTIVITY T-SQL scripts

*/

USE [master]
GO

-- create database if it doesn't exist
IF DATABASEPROPERTYEX (N'DBA', N'Version') IS NULL
BEGIN
	CREATE DATABASE [DBA];

	ALTER DATABASE [DBA]
	SET RECOVERY SIMPLE;
END
GO


USE [DBA]
GO


IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE [name] = 'sqlagent')
BEGIN
	EXEC sp_executesql @stmt = N'CREATE SCHEMA [sqlagent]';
END
GO


IF NOT EXISTS (
	SELECT 1
	FROM sys.schemas
	WHERE [name] = 'sqlagent'
)
BEGIN
	CREATE SEQUENCE [sqlagent].[RefreshKey]  
		START WITH 1  
		INCREMENT BY 1;  
END
GO  


CREATE OR ALTER PROCEDURE [sqlagent].[SimulateJobStepDuration]
	@MINIMUM_MINUTES	INT = 1
,	@MAXIMUM_MINUTES	INT = 10
,	@DEBUG				BIT = 0
AS
BEGIN
	-- convert to range of seconds
	DECLARE
	 	@MINIMUM_SECONDS		INT = @MINIMUM_MINUTES * 60
	,	@MAXIMUM_SECONDS		INT = @MAXIMUM_MINUTES * 60
	,	@STEP_DURATION_SECONDS	INT = 0
	,	@WAIT_FOR				VARCHAR(10);

	WHILE @STEP_DURATION_SECONDS < @MINIMUM_SECONDS
	BEGIN
		SET @STEP_DURATION_SECONDS = FLOOR(RAND() * @MAXIMUM_SECONDS);
	END

	DECLARE
		@HOURS		SMALLINT = 0
	,	@MINUTES 	SMALLINT = (@STEP_DURATION_SECONDS - 3600 ) / 60;

	IF @STEP_DURATION_SECONDS >= 3600
	BEGIN
		SET @HOURS = @STEP_DURATION_SECONDS / 3600;
		SET @MINUTES = (@STEP_DURATION_SECONDS - 3600 ) / 60;
	END
	ELSE
	BEGIN
		SET @MINUTES = @STEP_DURATION_SECONDS / 60;
	END

	DECLARE
		@WAITFOR_STRING		CHAR(5) = 
			RIGHT('00' + CONVERT(VARCHAR(2), @HOURS), 2) +
			':' +
			RIGHT('00' + CONVERT(VARCHAR(2), @MINUTES), 2);

	IF @DEBUG = 1
	BEGIN
		SELECT 
			@MINIMUM_MINUTES		AS [MINIMUM_MINUTES]
		,	@MAXIMUM_MINUTES		AS [MAXIMUM_MINUTES]
		,	@STEP_DURATION_SECONDS	AS [STEP_DURATION_SECONDS]
		,	@WAITFOR_STRING			AS [WAITFOR_STRING];
	END

	IF @DEBUG = 0
	BEGIN
		WAITFOR DELAY @WAITFOR_STRING;
	END
END
GO


IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'sqlagent'
	AND TABLE_NAME = 'ActiveJobs'
)
CREATE TABLE [sqlagent].[ActiveJobs] (
	[RefreshKey]			INT
,	[JobName]				NVARCHAR(128)
,	[CurrentDuration]		INT				-- seconds
,	[ExecutionCount]		INT
,	[AverageDuration]		INT				-- seconds
,	[EstimatedCompletion]	SMALLDATETIME
)
GO


IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'sqlagent'
	AND TABLE_NAME = 'ExcludeActiveJobs'
)
CREATE TABLE [sqlagent].[ExcludeActiveJobs] (
	[JobID]			UNIQUEIDENTIFIER
)
GO


-- Exclude these jobs from the ActiveJobs
INSERT [sqlagent].[ExcludeActiveJobs] (
	[JobID]
)
SELECT
	[job_id]
FROM msdb.dbo.sysjobs
WHERE [name] = 'SQL AGENT DATA MART ETL';


IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'sqlagent'
	AND TABLE_NAME = 'ActiveJobsRefresh'
)
CREATE TABLE [sqlagent].[ActiveJobsRefresh] (
	[RefreshKey]			INT
,	[RefreshDate]			SMALLDATETIME
)
GO


CREATE OR ALTER PROCEDURE [sqlagent].[GetActiveJobs]
AS
BEGIN
	DECLARE 
		@SESSION_ID	INT
	,	@REFRESH_KEY			INT = NEXT VALUE FOR [sqlagent].[RefreshKey]
	,	@REFRESH_DATE			SMALLDATETIME = CONVERT(SMALLDATETIME, GETDATE());

	SELECT
		@SESSION_ID = MAX(session_id)
	FROM msdb.dbo.syssessions;

	;WITH CTE_JOBS_RUNNING AS (
		SELECT
			j.[name]
		,	a.[job_id]
		,	a.[start_execution_date]
		,	a.[last_executed_step_id]
		,	a.[last_executed_step_date]
		FROM msdb.dbo.sysjobactivity a
		JOIN msdb.dbo.sysjobs j ON j.job_id = a.job_id
		LEFT JOIN [sqlagent].[ExcludeActiveJobs] e
		ON e.[JobID] = a.[job_id]
		WHERE a.session_id = @SESSION_ID
		AND a.start_execution_date IS NOT NULL
		AND stop_execution_date IS NULL
		AND e.[JobID] IS NULL
	)
	,	
	CTE_JOB_HISTORY AS (
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
		FROM CTE_JOBS_RUNNING a
		JOIN msdb.dbo.sysjobhistory h
		ON h.[job_id] = a.[job_id]
		WHERE h.[step_id] = 0
		AND h.[run_status] = 1
	)
	,
	CTE_JOB_AVERAGE_DURATION AS (
		SELECT 
			[job_id]
		,	COUNT(*) AS [execution_count]
		,	AVG([duration_seconds])	AS [average_duration]
		FROM CTE_JOB_HISTORY
		GROUP BY [job_id]
	)

	INSERT [sqlagent].[ActiveJobs] (
		[RefreshKey]
	,	[JobName]
	,	[CurrentDuration]
	,	[ExecutionCount]
	,	[AverageDuration]
	,	[EstimatedCompletion]
	)
	SELECT
		@REFRESH_KEY
	,	j.[name]	AS [JobName]
	,	DATEDIFF(second, j.[start_execution_date], GETDATE())	AS [current_duration]
	,	d.[execution_count]
	,	d.[average_duration]
	,	CONVERT(
			SMALLDATETIME
		,
			DATEADD(
				second
			,	d.[average_duration] - DATEDIFF(second, j.[start_execution_date], GETDATE())
			, j.[start_execution_date]
			) 
		) AS [estimated_completion]
	FROM CTE_JOBS_RUNNING j
	LEFT JOIN CTE_JOB_AVERAGE_DURATION d
	ON d.[job_id] = j.[job_id];

	INSERT [sqlagent].[ActiveJobsRefresh] (
		[RefreshKey]
	,	[RefreshDate]
	)
	SELECT
	 	@REFRESH_KEY
	,	@REFRESH_DATE;
END
GO


CREATE OR ALTER VIEW [sqlagent].[vActiveJobs]
AS
	SELECT 
		[RefreshKey]
	,	[JobName]
	,	[CurrentDuration]
	,	[ExecutionCount]
	,	[AverageDuration]
	,	[EstimatedCompletion]
	FROM [sqlagent].[ActiveJobs]
	WHERE [RefreshKey] = (
		SELECT 
			MAX([RefreshKey])
		FROM [sqlagent].[ActiveJobsRefresh]
	);
GO






