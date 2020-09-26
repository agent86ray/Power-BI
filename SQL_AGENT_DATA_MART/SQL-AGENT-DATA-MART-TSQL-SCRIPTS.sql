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






	




