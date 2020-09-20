/*

	SQL_AGENT_DATA_MART T-SQL scripts

*/

USE [master]
GO


-- create database if it doesn't exist
IF DATABASEPROPERTYEX (N'SQL_AGENT_DATA_MART', N'Version') IS NULL
BEGIN
	CREATE DATABASE [SQL_AGENT_DATA_MART];
END
GO


USE [SQL_AGENT_DATA_MART]
GO


CREATE SEQUENCE dbo.RefreshKey  
    START WITH 1  
    INCREMENT BY 1;  
GO  


CREATE OR ALTER PROCEDURE dbo.[SQL_AGENT_DATA_MART_ETL]
AS
BEGIN
	DECLARE
		@TODAY			DATE = GETDATE()
	,	@SESSION_ID		INT
	,	@REFRESH_KEY	INT = NEXT VALUE FOR dbo.RefreshKey;

	SELECT
		@SESSION_ID = MAX(session_id)
	FROM msdb.dbo.syssessions;

	SELECT 
		@REFRESH_KEY	AS [RefreshKey]
	,	[job_id]
	,	[start_execution_date]
	,	[last_executed_step_id]
	,	[last_executed_step_date]
	,	*
	FROM msdb.dbo.sysjobactivity
	WHERE session_id = @SESSION_ID
	AND stop_execution_date IS NULL
	AND start_execution_date > @TODAY;


END
GO


EXEC dbo.[SQL_AGENT_DATA_MART_ETL];




	




