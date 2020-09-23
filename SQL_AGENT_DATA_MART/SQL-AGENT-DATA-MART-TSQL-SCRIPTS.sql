/*

	SQL_AGENT_DATA_MART T-SQL scripts

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


-- TO DO: add conditional create
CREATE SEQUENCE [sqlagent].[RefreshKey]  
    START WITH 1  
    INCREMENT BY 1;  
GO  


CREATE OR ALTER PROCEDURE [sqlagent].[SimulateJobStepDuration]
	@MINIMUM_MINUTES	INT = 1
,	@MAXIMUM_MINUTES	INT = 10
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

	SELECT @STEP_DURATION_SECONDS;

	--WAITFOR DELAY = '00:05';
END




	




