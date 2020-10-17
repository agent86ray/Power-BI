/*
	LAUNCH SQL AGENT JOBS and VIEW POWER BI DASHBOARD

	SQL AGENT JOB BSSUG - SQL AGENT DATA MART ETL runs every minute

	SAMPLE SQL AGENT JOBS to run:
	CRM ETL
	DEMO 15-25 MINUTES
	DEMO FOR SQL AGENT DATA MART
	ONE-TWO-MINUTE-JOB
*/

EXEC msdb.dbo.sp_start_job @job_name = 'CRM ETL'

EXEC msdb.dbo.sp_start_job @job_name = 'DEMO 15-25 MINUTES'

EXEC msdb.dbo.sp_start_job @job_name = 'ONE-TWO-MINUTE-JOB';


-- show details on the jobs currently running

SELECT *
FROM [dbo].[vActiveJobs]