/*

-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Arseny Birukov arsbir@microsoft.com


Процедура создает строчку в таблице Workers и пару джобов, необходимых для работы 
The procedure creates row in Workers table and a couple of jobs to run VBMS
*/



CREATE PROCEDURE [dbo].[CreateWorkerAndJob]
	@database_name varchar(500)    ,      --Database to check.
	@fill_queue_time varchar(5)    ,      --db analysis start time
	@start_worker_time varchar(5)  ,      --maintenance window start time
	@total_time int                ,      --Duration in minutes
	@worker_name nvarchar(255)     = NULL     --Leave NULL to set same as db name
AS
---------------------------------------------------------------------------------------------------------

IF DB_ID(@database_name) is null
	RAISERROR('Database %s does not exist',16,1,@database_name)

IF @fill_queue_time NOT LIKE '[0-9][0-9]:[0-9][0-9]'
	RAISERROR('@fill_queue_time should be in HH:MM format',16,1)

IF @start_worker_time NOT LIKE '[0-9][0-9]:[0-9][0-9]'
	RAISERROR('@start_worker_time should be in HH:MM format',16,1)

SET @worker_name = ISNULL(@worker_name,@database_name)

DELETE FROM [dbo].[Workers] WHERE worker_name = @worker_name

INSERT INTO [dbo].[Workers]
           ([worker_name]
           ,[date_added]
           ,[owner]
           ,[comment]
           ,[use_user_db]
           ,[use_system_db]
           ,[dbname_list]
           ,[except_list]
           ,[indexes]
           ,[stats]
           ,[checkall]
           ,[checktable]
           ,[checkalloc]
           ,[checkcatalog]
           ,[online_only]
           ,[afterparty]
           ,[add_stats_runtime]
           ,[totaltimemin]
           ,[indextime]
           ,[stattime]
           ,[checktime])
     VALUES 
           (@worker_name                     --Worker name
           ,getdate()                        --Creation date 
           ,SUSER_SNAME()                    --Creator
           ,'No comments'                    --Comment
           ,1                                --Process user databases
           ,0                                --0=do not process system databases
           ,@database_name                   --database name
           ,0                                --except_list=0 means that we don't want to exclude dbname from check
           ,1                                --perform index defragmentation
           ,1                                --recalculate statistics
           ,1                                --checkall=1 perform all dbcc checks
           ,1                                --checktable no effect if checkall=1
           ,1                                --checkalloc no effect if checkall=1
           ,1                                --checkcatalog no effect if checkall=1
           ,0                                --0 = perform both online and offline checks
           ,1                                --perform afterparty processing
           ,1                                --add stats during run time
           ,@total_time                      --maintenace window in minutes
           ,NULL                             --% time for index rebulds. Default value in Preferences table
           ,NULL                             --% time for statistics
           ,NULL)                            --% time for alloc

PRINT 'Worker added'
------------------------------------------------------------------------------------------------------------------------
-- 1st job

DECLARE 
	@ReturnCode INT,
	@job_id BINARY(16),
	@job_name nvarchar(300),
	@job_command  nvarchar(1000),
	@owner_login varchar(100),
	@job_start_time int

/****** Object:  Job [VBMS_FillQueueAll_CRM]    Script Date: 30.10.2014 14:49:56 ******/
BEGIN TRANSACTION


SET @ReturnCode = 0

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
	EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
END

SET @job_name = 'VBMS_FillQueueAll_'+@worker_name
SET @job_command = N'EXEC [dbo].[FillQueueAll] @dbname_list='''+@database_name+''''
SET @owner_login = SUSER_SNAME()
SET @job_start_time = DATEPART(HOUR,@fill_queue_time)*10000+DATEPART(MINUTE,@fill_queue_time)*100


IF (EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE name = @job_name))
BEGIN
	EXEC @ReturnCode = msdb.dbo.sp_delete_job @job_name = @job_name
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	PRINT 'Existing job deleted'
END

EXEC @ReturnCode =  msdb.dbo.sp_add_job 
		@job_name=@job_name,
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=@owner_login, 
		@job_id = @job_id OUTPUT

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

/****** Object:  Step [Fill Queue]    Script Date: 30.10.2014 14:49:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
		@job_id=@job_id, 
		@step_name=N'Fill Queue', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=@job_command, 
		@database_name=N'VBMS', 
		@flags=0

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_update_job 
	@job_id = @job_id, 
	@start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule
		@job_id=@job_id, 
		@name=@job_name, 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20141028, 
		@active_end_date=99991231, 
		@active_start_time=@job_start_time, 
		@active_end_time=235959

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobserver 
	@job_id = @job_id, 
	@server_name = N'(local)'

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

COMMIT TRANSACTION
GOTO EndSave

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:


PRINT 'Job 1 created'
------------------------------------------------------------------------------------------------------------------------
-- 2nd job

BEGIN TRANSACTION

SET @ReturnCode = 0
SET @job_name = 'VBMS_StartWorker_'+@worker_name
SET @job_command = N'EXEC [dbo].[StartWorker] '''+@database_name+''''
SET @job_start_time = DATEPART(HOUR,@start_worker_time)*10000+DATEPART(MINUTE,@start_worker_time)*100
SET @ReturnCode = 0
SET @job_id = NULL

IF (EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE name = @job_name))
BEGIN
	EXEC @ReturnCode = msdb.dbo.sp_delete_job @job_name = @job_name
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	PRINT 'Existing job deleted'
END

EXEC @ReturnCode =  msdb.dbo.sp_add_job 
		@job_name=@job_name, 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=@owner_login, 
		@job_id = @job_id OUTPUT

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback2

/****** Object:  Step [Start Worker]    Script Date: 30.10.2014 15:00:35 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
		@job_id=@job_id, 
		@step_name=N'Start Worker', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=@job_command, 
		@database_name=N'VBMS', 
		@flags=0

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback2

EXEC @ReturnCode = msdb.dbo.sp_update_job 
	@job_id = @job_id, 
	@start_step_id = 1

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback2

EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
		@job_id=@job_id, 
		@name=@job_name, 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20141028, 
		@active_end_date=99991231, 
		@active_start_time=@job_start_time, 
		@active_end_time=235959

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback2

EXEC @ReturnCode = msdb.dbo.sp_add_jobserver 
	@job_id = @job_id, 
	@server_name = N'(local)'

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback2

COMMIT TRANSACTION

GOTO EndSave2

QuitWithRollback2:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION

EndSave2:

PRINT 'Job 2 created'

GO




