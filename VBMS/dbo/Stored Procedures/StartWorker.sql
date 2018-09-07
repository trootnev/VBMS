/*
-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Oleg Trutnev otrutnev@microsoft.com


Russian:
Хранимая процедура запуска заданий обслуживания с заранее сохраненным набором параметров. 
Сначала необходимо заполнить профиль в таблице Workers. Все поля обязательны, кроме date_added.
После сохранения профиля значение worker_name может быть использовано для запука задания с этими параметрами.

English:
Procedure starts maintenance tasks execution from queue with parameters, saved to dbo.Workers table.
All the fields there should be filled except date_added.
Then the worker_name can be used to start workers with this parameters.

*/

CREATE PROCEDURE [dbo].[StartWorker] 
	@worker_name nvarchar(255),
	@exec_guid uniqueidentifier = null
AS
SET NOCOUNT ON
DECLARE 
	@use_user_db1 bit
	,@use_system_db1 bit
	,@dbname_list1 nvarchar(500)
	,@except_list1 bit
	,@indexes1 bit
	,@stats1 bit 
	,@checkall1 bit
	,@checktable1 bit
	,@checkalloc1 bit
	,@checkcatalog1 bit
	,@online_only1 bit
	,@check_backup_state1 bit
	,@check_ag_state1 bit
	,@ag_max_queue1 bigint
	,@afterparty1 bit
	,@add_stats_runtime1 bit
	,@totaltimemin1 bigint
	,@indextime1 bigint
	,@stattime1 bigint
	,@checktime1 bigint

IF @exec_guid is null
	SET @exec_guid = NEWID()

IF NOT EXISTS (SELECT 1 FROM dbo.Workers where worker_name = @worker_name)
	BEGIN
	RAISERROR('No such worker registered! Please note the case of letters.',16,1)
	RETURN
	END
ELSE
BEGIN
SELECT 
	@use_user_db1 = [use_user_db]
	,@use_system_db1 =[use_system_db]
	,@dbname_list1 = [dbname_list]
	,@except_list1 =[except_list]
	,@indexes1 = [indexes]
	,@stats1 = [stats]
	,@checkall1 = [checkall]
	,@checktable1 = [checktable]
	,@checkalloc1 = [checkalloc]
	,@checkcatalog1 =[checkcatalog]
	,@online_only1 = [online_only]
	,@check_backup_state1 = [check_backup_state]
	,@check_ag_state1 =[check_ag_state]
	,@ag_max_queue1 = [ag_max_queue]
	,@afterparty1 = [afterparty]
	,@add_stats_runtime1 = [add_stats_runtime]
	,@totaltimemin1 = [totaltimemin]
	,@indextime1 = [indextime]
	,@stattime1 = [stattime]
	,@checktime1 = [checktime]
FROM 
	dbo.Workers
where 
	[worker_name] = @worker_name


--Clean WorkerSession. Just in case....	  
DELETE FROM dbo.[WorkerSessions]
WHERE not exists(
	select 1 from sys.dm_exec_sessions es 
		where es.session_id = dbo.[WorkerSessions].session_id 
		and es.program_name COLLATE DATABASE_DEFAULT = dbo.[WorkerSessions].program_name COLLATE DATABASE_DEFAULT
		and es.status in ('running','runnable','suspended'))


EXEC dbo.ProcessQueueAll
	@use_user_db = @use_user_db1
	,@use_system_db =@use_system_db1
	,@dbname_list = @dbname_list1
	,@except_list =@except_list1
	,@indexes = @indexes1
	,@stats = @stats1 
	,@checkall = @checkall1
	,@checktable = @checktable1
	,@checkalloc = @checkalloc1
	,@checkcatalog =@checkcatalog1
	,@online_only = @online_only1
	,@check_backup_state = @check_backup_state1
	,@check_ag_state = @check_ag_state1
	,@ag_max_queue = @ag_max_queue1
	,@afterparty = @afterparty1
	,@add_stats_runtime = @add_stats_runtime1
	,@worker_name = @worker_name
	,@total_time = @totaltimemin1
	,@index_time = @indextime1
	,@stat_time = @stattime1
	,@check_time = @checktime1
,@exec_guid = @exec_guid

END
