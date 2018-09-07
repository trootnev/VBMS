/*

-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Oleg Trutnev otrutnev@microsoft.com
		Arseny Birukov arsbir@microsoft.com

Procedure is a wrapper for FillQueueXXX procedures that passes neccessary parameters to them.

*/
CREATE PROCEDURE [dbo].[FillQueueAll] 
	-- Add the parameters for the stored procedure here
	@worker_name nvarchar(50) = NULL, --generate tasks only for a specific worker profile.
	@indexes bit = 1,
	@async_frag_collection bit =1,
	@collect_frag_now int =1, -- 0-dont collect frag data, only prepare indexes list, 1- prepare indexes list, collect frag data and generate tasks in this session. 2 - Indexes list with frag data has been filled already, need just to prepare the tasks. 
	@stats bit = 0,
	@stats_sample nvarchar(50) = 'RESAMPLE',
	@checkall bit = 1,
	@checktable bit = 0,
	@table_max_size_mb bigint = 1000000,
	@checkalloc bit = 0,
	@checkcatalog bit = 0,
	@use_user_db bit = 1,
	@use_system_db bit = 0,
	@dbname_list nvarchar(500) = '',
	@except_list bit =0, --invert database selection list
	@sortintempdb bit = NULL,
	@ix_maxdop int = NULL, --override global settings maxDOP.	
	@clear_latch bit = 0
	,@debug bit = 0
	
	
AS
BEGIN
	
	SET NOCOUNT ON;
	
	DECLARE 
		@dbint_id int
		,@batch UNIQUEIDENTIFIER
		,@db INT
		,@tableid INT
		,@indexid INT
		,@partitionnum INT
		,@subsystem_id INT
		,@actiontype INT
		,@entryid BIGINT
		,@frag_collection_time_limit_s int
		,@retention DATETIME

	SET @batch = NEWID()

	IF @worker_name is NOT NULL
		IF (select latched_spid FROM dbo.Workers WHERE worker_name = @worker_name) in (0,@@SPID) or @clear_latch = 1
		BEGIN
			SELECT 
				@indexes = [indexes]
				,@stats = [stats]
				,@stats_sample =  stats_sample
				,@checkall = checkall
				,@checktable = checktable
				,@checkalloc = checkalloc
				,@checkcatalog = checkcatalog
				,@use_system_db = use_system_db
				,@use_user_db = use_user_db
				,@dbname_list = dbname_list
				,@except_list = except_list
				,@frag_collection_time_limit_s = frag_eval_time_limit_s
			FROM dbo.Workers
			WHERE worker_name = @worker_name

			UPDATE dbo.Workers
			SET latched_spid = @@SPID
			WHERE worker_name = @worker_name
		END
		ELSE
		BEGIN 
			RAISERROR('FillQueueAll is already running for this worker!',16,1);
			RETURN
		END
	
	DELETE FROM dbo.[WorkerSessions]
WHERE not exists(
	select 1 from sys.dm_exec_sessions es 
		where es.session_id = dbo.[WorkerSessions].session_id 
		and es.program_name COLLATE DATABASE_DEFAULT = dbo.[WorkerSessions].program_name COLLATE DATABASE_DEFAULT
		and es.status in ('running','runnable','suspended')
		and es.session_id <> @@SPID
		)
		
	
	IF @worker_name is not null
	INSERT dbo.[WorkerSessions] (worker_name,session_id, program_name,subsystem_id)
	select @worker_name,@@SPID, program_name,0 from sys.dm_exec_sessions where session_id=@@SPID

	IF @ix_maxdop IS NULL
		SELECT @ix_maxdop = int_value
		FROM dbo.Parameters
		WHERE parameter = 'MaxDop'

	IF @sortintempdb IS NULL
		SELECT @sortintempdb= int_value
		FROM dbo.Parameters
		WHERE parameter = 'SortInTempdb'


	IF @frag_collection_time_limit_s is NULL
		SELECT @frag_collection_time_limit_s = int_value
		FROM dbo.Parameters
		WHERE parameter = 'FragCollectionTimeLimitS'

	--Fill tasks table

	DECLARE DB CURSOR FORWARD_ONLY
	FOR 
	SELECT database_id 
	FROM dbo.GetDbList(@use_system_db, @use_user_db, @dbname_list, @except_list) t

	OPEN DB

	FETCH NEXT FROM DB INTO @dbint_id

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @indexes = 1 
			IF @async_frag_collection = 0
				EXEC [dbo].[FillQueueIndex] @dbint_id, @batch, @ix_maxdop, @sortintempdb
			ELSE 
				BEGIN
					DELETE
					FROM dbo.Tasks
					WHERE subsystem_id = 1
					AND date_completed IS NULL
					AND [database_id] = @dbint_id
					
				IF @collect_frag_now = 2
					BEGIN
					EXEC [dbo].[FillQueueIndex_async] @dbint_id,@batch,@ix_maxdop,@sortintempdb 
					END
				ELSE
				EXEC [dbo].[CollectIndexData] @dbint_id, @batch
				IF @collect_frag_now = 1
					BEGIN 
						EXEC [dbo].[CollectIndexFragData] @dbint_id,@frag_collection_time_limit_s,@batch,@ix_maxdop,@sortintempdb
						--EXEC [dbo].[FillQueueIndex_async] @dbint_id,@batch,@ix_maxdop,@sortintempdb
					END
				IF @collect_frag_now = 0
				PRINT 'Index frag data needs to be collected in a separate session according to @collect_frag_now = 0. 
				Collect frag data by running  dbo.CollectIndexFragData (may be run in several parallel sessions) and rerun FillQueueAll with @async_frag_collection = 1 and @collect_frag_now = 1'	
			END

		IF ((@checkall = 1 or @checktable = 1) and @checktable <> 0)
			EXEC [dbo].[FillQueueCheckTable] @dbint_id, @batch, @table_max_size_mb

		IF ((@checkall = 1 or @checkcatalog = 1 ) and @checkcatalog <> 0) 
			EXEC [dbo].[FillQueueCheckCatalog] @dbint_id, @batch

		IF ((@checkall = 1 or @checkalloc = 1) and @checkalloc <> 0)
			EXEC [dbo].[FillQueueCheckAlloc] @dbint_id, @batch

		IF (@stats = 1)
			EXEC [dbo].[FillQueueStat] @dbint_id, @batch, @sample = @stats_sample

		FETCH NEXT FROM DB INTO @dbint_id
	END

	CLOSE DB
	DEALLOCATE DB

	--Calculate the expected execution time

	DECLARE TASKS CURSOR
	FOR SELECT entry_id, [database_id], table_id,index_id,partition_n,subsystem_id, action_type_id
	FROM dbo.Tasks WHERE time_prognosis_s is null

	OPEN TASKS

	FETCH NEXT FROM TASKS
	INTO @entryid, @db, @tableid, @indexid, @partitionnum, @subsystem_id, @actiontype

	WHILE @@FETCH_STATUS = 0 
	BEGIN 

		UPDATE dbo.Tasks
		SET time_prognosis_s = ISNULL(dbo.GetTimeFactor(@db,1, @tableid, @indexid, @partitionnum, @subsystem_id, @actiontype, 60, 1), 0) * size_mb
		WHERE entry_id = @entryid

		FETCH NEXT FROM TASKS
		INTO @entryid, @db, @tableid, @indexid, @partitionnum, @subsystem_id, @actiontype

	END

	CLOSE TASKS

	DEALLOCATE TASKS

	UPDATE dbo.Workers
		SET latched_spid = 0
		WHERE worker_name = @worker_name

	SELECT @batch as BatchId

	IF @debug = 1
	BEGIN
		SELECT 'Frag analysis results' as [-----]
		SELECT * FROM dbo.FragmentationData where batch_id = @batch
		SELECT 'Tasks generated' as [-----]
		SELECT * from dbo.Tasks where batch_id = @batch
	END


--Удаление старых записей / Removing old records
IF EXISTS (SELECT 1 FROM dbo.Parameters WHERE parameter = 'HistoryRetentionDays' and int_value is not null )
	SELECT @retention = dateadd(DD,-int_value,getdate()) 
	FROM dbo.Parameters
	WHERE parameter = 'HistoryRetentionDays'

IF EXISTS (SELECT 1 FROM dbo.Parameters WHERE parameter = 'HistoryRetentionDays' and int_value is not null )
BEGIN
	DELETE FROM dbo.Tasks
	WHERE date_added < @retention

	DELETE FROM dbo.DBCCChecksLog
	WHERE log_date < @retention

	DELETE FROM dbo.FragmentationData
	WHERE collection_date < @retention
END
DELETE FROM dbo.WorkerSessions where worker_name = @worker_name and subsystem_id = 0

END