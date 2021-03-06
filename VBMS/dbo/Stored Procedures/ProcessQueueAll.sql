﻿/*
-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Oleg Trutnev otrutnev@microsoft.com


Russian:

Процедура обработки очереди заданий в таблице dbo.Tasks.
Задания делятся по подсистемам (subsystem) и типам операций (action_type). Описание в таблице dbo.OperationTypes.
Задания обрабатываются в следующем порядке подсистем: 
1. Обслуживание индексов
2. Обслуживание статистики с предварительным формированием заданий, дабы не обновлять статистику, обновлённую при перестройке индексов.
3. Проверка целостности.
Порядок подсистем неизменен, т.к. представляется единственно разумным.
Внутри каждой подсистемы задания упорядочиваются отдельным образом.
Ведётся учёт остатка времени и предсказание продолжительности операций за счёт статистики прошлых выполнений за месяц по тем же объектам.
Общий лимит времени делится в процентах между подсистемами. Настройки в таблице dbo.Parameters.
После каждой операции выполняется запись показателей времени и результата для дальнейшего использования в прогнозах времени выполнения аналогичных операций.

English:

Stored procedure processes the queue of tasks form dbo.Tasks table.
Tasks are devided by subsystems and action types.
Tasks are processed in specific order:
1. Index maintenance
2. Satistics update (tasks formed prior to execution to exclude stats updated during index rebuild on step 1)
3. Table integrity checks

In each subsystem_id taks are ordered different.
Time is tracked and each operation time consumption is predicted by calculating previous execution times and extropolating it on current size of object.
Total maintenance time is devided between subsystems in percents. See dbo.Parameters.
After each task its duration is saved for future use.

*/

CREATE PROCEDURE [dbo].[ProcessQueueAll] 
	@use_user_db int =1 --process tasks for all online user DB's
	,@use_system_db int = 0 --process tasks for all system DB's
	,@dbname_list nvarchar(500) = '' --list of DB's.
	,@except_list bit = 0 --DB selection inversion
	,@indexes bit = 1
	,@stats bit = 1
	,@checkall bit = 1
	,@checktable bit = 1
	,@checkalloc bit = 1
	,@checkcatalog bit = 1
	,@online_only bit = 1 --perform online operations only
	,@check_backup_state bit = 1
	,@check_ag_state bit = 1
	,@ag_max_queue bigint
	,@afterparty int = 1 --redistribute remaining time in the end. Ex: We spend not all the time during stats update and this time can be used for rebuilding indexes after all operations pass.
	,@add_stats_runtime bit = 1 --It's better to add update statistics tasks after index maintenance, but you may deactivate this feature by passing 0 here.
	,@worker_name nvarchar(255)
	,@total_time BIGINT
	,@index_time BIGINT
	,@check_time BIGINT
	,@stat_time BIGINT
	,@exec_guid uniqueidentifier

AS

SET NOCOUNT ON

DECLARE 
	@command NVARCHAR(max) --выполняемое выражение / command to execute
	,@entry_id INT
	,@subsystem_id INT = 1 --Индексы обслуживаются первыми / Index maintenance first
	,@t1 DATETIME --две переменных @t1 и @t2 нужны для промежуточных расчётов времени / vars for time tracking
	,@finish_time datetime
	,@action_type_id INT
	,@size_mb INT
	,@timefactor FLOAT
	,@table_id INT
	,@index_id INT
	,@partition_number INT
	,@msg NVARCHAR(max)
	,@retention datetime
	,@db int
	,@deadlock_flag bit
	,@exit_code INT
	,@stoplight BIT
	,@log_space BIT
	,@log_space_threshold_mb bigint
	,@error_count int = 0 
	,@null_flag bit = 0
	,@db_backup bit = 0
	,@ag_state bit = 0

	
--Обработка заданий по БД, которых уже нет / Handeling tasks for non-existent DB's

UPDATE dbo.Tasks
SET 
	result = 'DB not found'
	,date_started = date_added
	,date_completed = date_added
where 
	date_completed is null
	AND DB_NAME([database_id]) is null


--Removing the stoplight if it is set somehow.
UPDATE dbo.Workers
SET stoplight = 0
WHERE 
	worker_name = @worker_name
	AND stoplight <> 0

--Row in WorkerSessions might remain from terminated execution
DELETE FROM dbo.[WorkerSessions] 
WHERE worker_name = @worker_name AND session_id = @@SPID

INSERT dbo.[WorkerSessions] (worker_name,session_id, program_name)
select @worker_name,@@SPID, program_name from sys.dm_exec_sessions where session_id=@@SPID

--Инициализация параметров / Loading parameters
IF (@index_time is not null or @stat_time is not null or @check_time is not null)
	BEGIN
	SET @null_flag = 1
	END

IF @total_time is null
BEGIN
	SELECT @total_time = int_value * 60 --перевод из минут в миллисекунды / from minutes to milliseconds
	FROM dbo.Parameters
	WHERE parameter = 'TotalMaintenanceWindowMm' --общее время на обслуживание / Total maintenace time
END
ELSE
	BEGIN
	SET @total_time = @total_time * 60
	END

--Print 'Total time:'
--Print @total_time

IF @index_time is null and @null_flag = 0
BEGIN
	SELECT @index_time = @total_time * int_value / 100
	FROM --расчёт значений в миллисекундах, параметры хранятся в процентах от общего времени. / From percents of total time to milliseconds.
	dbo.Parameters
	WHERE parameter = 'IndexMaintenanceWindowPercent' --время, отведённое на обслуживание индексов. Параметр хранится в процентах от общего времени./ Index maintenance time. Stored as percents of total maintenance time
END
ELSE IF @index_time is null and @null_flag = 1
	SET @index_time = 0
ELSE
	SET @index_time = @total_time * @index_time / 100


--Print 'Index time'
--Print @index_time

IF @check_time is null and @null_flag = 0
BEGIN
	SELECT @check_time = @total_time * int_value / 100
	FROM dbo.Parameters
	WHERE parameter = 'CheckWindowPercent' --время, отведённое на проверку целостности. Параметр хранится в процентах от общего времени. / Integrity checks time. Stored as percents of total maintenance time
END
ELSE IF
	@check_time is null and @null_flag = 1
	SET @check_time = 0
ELSE 
	SET @check_time = @total_time * @check_time / 100


--Print 'Check Time'
--Print @check_time

IF @stat_time is null and @null_flag = 0
BEGIN
	SELECT @stat_time = @total_time * int_value / 100
	FROM dbo.Parameters
	WHERE parameter = 'StatMaintenanceWindowPercent' --время, отведённое на обслуживание статистки. Параметр хранится в процентах от общего времени. / Statistics maintenance time. Stored as percents of total maintenance time
END
ELSE IF @stat_time is null and @null_flag = 1
	SET @stat_time = 0
ELSE
	SET @stat_time = @total_time * @stat_time / 100


--Print 'Stat time'
--Print @stat_time

IF @stat_time+@index_time+@check_time>@total_time
	BEGIN
	RAISERROR('Error during parameters check. Sum of index_time, check_time and stat_time cannot be more than total time!',16,1)
	RETURN
	END




SELECT @log_space_threshold_mb = int_value
FROM dbo.Parameters 
WHERE parameter = 'TranLogSpaceThresholdMb'

--Формирование списка обслуживаемых БД \ Creating a list of DB being optimized

DECLARE @db_list TABLE
(
	database_id int
)

INSERT @db_list
SELECT database_id 
FROM dbo.GetDbList(@use_system_db, @use_user_db, @dbname_list, @except_list) t



DECLARE @operation_types TABLE (
	subsystem_id int,
	action_type_id int,
	finish_time datetime --Время, до которого можно исполнять задания такого типа
)

DECLARE @start_time datetime = getdate()

INSERT INTO @operation_types SELECT 1,1,DATEADD(s, @index_time, @start_time) WHERE @indexes = 1 AND @online_only = 0
INSERT INTO @operation_types SELECT 1,2,DATEADD(s, @index_time, @start_time) WHERE @indexes = 1
INSERT INTO @operation_types SELECT 1,3,DATEADD(s, @index_time, @start_time) WHERE @indexes = 1

INSERT INTO @operation_types SELECT 2,1,DATEADD(s, @index_time+@stat_time, @start_time) WHERE @stats = 1

INSERT INTO @operation_types SELECT 3,1,DATEADD(s, @index_time+@stat_time+@check_time, @start_time) WHERE @checkall = 1 OR @checkcatalog = 1
INSERT INTO @operation_types SELECT 3,2,DATEADD(s, @index_time+@stat_time+@check_time, @start_time) WHERE @checkall = 1 OR @checkalloc = 1
INSERT INTO @operation_types SELECT 3,3,DATEADD(s, @index_time+@stat_time+@check_time, @start_time) WHERE @checkall = 1 OR @checktable = 1


SET @afterparty = ~CAST(@afterparty  as bit)  --Инвертируем afterparty. 0 - два прогона цикла, т.е. обычный+afterparty, 1 - один прогон цикла без afterparty.

 --При первой итерации этого цикла происходит обычная обработка,
 --а при второй - !!AFTERPARTY!! (dancing smile) (drunken smile)
 --First iteration is normal mode, second - afterparty

WHILE @afterparty < 2 
BEGIN

	SET @subsystem_id = 1

	--Цикл по подсистемам. Нужен чтобы между индексами и статистикой обновить задачи на статистику
	WHILE @subsystem_id < 4
	BEGIN

		--Очередь заданий по сбору статистики следует формировать сразу после перестройки индексов, т.к. часть статистик будет уже обновлена, а часть ещё нет.  
		--Statistics maintenance tasks should be added right before execution, because some of statistics will be updated during index maintenance.
		IF @subsystem_id = 2 and @add_stats_runtime = 1 and @stats = 1
		BEGIN

			DECLARE DB CURSOR
			FOR SELECT database_id from @db_list
				
			OPEN DB

			FETCH NEXT FROM DB INTO @db

			WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC [dbo].[FillQueueStat] @db
				FETCH NEXT FROM DB INTO @db
			END

			CLOSE DB
			
			DEALLOCATE DB
		
		END

		--This cursor iterates over tasks
		DECLARE command CURSOR
		FOR
		SELECT 
			t1.entry_id
			,t1.[database_id]
			,t1.command
			,t1.subsystem_id
			,t1.action_type_id
			,t1.size_mb
			,t1.table_id
			,t1.index_id
			,t1.partition_n
			,t2.finish_time
		FROM 
			dbo.Tasks t1
			INNER JOIN @operation_types t2 ON t1.subsystem_id = t2.subsystem_id AND t1.action_type_id = t2.action_type_id
		WHERE 1=1 
			AND t1.subsystem_id = @subsystem_id
			AND date_started IS NULL
			AND date_completed IS NULL --невыполненные задания заданного типа / Not completed tasks of each type
			AND [database_id] in (select database_id from @db_list) --Только указанные БД / Selected DBs only
		ORDER BY 
			CASE 
				WHEN @subsystem_id = 3 THEN t1.action_type_id
				ELSE NULL
			END
			,t1.[priority] --Этот столбец определяет, в каком порядке выполнять таски внутри группы.
			, CASE --для каждой подсистемы свой порядок сортировки заданий / Each subsystem_id uses it's own tasks sorting
				WHEN @subsystem_id = 1
					THEN entry_id
				END --просто по порядку, задания в очереди в порядке убывания критичности. / by entry_id. Tasks have been sorted while insertion.
			,CASE 
				WHEN @subsystem_id = 2
					THEN rowmod_factor
				END DESC -- в порядке убывания значения превышения порога перерасчёта статистики. rowmod_factor - это на сколько процентов превышен порог. / By rowmod_factor. It's a percent of dynamic threshold violation.
			,CASE 
				WHEN @subsystem_id = 3
					THEN checked_daysago
				END DESC --В порядке убывания количества дней с момента последней проверки / In descending order of days since last integrity check.
			,CASE 
				WHEN @subsystem_id = 3
					AND checked_daysago IS NULL
					THEN size_mb --если проверки не проводилось, то начинаем с самых маленьких / If tables has not been checked at all, then start with the smallest.
				END

		OPEN command

		FETCH NEXT
		FROM command
		INTO @entry_id
			,@db
			,@command
			,@subsystem_id
			,@action_type_id
			,@size_mb
			,@table_id
			,@index_id
			,@partition_number
			,@finish_time


		WHILE @@FETCH_STATUS = 0
		BEGIN

			IF NOT EXISTS (SELECT 1 FROM dbo.Workers WHERE worker_name = @worker_name and stoplight = 0)
			BEGIN
				SET @stoplight = 1
				BREAK
			END

			IF EXISTS (select 1 from dbo.Tasks (nolock) where entry_id = @entry_id and (worker_name is NULL or worker_name = @worker_name)) --Task is not started by another worker
			BEGIN
		
				IF @subsystem_id  = 1 --Index operations may require some logspace.
					BEGIN
					EXEC @log_space = dbo.CheckLogSpaceLeft @db, @log_space_threshold_mb 
					IF @check_backup_state = 1
						BEGIN
						SELECT @db_backup = dbo.GetDBBackupState(@db) --check if backup is running for current database
						END
						ELSE SET @db_backup = 0	
					IF (@@MicrosoftVersion/0x01000000)>=11 and @check_ag_state = 1
						BEGIN 
						SELECT @ag_state = dbo.GetAGSyncState(@db,@ag_max_queue)
						END
					ELSE SET @ag_state = 0
					END
				ELSE 
					BEGIN
					SET @log_space = 1 --Stats operations does not generate so many log records
					SET @db_backup = 0
					SET @ag_state = 0
					END
				SET @t1 = getdate() --засекаем время начала шага / Save the task beginning time

				-- рассчитываем сколько времени может потребоваться для выполнения данного шага
				-- на основе сведений о продолжительности прошлых выполнений аналогичной операции над тем же объектом
				-- Predicting how much time step can take basing on similar tasks execution time (same subsystem, same action type and same object).
				SELECT @timefactor = ISNULL(dbo.GetTimeFactor(@db,1,@table_id,@index_id,@partition_number,@subsystem_id, @action_type_id,60,1),0) 

				IF	DATEADD(s, (@size_mb * @timefactor), @t1) < @finish_time 
					AND @log_space = 1 and @db_backup = 0 and @ag_state = 0--если остатка времени и места под логи предположительно хватит на выполнение команды и бэкап не помешает. / Check if we have enough time and log space to execute the task adn the database backup will not interfere
				BEGIN
	
					UPDATE dbo.Tasks
					SET 
						date_started = getdate()
						,worker_name = @worker_name
						,[execution_id] = @exec_guid
					WHERE 
						entry_id = @entry_id

					SET @msg = 'Ok' --Если не произойдёт ошибки, то результатом выполнения операции будет Ok. / Setting Ok as a default exit message.
					SET @deadlock_flag = 0
					SET @exit_code = 1

					BEGIN TRY
						UPDATE dbo.[WorkerSessions]
						SET 
							subsystem_id = @subsystem_id, 
							entry_id = @entry_id
						WHERE 
							worker_name = @worker_name
						--PRINT @command
						EXEC (@command)
					END TRY

					BEGIN CATCH

						SET @exit_code = ERROR_NUMBER()
				
						IF ERROR_NUMBER() in (1205,1222,1912)
							SET @deadlock_flag = 1
				
						SET @msg = N'Error code: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10)) + N'. Error message:' + ERROR_MESSAGE()

						PRINT 'Error execuring "'+@command+'"   '+@msg

						SET @error_count += 1 --Global flag indicating that there were at least one error

					END CATCH

					--IF @msg = 'Ok'
					--BEGIN
					--SET @msg = 'Ok' + '. Time prognose was: '+CAST((@size_mb * @timefactor) as NVARCHAR(50)) + ' sec.'
					--END

					UPDATE dbo.Tasks
					SET 
						date_completed = GETDATE()
						,duration_s = DATEDIFF(ss, date_started, GETDATE())
						,time_factor = CASE WHEN @exit_code = 1 THEN (DATEDIFF(ms, date_started, GETDATE()) / 1000.0 / (size_mb + 1.0)) ELSE NULL END
						,result = @msg
						,exit_code = @exit_code
					WHERE 
						entry_id = @entry_id
	
					SELECT @stoplight = stoplight FROM dbo.Workers WHERE worker_name = @worker_name
		
				END 
				ELSE
				BEGIN

					IF @log_space = 0 --If not enougth log space
						UPDATE dbo.Tasks
						SET 
							worker_name = @worker_name
							,[execution_id] = @exec_guid
							,date_started = GETDATE()
							,date_completed = GETDATE()
							,duration_s = 0
							,result = 'Skipped. Not enough transaction log space'
							,exit_code = -2
						WHERE 
							entry_id = @entry_id

					IF @db_backup = 1--If backup is running and we need to stop due to that
						UPDATE dbo.Tasks
						SET 
							worker_name = @worker_name
							,[execution_id] = @exec_guid
							,date_started = GETDATE()
							,date_completed = GETDATE()
							,duration_s = 0
							,result = 'Skipped. Database Backup is running and this worker is configured to skip in this case'
							,exit_code = -2
						WHERE 
							entry_id = @entry_id
					IF @ag_state =1--If AG is not healthy or can't keep up with the pace
						UPDATE dbo.Tasks
						SET 
							worker_name = @worker_name
							,[execution_id] = @exec_guid
							,date_started = GETDATE()
							,date_completed = GETDATE()
							,duration_s = 0
							,result = 'Skipped. One of the AG replicas cant keep up with the log growth or is unhealthy'
							,exit_code = -2
						WHERE 
							entry_id = @entry_id
					

					IF DATEADD(SECOND, (@size_mb * @timefactor ), @t1) > @finish_time  --Недостаточно времени 
						AND @afterparty = 1                                             --Мы уже на второй итерации (на первой мы просто пропускаем таск)
						UPDATE dbo.Tasks
						SET 
							worker_name = @worker_name
							,[execution_id] = @exec_guid
							,date_started = GETDATE()
							,date_completed = GETDATE()
							,duration_s = 0
							,result = 'Skipped. Not enough time left'
							,exit_code = -1
						WHERE 
							entry_id = @entry_id
				END
		
			END --If task started by another worker
			-- We simply skip those tasks, because other worker "owns" them 

		
			FETCH NEXT
			FROM command
			INTO @entry_id
				,@db
				,@command
				,@subsystem_id
				,@action_type_id
				,@size_mb
				,@table_id
				,@index_id
				,@partition_number
				,@finish_time
		END

		CLOSE command

		DEALLOCATE command

		IF @stoplight = 1
			BREAK

		SET @subsystem_id += 1
	END
	
	IF @stoplight = 1
		BREAK

	IF @afterparty = 0
	BEGIN
		--Мы прогнали основной цикл один раз, потратив на каждую из стадий отведенное время
		--При этом, возможно мы пропустили некоторые шаги т.к. кончилось время соответствующей стадии
		--Теперь мы можем повторить все еще раз и все-таки выполнить их

		--Снимаем постадийное ограничение
		UPDATE @operation_types
		SET	finish_time = (SELECT max(finish_time) FROM @operation_types)
	
	END

	SET @afterparty = @afterparty + 1 --После второго захода надо завязывать, иначе потом будет болеть голова

END


--Праздник закончился, пора наводить порядок.
--В норме к этому моменту не должно остаться ни одного необработанного таска, кроме как в случае когда мы принудительно остановили воркер
--Тем не менее, здесь мы проверяем, не осталось ли таких тасков и прописываем в них коды ошибок.

--Afterparty is over. It's time to clean up.
--Normally, there should be no unprocessed tasks at this point, only exclusion is if the worker was stopped via stoplight.
--Here we set errorcode to all unprocessed tasks to simplify troubleshoting

UPDATE t1
SET 
	worker_name = @worker_name
	,[execution_id] = @exec_guid
	,date_started = GETDATE()
	,date_completed = GETDATE()
	,duration_s = 0
	,result = CASE WHEN @stoplight = 0 THEN 'Skipped. Reason is unknown' ELSE 'Skipped. User requested to stop' END
	,exit_code = CASE WHEN @stoplight = 0 THEN -4 ELSE -3 END -- There should be no -4 at all! -4 means some bug!
FROM 
	dbo.Tasks t1
	INNER JOIN @operation_types t2 ON t1.subsystem_id = t2.subsystem_id AND t1.action_type_id = t2.action_type_id
WHERE 
	date_completed is null
	and [database_id] in (select database_id from @db_list)
	and worker_name is null

IF @stoplight = 1
BEGIN
	UPDATE dbo.Workers
	SET stoplight = 0
	WHERE worker_name = @worker_name
	AND stoplight <> 0
END

--Calculate the expected execution time (for consistency only due to some stats tasks where generated runtime)

DECLARE TASKS CURSOR
FOR SELECT entry_id, [database_id], table_id,index_id,partition_n,subsystem_id, action_type_id
FROM dbo.Tasks WHERE time_prognosis_s is null


OPEN TASKS

FETCH NEXT FROM TASKS
INTO @entry_id,@db,@table_id,@index_id,@partition_number,@subsystem_id,@action_type_id

WHILE @@FETCH_STATUS = 0 
BEGIN 

UPDATE dbo.Tasks
SET time_prognosis_s = ISNULL(dbo.GetTimeFactor(@db,1,@table_id,@index_id,@partition_number,@subsystem_id, @action_type_id,60,1),0) * size_mb
WHERE entry_id = @entry_id
FETCH NEXT FROM TASKS
INTO @entry_id,@db,@table_id,@index_id,@partition_number,@subsystem_id,@action_type_id

END

CLOSE TASKS
DEALLOCATE TASKS




DELETE FROM dbo.[WorkerSessions]
WHERE worker_name = @worker_name

RETURN @error_count