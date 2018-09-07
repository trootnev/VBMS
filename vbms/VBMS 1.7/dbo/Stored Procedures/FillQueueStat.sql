/*

-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.
Author: Oleg Trutnev otrutnev@microsoft.com


Version 2.0.2.5

Russian:
Процедура добавления заданий пересчёта статистики в таблицу dbo.Tasks
При оценке актуальности статистики используется динамический порог,
работающий аналогично флагу трассировки 2371. Автор формулы Juergen Thomas.
Для приоретизации используется процент превышения порога перерасчёта статистики (поле rowmod_factor)

English:

Stored procedure creates tasks for statistics update in table dbo.Tasks.
Each task is a T-SQL Statement UPDATE STATISTICS(<Table name>) WITH FULLSCAN.
Dynamic threshold formula like in traceflag 2371 is used.
Formula is created by Juergen Thomas.
Tasks are sorted by rowmod_factor that is computed like a percent of threshold deviation.
Greater is worse.

*/
CREATE PROCEDURE [dbo].[FillQueueStat] @db INT ,@batch UNIQUEIDENTIFIER = null, @sample nvarchar(50) = 'FULLSCAN'
AS
DECLARE	@SQL NVARCHAR(MAX)
	,@tableid INT
	,@indexid INT
	,@partitionnum INT
	,@subsystem_id INT
	,@actiontype INT
	,@entryid BIGINT

IF @batch is null
SET @batch = newid()

IF @sample not like N'SAMPLE % ROWS' AND @sample not like N'FULLSCAN' AND @sample not like N'RESAMPLE' AND @sample not like N'SAMPLE % PERCENT'
BEGIN SET @sample = N'RESAMPLE' --@sample parameter control. 
PRINT N'Warning! Incorrect sample selected! Using RESAMPLE!'
END



DELETE --delete old tasks, because they are now useless.
FROM dbo.Tasks
WHERE subsystem_id = 2 --Подсистема обслуживания статистики / Statistics management subsystem_id tasks
	AND date_completed IS NULL
	AND [database_id] = @db
	AND [worker_name] IS NULL

SET @SQL = '
DECLARE @worker_name nvarchar(255)
SELECT @worker_name = worker_name
FROM VBMS.dbo.WorkerSessions
WHERE session_id = @@SPID

USE [' + db_name(@db) + '];
 INSERT VBMS.dbo.Tasks (
 batch_id
 ,subsystem_id
 ,action_type_id
 ,command
 ,date_added
 ,database_id
 ,table_id
 ,index_id
 ,size_mb
 ,rowcnt
 ,rowmod_factor)
 (
SELECT DISTINCT
''' + CAST(@batch AS NVARCHAR(50)) + '''
	,2
	,1
	,''USE [' + DB_NAME(@db) + 
	']
	UPDATE STATISTICS ['' + SCHEMA_NAME(so.schema_id) + ''].['' + OBJECT_NAME(so.object_id) +''] [''+ISNULL(s.name, '''') + ''] WITH '+@sample+''' AS command
	,getdate()
	,db_id()
	,so.object_id
	,s.stats_id
	,SUM(CAST(ps.used_page_count * 8 / 1024.00 AS DECIMAL(10, 3))) AS size_mb
	,ssi2.rows
	,CASE 
		WHEN ssi2.rows < 25000
			THEN (((ssi.rowmodctr + 0.0001) / (ssi2.rows + 0.000001)) * 100.00 - (ssi2.rows * 0.2 + 500) / (ssi2.rows +0.000001) * 100)
		ELSE (((ssi.rowmodctr + 0.0001) / (ssi2.rows + 0.000001)) * 100.00 - sqrt((ssi2.rows) * 1000.00) / (ssi2.rows + 0.000001) * 100.00)
		END as rowmod_factor
FROM sys.stats  s (NOLOCK)
join sys.dm_db_partition_stats ps (NOLOCK)  on s.object_id = ps.object_id and ps.index_id < 2
join sys.sysindexes ssi (NOLOCK)  on s.object_id = ssi.id and ssi.indid = s.stats_id
join sys.objects so (NOLOCK)  on so.object_id = s.object_id
AND so.type IN (
		N''U''
		,N''V''
		)
join sys.sysindexes ssi2 on s.object_id = ssi2.id and ssi2.indid < 2
join sys.indexes si  on s.object_id = si.object_id and si.index_id = s.stats_id and si.type in (1,2)
WHERE so.is_ms_shipped = 0
	AND
	(ssi2.rows > 500)
		AND ssi.rowmodctr > (
			CASE 
				WHEN (ssi2.rows < 25000)
					THEN (sqrt((ssi2.rows) * 1000))
				WHEN ((ssi2.rows) > 25000)
					THEN ((ssi2.rows) * 0.2 + 500)
				END
			)
--Blacklisting
AND not exists
(SELECT 1 
FROM VBMS.dbo.Blacklist bl
WHERE 
 (bl.database_id = db_id() 
 and (bl.table_id = so.object_id or bl.table_id is null))
 and (bl.index_id = s.stats_id or bl.index_id is null) 
and (bl.subsystem_id = 2 or bl.subsystem_id is null)
AND (bl.worker_name = @worker_name or worker_name is null)
and bl.enabled = 1
)
--Blacklisting
GROUP BY so.schema_id
	,so.object_id
	,s.stats_id
	,s.name
	,ssi.rowmodctr 
	,ssi2.rows
	
	
)'

EXEC(@SQL)


DECLARE TASKS CURSOR
FOR SELECT entry_id, table_id,index_id,partition_n,subsystem_id, action_type_id
FROM dbo.Tasks WHERE time_prognosis_s is null
and database_id = @db
and batch_id = @batch


OPEN TASKS

FETCH NEXT FROM TASKS
INTO @entryid,@tableid,@indexid,@partitionnum,@subsystem_id,@actiontype

WHILE @@FETCH_STATUS = 0 
BEGIN 

UPDATE dbo.Tasks
SET time_prognosis_s = ISNULL(dbo.GetTimeFactor(@db,1,@tableid,@indexid,@partitionnum,@subsystem_id, @actiontype,60,1),0) * size_mb
WHERE entry_id = @entryid
FETCH NEXT FROM TASKS
INTO @entryid,@tableid,@indexid,@partitionnum,@subsystem_id,@actiontype


END

CLOSE TASKS
DEALLOCATE TASKS
