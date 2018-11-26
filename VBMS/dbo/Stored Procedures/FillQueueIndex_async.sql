/*

-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: 
Oleg Trutnev otrutnev@microsoft.com
Arseny Birukov arsbir@microsoft.com


Russian:

Процедура формирования заданий обслуживания индексов на основе их состояния. 
Задания - выражения для обслуживания отдельных индексов - добавляются в таблицу dbo.Tasks
Применяется оценка процента фрагментации и возможности перестройки индекса онлайн.
Для приоритезации заданий используется частота обращений к индексу (только user_scan), фрагментация и размер индекса.  

English:

Stored procedure creates tasks for index maintenance based on their state and operational stats.
Tasks are T-SQL statements of index REBUILD/REORGANIZE, that are inserted in dbo.Tasks table.
Index is evaluated by frag percent, user scans counter and size. 
First goes the most used (user scans) index with higher fragmentation and the larger size.   

 
*/

CREATE PROCEDURE [dbo].[FillQueueIndex_async] 
	 @db INT
	,@batch UNIQUEIDENTIFIER
	,@maxdop INT = NULL
	,@sortintempdb BIT = NULL
	,@entry_id bigint = NULL
AS



DECLARE
	@reorg_threshold decimal(10,3),
	@frag_threshold bigint,
	@locktimeout bigint,
	@online_allowed bit,
	@SQL nvarchar(max),
	@index_minsize bigint
	
DECLARE @worker_name nvarchar(255)
SELECT @worker_name = worker_name
FROM dbo.WorkerSessions
WHERE session_id = @@SPID


--Считываем параметры / Loading parameters
SELECT @reorg_threshold = int_value
FROM dbo.Parameters
WHERE parameter = 'IndexReorgThresholdPercent'

SELECT @locktimeout = int_value
FROM dbo.Parameters
WHERE parameter = 'LockTimeoutMs'

SELECT @frag_threshold = int_value
FROM dbo.Parameters
WHERE parameter = 'IndexFragLowerThreshold'


SELECT @index_minsize = int_value
FROM dbo.Parameters
WHERE parameter = 'IndexMinimumSizeMB'

IF @maxdop is null
SELECT @maxdop = int_value
		FROM dbo.Parameters
		WHERE parameter = 'MaxDop'

IF @sortintempdb IS NULL
		SELECT @sortintempdb= int_value
		FROM dbo.Parameters
		WHERE parameter = 'SortInTempdb'


IF (CAST(SERVERPROPERTY('Edition') as NVARCHAR(50)) LIKE N'%Enterprise%'
  OR CAST(SERVERPROPERTY('Edition') as NVARCHAR(50)) LIKE N'%Datacenter%'
  OR CAST(SERVERPROPERTY('Edition') as NVARCHAR(50)) LIKE N'%Developer%')
	SELECT @online_allowed = int_value
	FROM dbo.Parameters
	WHERE parameter = 'IndexOnlineRebuild'
ELSE 
	SET @online_allowed = 0
	

IF OBJECT_ID('tempdb..#partitions') is not null
	DROP TABLE #partitions

CREATE TABLE #partitions
(
	database_id int, 
	schema_id int,  
	object_id int, 
	object_name nvarchar(4000),
	index_id int,
	index_name nvarchar(4000),
	dataspace_name nvarchar(255),
	allow_page_locks bit,
	legacy_col_count int,
	xml_col_count int,
	user_scans bigint,
	partition_count int,
	partition_number int,
	row_count bigint,
	size_mb decimal(10,3),
	avg_fragmentation_in_percent decimal(10,3)
)

INSERT INTO #partitions
SELECT 
database_id,
schema_id,
object_id,
object_name,
index_id,
index_name,
dataspace_name,
allow_page_locks,
legacy_col_count,
xml_col_count,
user_scans,
partition_count,
partition_number,
row_count,
size_mb,
avg_fragmentation_in_percent
FROM
[dbo].[FragmentationData] with (FORCESEEK)
WHERE
(entry_id = @entry_id
OR
(@entry_id is NULL and 
database_id = @db
and analysis_status = 1))
AND avg_fragmentation_in_percent > @frag_threshold



INSERT dbo.Tasks (
	batch_id
	,subsystem_id
	,action_type_id
	,command
	,date_added
	,[database_id]
	,table_id
	,index_id
	,partition_n
	,[maxdop]
	,ix_frag
	,user_seeks
	,user_scans
	,user_updates
	,rowcnt
	,size_mb
	,[priority]
	,dataspace_name
)
SELECT
	@batch,
	1,  --index maintenance
	t.action_type_id,
	t.command,
	getdate(),
	@db,
	t.object_id,
	t.index_id,
	t.partition_number,
	t.maxdop,
	t.avg_fragmentation_in_percent,
	NULL, --removed seeks - useless
	t.user_scans,
	NULL, --removed updates
	t.row_count,
	t.size_mb,
	CASE WHEN ISNULL(pt.exit_code,1) <> 1 THEN 2 ELSE 1 END --Если предыдущий таск завершился с ошибкой понижаем этму приоритет
	,t.dataspace_name
FROM
	(
		SELECT
			CASE 
				WHEN t.action = N'REBUILD' AND t.online LIKE N'OFF%' THEN 1
				WHEN t.action = N'REBUILD' AND t.online is null THEN 1
				WHEN t.action = N'REBUILD' AND t.online LIKE N'ON%' THEN 2
				WHEN t.action = N'REORGANIZE' THEN 3
			END as action_type_id,
			--Конструирую выражение / Assembling statement
			N'USE ' + QUOTENAME(DB_NAME(@db)) + N'; ' +
			CASE WHEN @locktimeout<60000 or (@@MicrosoftVersion/0x01000000)<12 THEN N'SET LOCK_TIMEOUT ' + t.lock_timeout + N'; ' ELSE '' END +
			N'ALTER INDEX [' + t.index_name + N'] ON '+ t.object_name + N' ' + t.action + N' ' + t.partition_spec +
			CASE WHEN t.action = N'REBUILD' THEN N'WITH ('+COALESCE('ONLINE='+ t.online,'') +N'SORT_IN_TEMPDB='+ t.sort_in_tempdb + N'MAXDOP='+ t.maxdop + N')' ELSE N'' END + N';' --rebuild options
			as command,
			*
		FROM
			(	--Подготавливаю опции / prepare options for the statement
				SELECT 
					--REORG or REBUILD:
					CASE WHEN avg_fragmentation_in_percent > @reorg_threshold THEN N'REBUILD' ELSE N'REORGANIZE' END as [action],
					--If we have multiple partitions, specify the partition number
					CASE WHEN partition_count > 1 THEN N' PARTITION = ' + CAST(partition_number as nvarchar(10)) ELSE N'' END + N' ' as partition_spec,
					--Sort in tempdb determined by global option
					CASE WHEN @sortintempdb = 1 THEN N'ON, ' ELSE N'OFF, ' END [sort_in_tempdb],
					--If possible, perform online rebuild
					CASE 
						WHEN @online_allowed = 0 THEN N'OFF, /*disabled*/ '  --online is disabled in settings
						WHEN legacy_col_count > 0 THEN N'OFF, /*legacy*/ ' --legacy column types can't be rebuilt online
						WHEN xml_col_count > 0 AND (@@MicrosoftVersion/0x01000000)<11 THEN N'OFF, /*xml*/ ' --xml columns can't be rebuild online prior to 2012
						--WHEN partition_count = 1 THEN NULL --one partition can't be rebuild online and even OFF is incorrect
						WHEN partition_count > 1 AND (@@MicrosoftVersion/0x01000000)<12 THEN NULL --prior to SQL 2014 partitions could not be rebuilt online and even OFF is incorrect
						ELSE --We can use online rebuild
							CASE 
							--Starting from 2014 rebuild can wait at low priority:
								WHEN (@@MicrosoftVersion/0x01000000)>=12 and @locktimeout >= 60000 /*more than a minute */ THEN N'ON (WAIT_AT_LOW_PRIORITY ( MAX_DURATION = '+cast(@locktimeout/60000 as nvarchar(20))+' , ABORT_AFTER_WAIT = SELF ) ),' 
								
								ELSE N'ON, ' --Simply online
								
								
							END

					END as [online],
					--Lock timeout from global settings
					CAST(@locktimeout as nvarchar(10)) as [lock_timeout],
					--Set maxdop if page locks are not prohibited. See  http://support.microsoft.com/kb/2292737 
					CASE 
						WHEN [allow_page_locks] = 1 OR (@@MicrosoftVersion/0x01000000)>=11  THEN  CAST(ISNULL(@maxdop,1) AS nvarchar(10))
						--WHEN @allow_pl = 0 AND (@@MicrosoftVersion/0x01000000)<11 THEN N'1' 
						ELSE N'1'
					END as [maxdop],
					*
				FROM 
					#partitions p
			) t
	) t
	OUTER APPLY --находим такой же предыдущий таск / Find similar prevous task 
	(
		SELECT TOP 1 pt.exit_code 
		FROM
			dbo.Tasks pt
		WHERE
			pt.subsystem_id = 1 
			AND pt.action_type_id = t.action_type_id
			AND pt.database_id = t.database_id
			AND pt.table_id = t.object_id
			AND pt.index_id = t.index_id
			AND pt.partition_n = t.partition_number
			AND pt.date_completed is not null
		ORDER BY
			pt.date_completed DESC
	) pt
WHERE
	--Фрагментация выше порога / Fragmentation above threshold
	NOT EXISTS	(
		SELECT 1 
		FROM dbo.Blacklist bl
		WHERE 
			bl.[database_id] = @db 
			AND (bl.table_id = t.object_id or bl.table_id is null)
			AND (bl.index_id = t.index_id or bl.index_id is null) 
			AND (bl.subsystem_id = 1 or bl.subsystem_id is null)
			AND (bl.action_type_id = t.action_type_id or bl.action_type_id is null)
			AND (bl.partition_n = t.partition_number or bl.partition_n is null)
			AND (bl.worker_name = @worker_name or worker_name is null)
			AND bl.enabled = 1
	)
ORDER BY 
	t.user_scans DESC,
	t.avg_fragmentation_in_percent DESC,
	t.size_mb DESC

GO


