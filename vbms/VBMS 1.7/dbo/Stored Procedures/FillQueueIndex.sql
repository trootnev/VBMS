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

CREATE PROCEDURE [dbo].[FillQueueIndex] 
	 @db INT
	,@batch UNIQUEIDENTIFIER
	,@maxdop INT = NULL
	,@sortintempdb BIT = NULL
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

--Удаляем устаревшие невыполненные задания / Old tasks cleanup
DELETE
FROM dbo.Tasks
WHERE subsystem_id = 1
	AND date_completed IS NULL
	AND [database_id] = @db

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


IF (CAST(SERVERPROPERTY('Edition') as NVARCHAR(50)) LIKE N'%Enterprise%'
  OR CAST(SERVERPROPERTY('Edition') as NVARCHAR(50)) LIKE N'%Datecenter%'
  OR CAST(SERVERPROPERTY('Edition') as NVARCHAR(50)) LIKE N'%Developer%')
	SELECT @online_allowed = int_value
	FROM dbo.Parameters
	WHERE parameter = 'IndexOnlineRebuild'
ELSE 
	SET @online_allowed = 0

--Get list of all partitions and some stats on them
--This statement should be executed in different database context, so we have to use dynamic T-SQL
--The statement itself is static, it's relatively easy to unquote it
SET @SQL = 
'USE '+QUOTENAME(DB_NAME(@db))+';

IF OBJECT_ID(''tempdb..#frag_data'') is not null
	DROP TABLE #frag_data
SELECT 
	DB_ID() as database_id, 
	t.schema_id, 
	t.object_id, 
	QUOTENAME(SCHEMA_NAME(t.schema_id))+''.''+QUOTENAME(OBJECT_NAME(t.object_id)) as object_name,
	--per index data:
	i.index_id,
	i.name as index_name,
	i.allow_page_locks,
	--c.legacy_col_count,
	--c.xml_col_count,
	ISNULL(ius.user_scans,0) as user_scans,
	count(*) OVER (PARTITION BY t.object_id, i.index_id) as partition_count,
	--per partition data:
	p.partition_number,
	p.rows as row_count,
	CAST(ps.used_page_count * 8 / 1024.00 AS DECIMAL(10,3)) as size_mb,
	CAST(ips.avg_fragmentation_in_percent as DECIMAL(10,3)) as avg_fragmentation_in_percent
INTO #frag_data
FROM 
	sys.tables t
	INNER JOIN sys.indexes i on t.object_id = i.object_id 
	INNER JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id = i.index_id
	LEFT OUTER JOIN sys.dm_db_partition_stats ps on ps.object_id = t.object_id AND ps.index_id = i.index_id AND ps.partition_number = p.partition_number
	LEFT OUTER JOIN sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, ''LIMITED'') ips ON ips.object_id = i.object_id AND ips.index_id = i.index_id AND ips.partition_number = p.partition_number AND alloc_unit_type_desc = ''IN_ROW_DATA''
	LEFT OUTER JOIN sys.dm_db_index_usage_stats ius ON ius.database_id = DB_ID() AND ius.index_id = i.index_id AND ius.object_id = i.object_id
	--LEFT OUTER JOIN
	--(  --number of legacy and xml columns is needed to decide if online rebuild is possible
	--   --If the index is clustered, consider all columns
	--	SELECT 
	--		c.object_id, 
	--		i.index_id,
	--		ISNULL(SUM(CASE WHEN TYPE_NAME(c.system_type_id) IN (''image'',''text'',''ntext'') THEN 1 ELSE 0 END),0) as legacy_col_count,
	--		ISNULL(SUM(CASE WHEN TYPE_NAME(c.system_type_id) = ''xml''  THEN 1 ELSE 0 END),0) as xml_col_count
	--	FROM 
	--		sys.columns c
	--		INNER JOIN sys.indexes i ON i.object_id = c.object_id
	--		LEFT OUTER JOIN	sys.index_columns ic ON ic.object_id = c.object_id	AND ic.column_id = c.column_id AND ic.index_id = i.index_id
	--	WHERE
	--		(i.index_id = 1               -- index is clustered (count all columns)
	--		OR ic.index_id is not null)   -- OR nonclustered    (count only index columns)
	--		GROUP BY 
	--		c.object_id, i.index_id	
	--) c ON c.index_id = i.index_id AND c.object_id = i.object_id
WHERE
	i.index_id > 0 -- we cant rebuild heap
	and ps.used_page_count > ' + CAST(@index_minsize /8 * 1024 as NVARCHAR(10))+ '
	and ips.avg_fragmentation_in_percent >' + CAST(@frag_threshold as NVARCHAR(10))+'

SELECT  
i.database_id,
i.schema_id,
i.object_id,
i.object_name,
i.index_id,
i.index_name,
i.allow_page_locks,
c.legacy_col_count,
c.xml_col_count,
i.user_scans,
i.partition_count,
i.partition_number,
i.row_count,
i.size_mb,
i.avg_fragmentation_in_percent

FROM #frag_data i
LEFT OUTER JOIN
	(  --number of legacy and xml columns is needed to decide if online rebuild is possible
	   --If the index is clustered, consider all columns
		SELECT 
			c.object_id, 
			i.index_id,
			ISNULL(SUM(CASE WHEN TYPE_NAME(c.system_type_id) IN (''image'',''text'',''ntext'') THEN 1 ELSE 0 END),0) as legacy_col_count,
			ISNULL(SUM(CASE WHEN TYPE_NAME(c.system_type_id) = ''xml''  THEN 1 ELSE 0 END),0) as xml_col_count
		FROM 
			sys.columns c
			INNER JOIN sys.indexes i ON i.object_id = c.object_id
			LEFT OUTER JOIN	sys.index_columns ic ON ic.object_id = c.object_id	AND ic.column_id = c.column_id AND ic.index_id = i.index_id
		WHERE
			(i.index_id = 1               -- index is clustered (count all columns)
			OR ic.index_id is not null)   -- OR nonclustered    (count only index columns)
			GROUP BY 
			c.object_id, i.index_id	
	) c ON c.index_id = i.index_id AND c.object_id = i.object_id

DROP TABLE #frag_data



'
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
EXEC (@SQL)



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
								WHEN (@@MicrosoftVersion/0x01000000)>=12 and @locktimeout > 60000 /*more than a minute */ THEN N'ON (WAIT_AT_LOW_PRIORITY ( MAX_DURATION = '+cast(@locktimeout/60000 as nvarchar(20))+' , ABORT_AFTER_WAIT = SELF ) ),' 
								
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
	t.avg_fragmentation_in_percent > @frag_threshold
	AND size_mb > @index_minsize
	AND	NOT EXISTS	(
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

