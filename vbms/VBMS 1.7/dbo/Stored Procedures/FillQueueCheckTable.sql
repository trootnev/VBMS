/*
-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Oleg Trutnev otrutnev@microsoft.com


Russian version:

Процедура формирования заданий проверки целостности отдельных таблиц.
Задание представляет собой выражение DBCC CHECKTABLE('<имя таблицы>').
Отбираются таблицы не проверявшиеся более чем количество дней, заданное параметром CheckTableIntervalDays в таблице Parameters.
Таблицы упорядочиваются по давности последней проверки и размеру таблицы.

Englsh version:

Procedure creates a tasks of integrity checks for perticular tables.
Tasks are statements like DBCC CHECKTABLE('<Table name>').
Tables are being scheduled for check if they have not been checked for more than X days,
where X is a value of CheckTableIntervalDays parameter in dbo.Parameters table.
Tasks are sorted by date of last check and table size Mb.

*/
CREATE PROCEDURE [dbo].[FillQueueCheckTable] 
	@db INT, 
	@batch UNIQUEIDENTIFIER, 
	@table_max_size_mb bigint = 1000000 --exclude bigger tables. By default no more than 1 Pb
AS

DECLARE	@SQL NVARCHAR(MAX)

DECLARE @check_interval int

SELECT @check_interval = int_value
FROM dbo.Parameters where parameter = 'CheckTableIntervalDays'


DECLARE @worker_name nvarchar(255)
SELECT @worker_name = worker_name
FROM dbo.WorkerSessions
WHERE session_id = @@SPID

IF @table_max_size_mb = 0
	SET @table_max_size_mb = 1000000

DELETE
FROM dbo.Tasks
WHERE subsystem_id = 3 --Тип действия Проверка целостности таблиц / subsystem_id 3 is an integrity check
	AND action_type_id = 3
	AND date_completed IS NULL
	AND [database_id] = @db


DECLARE @tables TABLE
(
	database_id int,
	schema_id int,
	object_id int,
	object_name nvarchar(4000),
	row_count bigint,
	size_mb decimal(10, 3)
)

SET @SQL = 
'USE '+QUOTENAME(DB_NAME(@db))+
'SELECT 
	DB_ID() as database_id, 
	t.schema_id, 
	t.object_id, 
	QUOTENAME(SCHEMA_NAME(t.schema_id))+''.''+QUOTENAME(OBJECT_NAME(t.object_id)) as object_name,
	sum(p.rows) as row_count,
	sum(CAST(ps.used_page_count * 8 / 1024.00 AS decimal(10, 3))) as size_mb
	
FROM 
	sys.tables t
	INNER JOIN sys.indexes i on t.object_id = i.object_id and i.index_id < 2 --Heap or clustered index
	INNER JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id = i.index_id
	LEFT JOIN sys.dm_db_partition_stats ps on ps.object_id = t.object_id AND ps.index_id = i.index_id AND ps.partition_number = p.partition_number
WHERE rows > 0
GROUP BY 
	t.schema_id, 
	t.object_id'
	
INSERT INTO @tables
EXEC (@SQL)


INSERT dbo.Tasks (
	[batch_id]
	,[subsystem_id]
	,[action_type_id]
	,[command]
	,[date_added]
	,[database_id]
	,[table_id]
	,[rowcnt]
	,[size_mb]
	,[checked_daysago]
)
SELECT
	@batch
	,3
	,3 
	,N'USE [VBMS]; EXEC [dbo].[ExecuteDBCCCheck] @action_type_id = 3, @db = '+cast(@db as nvarchar(4))+', @object_id = '+cast(nt.object_id as nvarchar(20)) +'; --DBCC CHECKTABLE('+nt.object_name+') WITH NO_INFOMSGS'
	,getdate()
	,@db
	,nt.object_id
	,nt.row_count
	,nt.size_mb
	,DATEDIFF(dd,pt.date_completed,getdate())
FROM
	@tables nt 
	LEFT OUTER JOIN
	( 
		SELECT 
			ROW_NUMBER() OVER (PARTITION BY table_id order by date_completed DESC) AS rn,
			table_id,
			date_completed
		FROM  
			dbo.Tasks
		WHERE 
			subsystem_id = 3
			AND action_type_id = 3
			AND database_id = @db
			AND exit_code = 1
	) pt ON pt.table_id = nt.object_id
WHERE
	ISNULL(nt.size_mb,0) < @table_max_size_mb
	AND (pt.rn = 1 OR pt.rn is null) --last row or nothing
	AND (DATEDIFF(dd,ISNULL(pt.date_completed,0),getdate()) > @check_interval OR pt.date_completed is null)
	AND NOT EXISTS
		(
			SELECT 1 
			FROM dbo.Blacklist bl
			WHERE 
				bl.database_id = @db 
				and (bl.table_id = nt.object_id or bl.table_id is null)
				and (bl.subsystem_id = 3 or bl.subsystem_id is null)
				and (bl.action_type_id = 3 or action_type_id is null)
				AND (bl.worker_name = @worker_name or worker_name is null)
				and bl.enabled = 1
		)

	