
/*
-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Oleg Trutnev otrutnev@microsoft.com

Russian version:

Процедура формирования заданий проверки целостности БД с точки зрения аллокации.
Задание представляет собой выражение DBCC CHECKALLOC('<имя БД>').
Отбираются БД не проверявшиеся более чем количество дней, заданное параметром CheckAllocIntervalDays в таблице Parameters.
Задания упорядочиваются по давности последней проверки или размеру БД.

Englsh version:

Procedure creates a tasks of integrity checks of DB extents allocation.
Tasks are statements like DBCC CHECKALLOC('<DB name>').
Databases are being scheduled for check if they have not been checked for more than X days,
where X is a value of CheckAllocIntervalDays parameter in dbo.Parameters table.
Tasks are sorted by date of last check and/or DB size Mb.
  


*/
CREATE PROCEDURE [dbo].[FillQueueCheckAlloc] @db INT, @batch UNIQUEIDENTIFIER
AS

DECLARE	@SQL NVARCHAR(MAX)


DECLARE @check_interval int
SELECT @check_interval = int_value
FROM dbo.Parameters where parameter = 'CheckAllocIntervalDays'

DECLARE @worker_name nvarchar(255)
SELECT @worker_name = worker_name
FROM dbo.WorkerSessions
WHERE session_id = @@SPID

DELETE
FROM dbo.Tasks
WHERE subsystem_id = 3 --Подсистема проверки целостности / subsystem_id 3 is an integrity check
	AND action_type_id = 2 -- CHECKALLOC
	AND date_completed IS NULL
	AND [database_id] = @db

DECLARE @last_check datetime

SELECT 
	@last_check = MAX(date_completed)
FROM dbo.Tasks
WHERE database_id = @db
	AND subsystem_id = 3
	AND action_type_id = 2
	AND exit_code = 1


IF 
	DATEDIFF(dd,ISNULL(@last_check,0),getdate()) > @check_interval
	AND NOT EXISTS (SELECT 1 
		FROM dbo.Blacklist bl
		WHERE 
			bl.database_id = @db
			AND (bl.subsystem_id = 3 or bl.subsystem_id is null)
			AND (bl.action_type_id = 2 or action_type_id is null)
			AND (bl.worker_name = @worker_name or worker_name is null)
			AND bl.enabled = 1)

INSERT INTO dbo.Tasks (
	[batch_id]
	,[subsystem_id]
	,[action_type_id]
	,[command]
	,[date_added]
	,[database_id]
	,[size_mb]
	,[checked_daysago]
)
SELECT
	@batch
	,3
	,2 
	,'USE [VBMS]; EXEC [dbo].[ExecuteDBCCCheck] @action_type_id = 2, @db = '+cast(@db as nvarchar(3))+'--DBCC CHECKALLOC(['+db_name(@db)+']) WITH NO_INFOMSGS'
	,getdate()
	,@db
	,sum(CAST(ps.used_page_count * 8 / 1024.00 AS DECIMAL(10, 3)))
	,DATEDIFF(dd,@last_check,getdate())
from 
	sys.dm_db_partition_stats ps