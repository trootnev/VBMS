/*
-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Oleg Trutnev otrutnev@microsoft.com


Russian version:

Процедура формирования заданий проверки целостности системного каталога БД.
Задание представляет собой выражение DBCC CHECKCATALOG('<имя БД>').
Отбираются БД не проверявшиеся более чем количество дней, заданное параметром CheckCatalogIntervalDays в таблице Parameters.
Задания упорядочиваются по давности последней проверки и размеру БД.

Englsh version:

Procedure creates a tasks of integrity checks of system tables.
Tasks are statements like DBCC CHECKTABLE('<DB name>').
Databases are being scheduled for check if they have not been checked for more than X days,
where X is a value of CheckCatalogIntervalDays parameter in dbo.Parameters table.
Tasks are sorted by date of last check and DB size Mb.

*/
CREATE PROCEDURE [dbo].[FillQueueCheckCatalog] @db INT, @batch UNIQUEIDENTIFIER
AS

--Считываем параметры / Loading parameters
DECLARE @check_interval int
SELECT @check_interval = int_value
FROM dbo.Parameters where parameter = 'CheckCatalogIntervalDays'

DECLARE @worker_name nvarchar(255)
SELECT @worker_name = worker_name
FROM dbo.WorkerSessions
WHERE session_id = @@SPID

--Удаляем устаревшие невыполненные задания / Old tasks cleanup
DELETE
FROM dbo.Tasks
WHERE subsystem_id = 3     --Подсистема проверки целостности / subsystem_id 3 is an integrity check
	AND action_type_id = 1 -- CHECKCATALOG
	AND date_completed IS NULL
	AND [database_id] = @db

DECLARE @last_check datetime

SELECT 
	@last_check = MAX(date_completed)
FROM dbo.Tasks
WHERE database_id = @db
	AND subsystem_id = 3
	AND action_type_id = 1
	AND exit_code = 1


IF 
	DATEDIFF(dd,ISNULL(@last_check,0),getdate()) > @check_interval
	AND NOT EXISTS (SELECT 1 
		FROM dbo.Blacklist bl
		WHERE 
			bl.database_id = @db
			AND (bl.subsystem_id = 3 or bl.subsystem_id is null)
			AND (bl.action_type_id = 1 or action_type_id is null)
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
	,1 
	,'USE [VBMS]; EXEC [dbo].[ExecuteDBCCCheck] @action_type_id = 1, @db = '+cast(@db as nvarchar(3))+'--DBCC CHECKCATALOG(['+db_name(@db)+']) WITH NO_INFOMSGS'
	,getdate()
	,@db
	,5.0 --system catalog is usually 2-10 Mb for all DBs. Size does not really matter.
	,DATEDIFF(dd,@last_check,getdate())



