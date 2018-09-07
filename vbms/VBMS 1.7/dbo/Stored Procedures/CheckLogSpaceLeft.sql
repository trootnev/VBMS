/*

-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Oleg Trutnev otrutnev@microsoft.com
		Arseny Birukov arsbir@microsoft.com

Процедура оценивает объем свободного места, доступный для журнала транзакций
ВАЖНО: при использовании маунт пойнтов процедура ведет себя некорректно.

The procedure estimates the free space available for the transaction log
IMPORTANT: the result is wrong when mount points are used.
*/



CREATE PROCEDURE [dbo].[CheckLogSpaceLeft] 
(
	@db int, 
	@limit_mb bigint
)
--DECLARE @db int = 6,@limit_mb int = 5000
AS
DECLARE @sql nvarchar(max)
IF EXISTS (select 1 from sys.objects where name like '#result' and type = 'U')
DROP TABLE #result

CREATE TABLE #result  (
	dbname nvarchar(255),
	[mb_free] int
)

--We need dynamic t-sql to use FILEPROPERY function.
IF (@@MicrosoftVersion/0x01000000)<11
	BEGIN
	SET @sql='
USE [' + cast(db_name(@db) AS NVARCHAR(50)) + '];
DECLARE @Drives TABLE
(
	[drive] nvarchar(255),
	[free] bigint
)

INSERT INTO @Drives 
EXEC(''xp_fixeddrives'')

INSERT #result
SELECT
	DB_NAME(ms.database_id) as database_name,
	SUM(size/128.0 - CAST(FILEPROPERTY(name, ''SPACEUSED'') AS INT)/128.0 + CASE WHEN ms.max_size = -1 THEN d.free ELSE (max_size -size)/128.0 END) AS FREESPACEMB 
FROM 
	sys.master_files ms
	JOIN @Drives d on SUBSTRING(ms.physical_name,1,1) collate SQL_Latin1_General_CP1_CI_AS = d.drive collate SQL_Latin1_General_CP1_CI_AS
WHERE 
	database_id = DB_ID()
	and type = 1
GROUP BY 
	database_id
HAVING 
	SUM(size/128.0 - CAST(FILEPROPERTY(name, ''SPACEUSED'') AS INT)/128.0 + CASE WHEN ms.max_size = -1 THEN d.free ELSE (max_size -size)/128.0 END) < '+CAST(@limit_mb as nvarchar(50))
 END
 ELSE
 BEGIN
 set @sql = '
 USE [' + cast(db_name(@db) AS NVARCHAR(50)) + '];
 
INSERT #result
SELECT
	DB_NAME(ms.database_id) as database_name,
	SUM(size/128.0 - CAST(FILEPROPERTY(name, ''SPACEUSED'') AS INT)/128.0 + CASE WHEN ms.max_size = -1 THEN (vs.available_bytes)/1024/1024 ELSE (max_size -size)/128.0 END) AS FREESPACEMB 
FROM 
	sys.master_files ms
	CROSS APPLY sys.dm_os_volume_stats(ms.database_id, ms.file_id) vs
WHERE 
	ms.database_id = DB_ID()
	and type = 1
GROUP BY 
	ms.database_id
HAVING 
	SUM(size/128.0 - CAST(FILEPROPERTY(name, ''SPACEUSED'') AS INT)/128.0 + CASE WHEN ms.max_size = -1 THEN (vs.available_bytes)/1024/1024 ELSE (max_size -size)/128.0 END) < '+CAST(@limit_mb as nvarchar(50))
 
 END

EXEC(@sql)

IF @@rowcount >0
	BEGIN
	DROP TABLE #result
	RETURN 0
	END
ELSE 
	BEGIN
	DROP TABLE #result
	RETURN 1
	END

 