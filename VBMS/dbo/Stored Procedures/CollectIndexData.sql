/*

-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: 
Oleg Trutnev otrutnev@microsoft.com



Russian:



English:
  

 
*/

CREATE PROCEDURE [dbo].[CollectIndexData] 
	 @db INT
	,@batch UNIQUEIDENTIFIER = NULL
	
AS
SET NOCOUNT ON
DECLARE
	@SQL nvarchar(max),
	@index_minsize bigint,
	@locktimeout bigint

	IF @batch is null
		SET @batch = NEWID()

--Удаляем устаревшие невыполненные задания / Old tasks cleanup
DELETE FROM dbo.FragmentationData
	WHERE database_id = @db
	AND analysis_started is null
	
	

--Считываем параметры / Loading parameters

SELECT @index_minsize = int_value
FROM dbo.Parameters
WHERE parameter = 'IndexMinimumSizeMB'

SELECT @locktimeout = int_value
FROM dbo.Parameters
WHERE parameter = 'LockTimeoutMs'



--Get list of all partitions and some stats on them
--This statement should be executed in different database context, so we have to use dynamic T-SQL
--The statement itself is static, it's relatively easy to unquote it
SET @SQL = 
'USE '+QUOTENAME(DB_NAME(@db))+';

DECLARE @worker_name nvarchar(255)
SELECT @worker_name = worker_name
FROM VBMS.dbo.WorkerSessions
WHERE session_id = @@SPID

SET LOCK_TIMEOUT '+CAST(@locktimeout as nvarchar(10))+';

INSERT INTO VBMS.[dbo].[FragmentationData]
           ([batch_id]
           ,[collection_date]
           ,[database_id]
           ,[schema_id]
           ,[object_id]
           ,[object_name]
           ,[index_id]
           ,[index_name]
           ,[allow_page_locks]
           ,[legacy_col_count]
           ,[xml_col_count]
           ,[user_scans]
           ,[partition_count]
           ,[partition_number]
           ,[row_count]
           ,[size_mb]
           ,[avg_fragmentation_in_percent]
		   ,[analysis_status]
		   ,[volume_mount_point])
     
SELECT '''+CAST(@batch as nvarchar(50))+''',
GETDATE() as collection_date,
	DB_ID() as database_id, 
	t.schema_id, 
	t.object_id, 
	QUOTENAME(SCHEMA_NAME(t.schema_id))+''.''+QUOTENAME(OBJECT_NAME(t.object_id)) as object_name,
	--per index data:
	i.index_id,
	i.name as index_name,
	i.allow_page_locks,
	c.legacy_col_count,
	c.xml_col_count,
	ISNULL(ius.user_scans,0) as user_scans,
	count(*) OVER (PARTITION BY t.object_id, i.index_id) as partition_count,
	--per partition data:
	p.partition_number,
	p.rows as row_count,
	CAST(ps.used_page_count * 8 / 1024.00 AS DECIMAL(10,3)) as size_mb,
	NULL as avg_fragmentation_in_percent,
	0 as analysis_status,
	vs.volume_mount_point
	
FROM 
	sys.tables t
	INNER JOIN sys.indexes i on t.object_id = i.object_id 
	INNER JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id = i.index_id
	INNER JOIN sys.allocation_units au on p.partition_id = au.container_id and au.type =1
	INNER JOIN sys.data_spaces ds on au.data_space_id = ds.data_space_id
	INNER JOIN sys.database_files dbf on dbf.data_space_id = ds.data_space_id
	CROSS APPLY sys.dm_os_volume_stats(DB_ID(),dbf.file_id) vs
	LEFT OUTER JOIN sys.dm_db_partition_stats ps on ps.object_id = t.object_id AND ps.index_id = i.index_id AND ps.partition_number = p.partition_number
	LEFT OUTER JOIN sys.dm_db_index_usage_stats ius ON ius.database_id = DB_ID() AND ius.index_id = i.index_id AND ius.object_id = i.object_id
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
WHERE
	i.index_id > 0 -- we cant rebuild heap
	AND NOT EXISTS	(
		SELECT 1 
		FROM VBMS.dbo.Blacklist bl
		WHERE 
			bl.[database_id] = DB_ID()
			AND (bl.table_id = t.object_id or bl.table_id is null)
			AND (bl.index_id = i.index_id or bl.index_id is null) 
			AND (bl.subsystem_id = 1 or bl.subsystem_id is null)
			AND (bl.partition_n = p.partition_number or bl.partition_n is null)
			AND (bl.worker_name = @worker_name or worker_name is null)
			AND bl.enabled = 1)
	and ps.used_page_count > '+ CAST(@index_minsize /8*1024 as nvarchar(20)) 
EXEC(@SQL)



