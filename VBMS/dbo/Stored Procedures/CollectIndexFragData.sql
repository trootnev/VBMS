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

CREATE PROCEDURE [dbo].[CollectIndexFragData] 
	@db int
	,@time_limit_s int = 3600
	,@batch_id uniqueidentifier= NULL
	,@maxdop int=  NULL
	,@sortintempdb bit =NULL
AS
SET NOCOUNT ON
DECLARE
	
	@SQL nvarchar(max),
	@locktimeout bigint
	IF @batch_id is null
		SET @batch_id = NEWID()
	
SELECT @locktimeout = int_value
FROM dbo.Parameters
WHERE parameter = 'LockTimeoutMs'	

--Удаляем устаревшие невыполненные задания / Old tasks cleanup
--DELETE
--FROM dbo.FragmentationData
--WHERE analysis_spid = @@SPID
--	and analysis_started is not null
--	AND analysis_complete is NULL
--	AND [database_id] = @db



SET @SQL = 
'USE '+QUOTENAME(DB_NAME(@db))+';

DECLARE @t1 datetime
,@timefactor float
,@exit_code int
,@Error_message nvarchar(max)

SET @t1 = DATEADD(s,'+CAST(@time_limit_s as nvarchar(255))+',GETDATE())


----Fill Fragmentation Data

SET LOCK_TIMEOUT '+CAST(@locktimeout as nvarchar(10))+';

SET NOCOUNT ON
	DECLARE @entry_id bigint, 
			@db int, 
			@table_id int,
			@index_id int, 
			@partition_id int, 
			@frag int, 
			@size_mb int,
			@batch_id uniqueidentifier
	SET @batch_id = '''+CAST(@batch_id as nvarchar(50))+'''

	DECLARE PARTS CURSOR 
	FOR SELECT entry_id, DB_ID(),object_id, index_id, partition_number, size_mb
	FROM VBMS.dbo.FragmentationData WITH (NOLOCK) where avg_fragmentation_in_percent IS NULL and database_id = DB_ID() and analysis_spid is null
	ORDER BY user_scans desc


	OPEN PARTS

	FETCH NEXT FROM PARTS
	INTO @entry_id, @db, @table_id, @index_id,@partition_id, @size_mb

	WHILE @@FETCH_STATUS=0
	BEGIN
	SET @timefactor = ISNULL(VBMS.dbo.GetTimeFactor(@db,0,@table_id,@index_id,@partition_id,1, 1,60,1),0)
	--PRINT @entry_id
	--PRINT DATEDIFF(s,@t1,(DATEADD(s,(@size_mb * @timefactor/1000),getdate())))
	--PRINT DATEADD(s,(@size_mb * @timefactor/1000),getdate())
	IF exists (select 1 FROM 
				sys.indexes i 
	INNER JOIN sys.partitions p ON p.object_id = i.object_id AND p.index_id = i.index_id
	where i.object_id = @table_id 
		AND i.index_id = @index_id
		AND p.partition_number = @partition_id)
	IF (SELECT analysis_spid FROM VBMS.dbo.FragmentationData WHERE entry_id = @entry_id ) IS NULL
		BEGIN
		IF (DATEADD(s,(@size_mb * @timefactor/1000),getdate())<@t1)
		BEGIN
			UPDATE VBMS.dbo.FragmentationData WITH (ROWLOCK)
			SET analysis_started = GETDATE(), analysis_spid = @@SPID, analysis_batch_id = @batch_id
			,analysis_time_prognosis_ms = @size_mb * @timefactor
			where entry_id = @entry_id

			SET @exit_code = 1
			SET @error_message = ''Ok''
			BEGIN TRY				 	
			SELECT @frag =  avg_fragmentation_in_percent from sys.dm_db_index_physical_stats(@db,@table_id,@index_id,@partition_id,''LIMITED'') WHERE alloc_unit_type_desc = ''IN_ROW_DATA''
			END TRY
			BEGIN CATCH

						SET @exit_code = ERROR_NUMBER()
						SET @error_message = N''Error code: '' + CAST(ERROR_NUMBER() AS NVARCHAR(10)) + N''. Error message:'' + ERROR_MESSAGE()
			END CATCH
			
			UPDATE VBMS.dbo.FragmentationData
			SET avg_fragmentation_in_percent= @frag
				,analysis_completed = getdate()
				,analysis_status = 1
				--,analysis_time_prognosis_ms = @size_mb * @timefactor
				,analysis_duration_ms = DATEDIFF(ms,analysis_started,getdate())
				,time_factor = CASE WHEN @frag is not null THEN (DATEDIFF(ms, analysis_started, GETDATE())/ (size_mb + 1.0)) ELSE NULL END
				,exit_code = @exit_code
				,exit_message = @error_message
			WHERE entry_id = @entry_id and analysis_spid = @@SPID and avg_fragmentation_in_percent is null
			IF @exit_code = 1
				BEGIN 
				exec VBMS.dbo.FillQueueIndex_async @db = @db,@batch = @batch_id, @maxdop = '+ISNULL(CAST(@maxdop as nvarchar(50)),'NULL')+', @sortintempdb = '+ISNULL(CAST(@sortintempdb as nvarchar(50)),'NULL')+', @entry_id = @entry_id
				
			
				UPDATE VBMS.dbo.FragmentationData
				SET analysis_status = 2
				WHERE entry_id = @entry_id 
				END
			ELSE
				BEGIN
				UPDATE VBMS.dbo.FragmentationData
					SET analysis_status = 3
					WHERE entry_id = @entry_id 
				END
			END
		ELSE 
			BEGIN
			--Print @entry_id
			--Print ''Skipped''
			UPDATE VBMS.dbo.FragmentationData
			SET analysis_completed = getdate()
			,analysis_duration_ms = DATEDIFF(ms,analysis_started,getdate())
			,analysis_status = -1
			,time_factor = 0
			,exit_code = -1
			,exit_message = ''Skipped. Not enough time left''
			WHERE entry_id = @entry_id and analysis_spid is null and avg_fragmentation_in_percent is null
			END
		END

		
		FETCH NEXT FROM PARTS
		INTO @entry_id, @db, @table_id, @index_id,@partition_id, @size_mb
		END

	CLOSE PARTS
	DEALLOCATE PARTS


'

EXEC (@SQL)
