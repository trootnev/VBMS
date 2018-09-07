
CREATE PROCEDURE [dbo].[ExecuteDBCCCheck]
	@action_type_id int = 1,
	@db int = 1,
	@object_id nvarchar(50) = NULL,
	@exec_guid uniqueidentifier = NULL
AS

DECLARE 
	@SQL nvarchar(max), 
	@msg nvarchar(max)
	--@sql1 nvarchar(255) 
 
 IF @exec_guid is null
	SET @exec_guid = NEWID()

 IF OBJECT_ID('tempdb..#t') IS NOT NULL
	DROP TABLE #t


IF DB_NAME(@db) is null
	RAISERROR ('Wrong database specified',16,1)

IF @action_type_id = 3 AND OBJECT_NAME(@object_id, @db) is null
BEGIN
	DECLARE @dbname sysname = DB_NAME(@db)
	RAISERROR ('Table with object_id = %d does not exist in database %s', 16, 1, @object_id, @dbname)
END


CREATE TABLE #t 
(
	[ErrorNum] [smallint] NULL,
	[Level] [tinyint] NULL,
	[State] [tinyint] NULL,
	[MessageText] [varchar](500) NULL,
	[RepairLevel] [varchar](40) NULL,
	[Status] [tinyint] NULL,
	[Dbid] [smallint] NULL,
	[DbFragId] [int] NULL,
	[ObjectId] [int] NULL,
	[IndexId] [int] NULL,
	[PartitionId] [bigint] NULL,
	[AllocUnitId] [bigint] NULL,
	[RidDbId] [int] NULL,
	[RidPruId] [int] NULL,
	[File] [int] NULL,
	[Page] [int] NULL,
	[Slot] [int] NULL,
	[RefDbId] [int] NULL,
	[RefPruId] [int] NULL,
	[RefFile] [int] NULL,
	[RefPage] [int] NULL,
	[RefSlot] [int] NULL,
	[Allocation] [int] NULL
)

SET @SQL = 
	N'USE ' + QUOTENAME(db_name(@db)) + N'; ' +
	CASE 
		WHEN @action_type_id=3 
		THEN N'DECLARE @table_name nvarchar(255)	SELECT @table_name = QUOTENAME(SCHEMA_NAME(schema_id))+''.''+QUOTENAME(name) FROM sys.objects WHERE object_id = ' + @object_id + N'; '
		ELSE N''
	END +
	N'INSERT INTO #t ' +
	CASE @action_type_id	
		WHEN 1 THEN N'EXEC(''DBCC CHECKCATALOG(['+db_name(@db)+N']) WITH NO_INFOMSGS'')'
		WHEN 2 THEN N'EXEC(''DBCC CHECKALLOC(['+db_name(@db)+N']) WITH NO_INFOMSGS, ALL_ERRORMSGS, TABLERESULTS'')'
		WHEN 3 THEN N'EXEC(''DBCC CHECKTABLE("''+@table_name+''") WITH NO_INFOMSGS, ALL_ERRORMSGS, TABLERESULTS'')'
		ELSE N'RAISERROR (''Incorrect value for parameter @action_type! 1 - CHECKCATALOG, 2 - CHECKALLOC, 3 - CHECKTABLE'',16,1)'
	END

EXEC (@SQL)

IF @@ROWCOUNT > 0
BEGIN
	SET @msg = N'Consistency check found some errors! See dbo.DBCCChecksLog table for details! execution_guid:'+cast(@exec_guid as nvarchar(40))

	IF (select @@MicrosoftVersion/0x01000000)<10
	BEGIN
		INSERT dbo.DBCCChecksLog(
			[execution_guid] , 
			log_date,
			error_num ,
			[level] ,
			[State] ,
			[message_text],
			[repair_level] ,
			[status] ,
			[database_id],
			[object_id] ,
			[index_id],
			[partition_id],
			[alloc_unit_id],
			[file],
			[page],
			[slot],
			[ref_file] ,
			[ref_page],
			[ref_slot] ,
			[allocation] 
		)
		SELECT			
			@exec_guid,
			getdate(),
			[ErrorNum] ,
			[Level] ,
			[State] ,
			[MessageText],
			[RepairLevel] ,
			[Status] ,
			[DbId],
			[ObjectId] ,
			[IndexId],
			[PartitionId],
			[AllocUnitId],
			[File],
			[Page],
			[Slot],
			[RefFile] ,
			[RefPage],
			[RefSlot] ,
			[Allocation]
		FROM #t
	END
	ELSE
	BEGIN
		INSERT dbo.DBCCChecksLog(
			[execution_guid] , 
			[log_date],
			[error_num],
			[level],
			[State],
			[message_text],
			[repair_level],
			[status],
			[database_id],
			[db_frag_id],
			[object_id],
			[index_id],
			[partition_id],
			[alloc_unit_id],
			[rid_db_id],
			[rid_pru_id],
			[file] ,
			[page] ,
			[slot] ,
			[ref_db_id],
			[ref_pru_id],
			[ref_file],
			[ref_page],
			[ref_slot],
			[allocation])
		SELECT @exec_guid,
			getdate(),
			[ErrorNum],
			[Level],
			[State],
			[MessageText],
			[RepairLevel],
			[Status],
			[Dbid],
			[DbFragId],
			[ObjectId],
			[IndexId],
			[PartitionId],
			[AllocUnitId],
			[RidDbId],
			[RidPruId],
			[File] ,
			[Page] ,
			[Slot] ,
			[RefDbId],
			[RefPruId],
			[RefFile],
			[RefPage],
			[RefSlot],
			[Allocation]
		FROM #t
	END
	RAISERROR (@msg,16,1)
END
