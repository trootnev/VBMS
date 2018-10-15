CREATE PROCEDURE [dbo].[AddBlacklistItem]
	@entryid bigint = NULL,
	@database_id INT = NULL , 
    @table_id INT = NULL, 
    @index_id INT = NULL, 
    @partition_n INT = NULL, 
    @subsystem_id INT = NULL, 
    @action_type_id INT = NULL,
	@worker_name nvarchar(255) = NULL
AS

IF @entryid is not null 
BEGIN 

	IF @database_id is not null or @table_id is not null or @index_id is not null or @partition_n is not null or @subsystem_id is not null or @action_type_id is not null
	BEGIN
		RAISERROR('Parameter @entryid is incompatible with other parameters!',16,1)
		RETURN
	END

	SELECT 
		@database_id=[database_id],
		@table_id = table_id,
		@index_id = index_id,
		@partition_n = partition_n,
		@subsystem_id = CASE WHEN @subsystem_id is null THEN subsystem_id ELSE @subsystem_id END,
		@action_type_id = CASE WHEN @action_type_id is null THEN action_type_id ELSE @action_type_id END
	FROM dbo.Tasks
	WHERE entry_id = @entryid

	IF @subsystem_id <> 1 
		SET @partition_n = NULL
		
END
ELSE
BEGIN
	IF @database_id is null
	BEGIN
		RAISERROR('database_id not defined!',16,1)
		RETURN
	END

	IF @subsystem_id is null and @database_id is null
	BEGIN
		RAISERROR('subsystem_id not defined while DB Id is also not defined.',16,1)
		RETURN
	END

	IF @table_id is null and @subsystem_id <> 3 and @action_type_id not in (1,2)
	BEGIN
		RAISERROR('table_id not defined! P.s. You can not blacklist the whole DB this way.',16,1)
		RETURN
	END
END	
	INSERT INTO dbo.Blacklist 
	(
		[database_id], 
		[table_id], 
		[index_id], 
		[partition_n], 
		[subsystem_id], 
		[action_type_id],
		[enabled],
		[worker_name]
	)
	VALUES
	( 
		@database_id,
		@table_id,
		@index_id,
		@partition_n,
		@subsystem_id, 
		@action_type_id,
		1
		,@worker_name
	)
