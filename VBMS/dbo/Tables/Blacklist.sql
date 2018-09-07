CREATE TABLE [dbo].[Blacklist]
(
	[item_id] BIGINT NOT NULL PRIMARY KEY IDENTITY, 
	[date_added] DATETIME NOT NULL DEFAULT getdate(),
    [database_id] INT NOT NULL, 
    [table_id] INT NULL, 
    [index_id] INT NULL, 
    [partition_n] INT NULL, 
    [subsystem_id] INT NULL, 
    [action_type_id] INT NULL, 
    [enabled] BIT NOT NULL DEFAULT 1, 
    [worker_name] NVARCHAR(255) NULL
)
