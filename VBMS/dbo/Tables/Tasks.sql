CREATE TABLE [dbo].[Tasks] (
    [entry_id]        BIGINT           IDENTITY (1, 1) NOT NULL,
    [batch_id]        UNIQUEIDENTIFIER NOT NULL,
    [subsystem_id]       INT              NOT NULL,
    [action_type_id]     INT              NOT NULL,
    [command]         NVARCHAR (4000)   NOT NULL,
    [date_added]      DATETIME         NOT NULL,
    [date_started]    DATETIME         NULL,
    [date_completed]  DATETIME         NULL,
    [duration_s]      INT              NULL,
    [database_id]        INT              NOT NULL,
    [table_id]        INT              NULL,
    [index_id]        INT              NULL,
    [partition_n]     INT              NULL,
    [maxdop]          INT              NULL,
    [user_scans]      BIGINT              NULL,
    [user_seeks]      BIGINT              NULL,
    [user_updates]    BIGINT              NULL,
    [rowcnt]          BIGINT           NULL,
    [size_mb]         DECIMAL(38, 3)       NULL,
    [ix_frag]         DECIMAL(10, 3)       NULL,
    [time_factor]     FLOAT (53)       NULL,
    [rowmod_factor]   FLOAT (53)       NULL,
    [checked_daysago] INT              NULL,
    [result]          NVARCHAR (MAX)   NULL,
    [time_prognosis_s] BIGINT              NULL,
    [exit_code]       INT              NULL,
    [worker_name]     NVARCHAR (255)    NULL,
    [execution_id]  UNIQUEIDENTIFIER NULL,
    [priority] INT NULL, 
    [dataspace_name] NVARCHAR(255) NULL, 
    CONSTRAINT [PK_TasksHistory] PRIMARY KEY CLUSTERED ([entry_id] ASC),
    CONSTRAINT [FK_Tasks_OperationTypes] FOREIGN KEY ([subsystem_id], [action_type_id]) REFERENCES [dbo].[OperationTypes] ([subsystem_id], [action_type_id])
);


GO
CREATE NONCLUSTERED INDEX [NC_IX_duration_plus_five]
    ON [dbo].[Tasks]([time_prognosis_s] ASC)
    INCLUDE([entry_id], [subsystem_id], [action_type_id], [database_id], [table_id], [index_id], [partition_n])
    ON [PRIMARY];


GO
CREATE NONCLUSTERED INDEX [NC_db_tbl_ix_prt]
    ON [dbo].[Tasks]([database_id] ASC, [table_id] ASC, [index_id] ASC, [partition_n] ASC, [date_completed] ASC, [subsystem_id] ASC, [action_type_id] ASC, [time_factor] ASC, [exit_code] ASC)
    INCLUDE([duration_s], [ix_frag]) WITH (FILLFACTOR = 50, PAD_INDEX = ON)
    ON [PRIMARY];


GO
CREATE NONCLUSTERED INDEX [NC_db_subs_dc]
    ON [dbo].[Tasks]([database_id] ASC, [subsystem_id] ASC, [date_completed] ASC);

