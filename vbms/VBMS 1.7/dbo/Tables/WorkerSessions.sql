CREATE TABLE [dbo].[WorkerSessions] (
    [record_date]   DATETIME       DEFAULT getdate() NOT NULL,
    [worker_name]   NVARCHAR (255) NOT NULL,
    [session_id]    INT            NOT NULL,
    [program_name]  NVARCHAR (150) NULL,
    [subsystem_id]     INT            NULL,
    [entry_id]      BIGINT         NULL,
    [is_afterparty] BIT            DEFAULT 0 NULL,
    CONSTRAINT [FK_WorkerSession_Workers] FOREIGN KEY ([worker_name]) REFERENCES [dbo].[Workers] ([worker_name]),
    CONSTRAINT [AK_WorkerSession_worker_name] UNIQUE NONCLUSTERED ([worker_name] ASC)
) ON [PRIMARY];

