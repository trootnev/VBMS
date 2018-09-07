CREATE TABLE [dbo].[Workers] (
    [worker_name]       NVARCHAR (255) NOT NULL,
    [date_added]        DATETIME       CONSTRAINT [DF_Workers_date_added] DEFAULT (getdate()) NOT NULL,
    [owner]             NVARCHAR (150) DEFAULT 'John Doe' NOT NULL,
    [comment]           NVARCHAR (500) DEFAULT 'Owner did not add any comment' NOT NULL,
    [use_user_db]       BIT            DEFAULT 1 NOT NULL,
    [use_system_db]     BIT            DEFAULT 0 NOT NULL,
    [dbname_list]       NVARCHAR(500)  DEFAULT '' NOT NULL,
    [except_list]       BIT            DEFAULT 0 NOT NULL,
    [indexes]           BIT            DEFAULT 1 NOT NULL,
	[frag_eval_time_limit_s]	INT	   NULL,
	[stats]             BIT            DEFAULT 1 NOT NULL,
    [stats_sample] NVARCHAR(50) NULL DEFAULT 'FULLSCAN', 
	[checkall]          BIT            DEFAULT 1 NOT NULL,
    [checktable]        BIT            DEFAULT 1 NOT NULL,
    [checkalloc]        BIT            DEFAULT 1 NOT NULL,
    [checkcatalog]      BIT            DEFAULT 1 NOT NULL,
    [online_only]       BIT            DEFAULT 1 NOT NULL,
	[check_backup_state]	BIT			DEFAULT 1 NOT NULL,
	[check_ag_state]	BIT				DEFAULT 1 NOT NULL,
	[ag_max_queue]		BIGINT			DEFAULT 20000000 NOT NULL,
    [afterparty]        BIT            DEFAULT 1 NOT NULL,
    [add_stats_runtime] BIT            DEFAULT 1 NOT NULL,
    [totaltimemin]      BIGINT         NULL,
    [indextime]         BIGINT         NULL,
    [stattime]          BIGINT         NULL,
    [checktime]         BIGINT         NULL,
    
    [latched_spid] INT NULL DEFAULT 0, 
    [stoplight] BIT NULL DEFAULT 0, 
    CONSTRAINT [PK_Workers] PRIMARY KEY CLUSTERED ([worker_name] ASC) ON [PRIMARY],
    CONSTRAINT [CK_Workers_Time] CHECK (ISNULL(indextime,0)+ISNULL(stattime,0)+ISNULL(checktime,0) <= 100)
) ON [PRIMARY];


GO