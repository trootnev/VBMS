CREATE TABLE [dbo].[FragmentationData](
	[entry_id] [bigint] IDENTITY(1,1) NOT NULL,
	[batch_id] [uniqueidentifier] NOT NULL,
	[collection_date] [datetime] NOT NULL,
	[database_id] [smallint] NOT NULL,
	[schema_id] [int] NOT NULL,
	[object_id] [int] NOT NULL,
	[object_name] [nvarchar](517) NOT NULL,
	[index_id] [int] NOT NULL,
	[index_name] [sysname] NOT NULL,
	[allow_page_locks] [bit] NOT NULL,
	[legacy_col_count] [int] NOT NULL,
	[xml_col_count] [int] NOT NULL,
	[user_scans] [bigint] NOT NULL,
	[partition_count] [int] NOT NULL,
	[partition_number] [int] NOT NULL,
	[row_count] [bigint] NOT NULL,
	[size_mb] [decimal](10, 3) NOT NULL,
	[avg_fragmentation_in_percent] [decimal](10, 3) NULL,
	[analysis_started] [datetime] NULL,
	[analysis_completed] [datetime] NULL,
	[analysis_spid] [int] NULL,
 [analysis_duration_ms] BIGINT NULL, 
    [analysis_status] INT NULL, 
    [analysis_time_prognosis_ms] BIGINT NULL, 
    [exit_code] INT NULL, 
    [exit_message] NVARCHAR(4000) NULL, 
    [analysis_batch_id] UNIQUEIDENTIFIER NULL, 
    [time_factor] FLOAT NULL, 
    [volume_mount_point] NVARCHAR(255) NULL, 
    CONSTRAINT [PK_FragmentationData] PRIMARY KEY CLUSTERED 
(
	[entry_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_FragData_Status_entry_databaseid] ON [dbo].[FragmentationData]
(
	[analysis_status] ASC,
	[entry_id] ASC,
	[database_id] ASC,
	[avg_fragmentation_in_percent] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_FragData_dbid_objid_indexid_part_start_completed_code] ON [dbo].[FragmentationData]
(
	[database_id] ASC,
	[object_id] ASC,
	[index_id] ASC,
	[partition_number] ASC,
	[analysis_started] ASC,
	[analysis_completed] ASC,
	[exit_code] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_FragData_dbid_frag_spid_userscans] ON [dbo].[FragmentationData]
(
	[database_id] ASC,
	[avg_fragmentation_in_percent] ASC,
	[analysis_spid] ASC,
	[user_scans] ASC
)
INCLUDE ( 	[object_id],
	[index_id],
	[partition_number],
	[size_mb]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO