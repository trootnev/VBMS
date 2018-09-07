
CREATE TABLE [dbo].[DBCCChecksLog](
	[rec_id] [int] IDENTITY(1,1) NOT NULL,
	[execution_guid] [uniqueidentifier] NULL,
	log_date [datetime] NULL DEFAULT (getdate()),
	error_num [smallint] NULL,
[level] [tinyint] NULL,
[State] [tinyint] NULL,
[message_text] [varchar](500) NULL,
[repair_level] [varchar](40) NULL,
[status] [tinyint] NULL,
[database_id] [smallint] NULL,
[db_frag_id] [int] NULL,
[object_id] [int] NULL,
[index_id] [int] NULL,
[partition_id] [bigint] NULL,
[alloc_unit_id] [bigint] NULL,
[rid_db_id] [int] NULL,
[rid_pru_id] [int] NULL,
[file] [int] NULL,
[page] [int] NULL,
[slot] [int] NULL,
[ref_db_id] [int] NULL,
[ref_pru_id] [int] NULL,
[ref_file] [int] NULL,
[ref_page] [int] NULL,
[ref_slot] [int] NULL,
[allocation] [int] NULL
PRIMARY KEY CLUSTERED 
(
[rec_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO



