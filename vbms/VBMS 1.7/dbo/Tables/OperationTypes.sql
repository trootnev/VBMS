CREATE TABLE [dbo].[OperationTypes] (
    [subsystem_id]     INT            NOT NULL,
    [action_type_id]   INT            NOT NULL,
    [subsystem_name]   NVARCHAR (255) COLLATE Cyrillic_General_CI_AS NOT NULL,
    [action_type_name] NVARCHAR (255) COLLATE Cyrillic_General_CI_AS NOT NULL,
    CONSTRAINT [PK_OperationTypes] PRIMARY KEY CLUSTERED ([subsystem_id] ASC, [action_type_id] ASC)
);

