CREATE TABLE [dbo].[DbVersion] (
    [Id]            INT           IDENTITY (1, 1) NOT NULL,
    [Version]       NVARCHAR (20) NOT NULL,
    [DateInstalled] DATETIME      NOT NULL,
    PRIMARY KEY CLUSTERED ([Id] ASC)
);

