CREATE TABLE [dbo].[Parameters] (
    [parameter]    NVARCHAR (50)  COLLATE Cyrillic_General_CI_AS NOT NULL,
    [string_value] NVARCHAR (150) COLLATE Cyrillic_General_CI_AS NULL,
    [int_value]    BIGINT         NULL,
    [float_value]  FLOAT (53)     NULL,
    [description]  NVARCHAR (MAX) COLLATE Cyrillic_General_CI_AS NULL
);

