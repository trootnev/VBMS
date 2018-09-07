CREATE FUNCTION [dbo].[GetDBBackupState]
(
	@dbint_id int
)
RETURNS bit
AS
BEGIN
	DECLARE @result bit = 0
	select top 1 @result = 1 from sys.dm_exec_requests 
	where command like 'BACKUP DATABASE%' and database_id = @dbint_id
	RETURN @result
END
