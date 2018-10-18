-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.


CREATE FUNCTION [dbo].[GetDbList]
(
	@use_system_db bit,
	@use_user_db bit,
	@dbname_list varchar(500),
	@except_list bit
)

RETURNS TABLE
AS
RETURN 	
SELECT database_id 
FROM sys.databases 
WHERE 
	is_read_only = 0 
	AND [state] = 0 
	AND ( 
		(@use_system_db=1 and name in ('master','msdb','VBMS')) 
		or 
		(@use_user_db=1 and name not in ('master','msdb','model','tempdb','VBMS')) 
	)
	AND (
		@dbname_list='' 
		or 
		(@except_list=0 and ','+@dbname_list+',' like '%,'+name+',%') 
		or 
		(@except_list=1 and ','+@dbname_list+',' not like '%,'+name+',%') 
	)

