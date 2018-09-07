/*
-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Oleg Trutnev otrutnev@microsoft.com



Russian:
Процедура прерывания процессов обработки очереди заданий (Worker). 
Процедура может принимать в качестве входного параметра список имен профилей через запятую без пробелов.
Если Worker обнаружен в состоянии выполения заданий обслуживания индексов, то прерывания не происходит,
 выполняется ожидание завершения этого процесса.
Это сделано с целью избежания отката операции с индексом, который может потребовать даже больше времени.  
Эта проверка отменяется параметром @force.

English:
Procedure designed to kill one or several Workers (names supplied as a list without spaces). 
If worker is found performing index operations then
procedure waits, to avoid rollback. If @force = 1, then worker is killed immediately.


*/



CREATE PROCEDURE [dbo].[KillWorkers] (@worker_names nvarchar(500) = '', @except_list bit = 0, @force bit = 0)
AS

DECLARE @spid INT
DECLARE @sql NVARCHAR(500) = ''
DECLARE @subsystem_id INT
DECLARE @entry_id bigint
DECLARE @victim nvarchar(50)

DECLARE workers CURSOR FORWARD_ONLY
	FOR SELECT worker_name, session_id, subsystem_id, entry_id from dbo.[WorkerSessions] 
	WHERE 
 (
      @worker_names='' 
      or 
      (@except_list=0 and ','+@worker_names+',' like '%,'+worker_name+',%') 
      or 
      (@except_list=1 and ','+@worker_names+',' not like '%,'+worker_name+',%') 
)

WHILE EXISTS (
SELECT 1 FROM dbo.[WorkerSessions] 
	WHERE 
 (
      @worker_names='' 
      or 
      (@except_list=0 and ','+@worker_names+',' like '%,'+worker_name+',%') 
      or 
      (@except_list=1 and ','+@worker_names+',' not like '%,'+worker_name+',%') 
)

)
BEGIN

DELETE FROM dbo.[WorkerSessions]
WHERE not exists(
	select 1 from sys.dm_exec_sessions es 
		where es.session_id = dbo.[WorkerSessions].session_id 
		and es.program_name COLLATE DATABASE_DEFAULT = dbo.[WorkerSessions].program_name COLLATE DATABASE_DEFAULT)

	
OPEN workers

FETCH NEXT FROM workers
INTO @victim, @spid,@subsystem_id, @entry_id

WHILE @@FETCH_STATUS = 0 
BEGIN
IF (@subsystem_id <> 1 or @force = 1)
	BEGIN
	SET @sql = 'KILL ' + cast(@spid AS NVARCHAR(10)) + '
			UPDATE dbo.Tasks SET result = ''Killed'',exit_code = 0, date_completed = getdate() WHERE entry_id = '+cast(@entry_id as nvarchar(10))+' 
			UPDATE dbo.Workers SET stoplight = 0 where worker_name = ''' + CAST(@victim AS NVARCHAR(36)) + ''' and stoplight <> 0
			DELETE FROM dbo.WorkerSessions where worker_name = ''' + CAST(@victim AS NVARCHAR(36)) + ''''

			EXEC (@sql)
			PRINT 'Worker ''' + CAST(@victim AS NVARCHAR(36)) + ''' has been killed at step' + cast(@subsystem_id AS NVARCHAR(5)) + ' at ' + CAST(GETDATE() as NVARCHAR(30)) + '. R.I.P. Bro...'
	END
ELSE
BEGIN
UPDATE dbo.Workers 
SET stoplight = -1
WHERE worker_name = @victim
AND stoplight = 0
END
FETCH NEXT FROM workers
INTO @victim, @spid,@subsystem_id, @entry_id

END

CLOSE workers
WAITFOR DELAY '00:00:05'
END

DEALLOCATE workers