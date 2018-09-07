CREATE FUNCTION dbo.GetAGSyncState
(
@dbint_id int
,@ag_max_queue bigint
)

RETURNS BIT
AS
BEGIN
DECLARE @result bit = 0
select top 1 @result = 1 from sys.dm_hadr_database_replica_states
where database_id = @dbint_id
and is_local = 0
and (synchronization_health <> 2
or log_send_queue_size > @ag_max_queue
or redo_queue_size > @ag_max_queue)
RETURN @result
END