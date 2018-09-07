/*

-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.

Author: Arseny Birukov arsbir@microsoft.com

Это представление показывает агрегированную статистику по выполненным таскам
This view shows agregated statistics on executed tasks

*/


CREATE VIEW Executions
AS
SELECT 
	worker_name,
	[execution_id],
	ot.subsystem_name,
	ot.action_type_name,
	count(*) as total_task_count,
	count(CASE WHEN exit_code = 1 THEN 1 ELSE NULL END) as successful_task_count,
	count(CASE WHEN exit_code > 1 THEN 1 ELSE NULL END) as failed_task_count,
	count(CASE WHEN exit_code < 0 THEN 1 ELSE NULL END) as skipped_task_count,
	sum(DATEDIFF(ms,date_started, date_completed))/1000 as total_duration_s,
	sum(CASE WHEN exit_code > 1 THEN DATEDIFF(ms,date_started, date_completed) ELSE 0 END)/1000 as failed_duration_s,
	min(date_started) as first_task_started,
	max(date_completed) as last_task_completed,
	max(entry_id) as last_entry_id  --this column is needed for automated reports collection system
FROM 
	dbo.Tasks t
	INNER JOIN dbo.OperationTypes ot ON t.subsystem_id = ot.subsystem_id AND t.action_type_id = ot.action_type_id
WHERE
	[execution_id] is not null --Exclude not yet executed
GROUP BY
	[execution_id],
	worker_name,
	ot.subsystem_name,
	ot.action_type_name
--ORDER BY
--	worker_name,
--	execution_guid,
--	first_task_started