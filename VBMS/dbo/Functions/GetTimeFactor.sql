
/*
-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.


Author: Oleg Trutnev otrutnev@microsoft.com

Russian:

Скалярная функция получения величины time factor, которая используется для предсказания времени выполнения заданий. 
Используется статистика ранее выполненных заданий. Для отсечки девиаций используется 2-сигма окрестность.
*/

CREATE FUNCTION [dbo].[GetTimeFactor] (
	@db INT
	,@execution bit --  1 - tasks execution or 0 - index fragmentation evaluation
	,@tableid INT
	,@indexid INT
	,@partitionnum INT
	,@subsystem_id INT = 1
	,@action_type_id INT =1
	,@days_past INT = 60
	,@normalize INT = 1
	)
RETURNS FLOAT
AS
BEGIN
	DECLARE 
		@RESULT FLOAT
		,@avg FLOAT
		,@dev FLOAT

IF @execution = 1
BEGIN
	
	SELECT 
		@avg = avg(time_factor)
		,@dev = stdev(time_factor)
	FROM [dbo].[Tasks] (NOLOCK)
	WHERE [database_id] = @db
		AND ((table_id is null and @tableid is null) or (table_id = @tableid))
		AND ((index_id is null and @indexid is null) or (index_id = @indexid))
		AND ((partition_n is null and @partitionnum is null) or (partition_n = partition_n))
		AND subsystem_id = @subsystem_id
		AND [action_type_id] = @action_type_id
		AND datediff(dd, date_completed, getdate()) < @days_past
		AND time_factor IS NOT NULL
		AND exit_code = 1
		and size_mb > 0
	
	IF @normalize = 1 AND @dev IS NOT NULL
	BEGIN
		SELECT @RESULT = AVG(time_factor)
		FROM [dbo].[Tasks] (NOLOCK)
		WHERE [database_id] = @db
			AND ((table_id is null and @tableid is null) or (table_id = @tableid))
			AND ((index_id is null and @indexid is null) or (index_id = @indexid))
			AND ((partition_n is null and @partitionnum is null) or (partition_n = partition_n))
			AND subsystem_id = @subsystem_id
			AND [action_type_id] = @action_type_id
			AND time_factor BETWEEN @avg - @dev
				AND @avg + @dev
			AND  date_completed > DATEADD(dd,-1*@days_past,getdate())
			AND time_factor IS NOT NULL
			AND exit_code = 1
			and size_mb > 0

		


	END
	ELSE
	BEGIN
		SET @RESULT = @avg
	END
	--If no data available then try to use history of other tasks
	IF @RESULT is NULL --try find data for another partition
			SELECT @RESULT = AVG(time_factor)
			FROM [dbo].[Tasks] (NOLOCK)
			WHERE [database_id] = @db
			AND ((table_id is null and @tableid is null) or (table_id = @tableid))
			AND ((index_id is null and @indexid is null) or (index_id = @indexid))
			--AND ((partition_n is null and @partitionnum is null) or (partition_n = partition_n))
			AND subsystem_id = @subsystem_id
			AND [action_type_id] = @action_type_id
			
			AND  date_completed > DATEADD(dd,-1*@days_past,getdate())
			AND time_factor IS NOT NULL
			AND exit_code = 1
			and size_mb > 0
			
			IF @RESULT is NULL --if still nothing try another index on same table
				SELECT @RESULT = AVG(time_factor)
			FROM [dbo].[Tasks] (NOLOCK)
			WHERE [database_id] = @db
			AND ((table_id is null and @tableid is null) or (table_id = @tableid))
			--AND ((index_id is null and @indexid is null) or (index_id = @indexid))
			--AND ((partition_n is null and @partitionnum is null) or (partition_n = partition_n))
			AND subsystem_id = @subsystem_id
			AND [action_type_id] = @action_type_id
			
			AND  date_completed > DATEADD(dd,-1*@days_past,getdate())
			AND time_factor IS NOT NULL
			AND exit_code = 1
			and size_mb > 0
					
					IF @RESULT is NULL --If still nothing try another table
						SELECT @RESULT = AVG(time_factor)
							FROM [dbo].[Tasks] (NOLOCK)
							WHERE [database_id] = @db
							--AND ((table_id is null and @tableid is null) or (table_id = @tableid))
							--AND ((index_id is null and @indexid is null) or (index_id = @indexid))
							--AND ((partition_n is null and @partitionnum is null) or (partition_n = partition_n))
							AND subsystem_id = @subsystem_id
							AND [action_type_id] = @action_type_id
							
							AND  date_completed > DATEADD(dd,-1*@days_past,getdate())
							AND time_factor IS NOT NULL
							AND exit_code = 1
							and size_mb > 0

							IF @RESULT is NULL --may be other database?
						SELECT @RESULT = AVG(time_factor)
							FROM [dbo].[Tasks] (NOLOCK)
							WHERE
							--[database_id] = @db
							--AND ((table_id is null and @tableid is null) or (table_id = @tableid))
							--AND ((index_id is null and @indexid is null) or (index_id = @indexid))
							--AND ((partition_n is null and @partitionnum is null) or (partition_n = partition_n))
							subsystem_id = @subsystem_id
							AND [action_type_id] = @action_type_id
							AND  date_completed > DATEADD(dd,-1*@days_past,getdate())
							AND time_factor IS NOT NULL
							AND exit_code = 1
							and size_mb > 0

					IF @RESULT is NULL --well, if nothing is available, then we'll use basic values for this kind of operations. cross your fingers...
						SET @RESULT = CASE @subsystem_id 
										WHEN  1 then 0.05 
										WHEN 2 THEN 0.05 
										WHEN 3 THEN CASE @action_type_id 
													WHEN 1 then 0.025
													when 2 then 0.05 
													when 3 then 0.05 
													end 
										end 	
END

ELSE --If we deal with fragmentation data and not the execution statistics
	BEGIN
	SELECT 
				@avg = avg(time_factor)
				,@dev = stdev(time_factor)
			FROM [dbo].[FragmentationData] (NOLOCK)
			WHERE [database_id] = @db
				AND (object_id = @tableid)
				AND (index_id = @indexid)
				AND (partition_number = @partitionnum)
				
				AND analysis_completed > DATEADD(dd,-1*@days_past,GETDATE())
				AND time_factor IS NOT NULL
				AND exit_code = 1
	
			IF @normalize = 1 AND @dev IS NOT NULL
			BEGIN
				SELECT @RESULT = AVG(time_factor)
				FROM [dbo].[FragmentationData] (NOLOCK)
				WHERE [database_id] = @db
					AND (object_id = @tableid)
				AND (index_id = @indexid)
				AND (partition_number = @partitionnum)
					AND time_factor BETWEEN @avg - @dev
						AND @avg + @dev
					AND analysis_completed > DATEADD(dd,-1*@days_past,GETDATE())
					AND time_factor IS NOT NULL
					AND exit_code = 1

					END
	ELSE
	BEGIN
		SET @RESULT = @avg
	END

	
	END
RETURN @RESULT
END