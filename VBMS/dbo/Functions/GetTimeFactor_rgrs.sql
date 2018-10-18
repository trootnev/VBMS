-- THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE PROVIDED "AS IS" WITHOUT
--WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
--LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS
--FOR A PARTICULAR PURPOSE.


CREATE FUNCTION [dbo].[GetTimeFactor_rgrs] (
	@db INT
	,@tableid INT
	,@indexid INT
	,@partitionnum INT
	,@subsystem_id INT
	,@actiontype INT
	,@days_past INT = 30
	,@normalize INT = 1
	)
RETURNS FLOAT
AS
BEGIN
	DECLARE @RESULT FLOAT
		

	IF @normalize = 1
	BEGIN
		
SELECT @RESULT= k * (num + 1) + (sy - k * sx) / num
FROM (
	SELECT (num * sxy - sx * sy) / nullif(num * sxx - sx * sx, 0) AS k
		,sy
		,sx
		,num
	FROM (
		SELECT sum(y) AS sy
			,sum(x) AS sx
			,sum(x * y) AS sxy
			,sum(x * x) AS sxx
			,count(1) AS num
		FROM (
			SELECT ROW_NUMBER() OVER (
					ORDER BY date_started DESC
					) AS x
				,time_factor AS y
			FROM dbo.Tasks
			WHERE [database_id] = @db
			AND [table_id] = @tableid
			AND [index_id] = @indexid
			AND [partition_n] = @partitionnum
			AND [action_type_id] = @actiontype
			AND duration_s > 0
			AND datediff(dd, date_completed, getdate()) < @days_past
			AND time_factor IS NOT NULL
			AND exit_code = 1
			) a
		) a
	) a
	END
	ELSE
	BEGIN
		SELECT @RESULT = AVG(time_factor)
		FROM [dbo].[Tasks]
		WHERE [database_id] = @db
			AND [table_id] = @tableid
			AND [index_id] = @indexid
			AND [partition_n] = @partitionnum
			AND [action_type_id] = @actiontype
			AND duration_s > 0
			AND datediff(dd, date_completed, getdate()) < @days_past
			AND time_factor IS NOT NULL
			AND exit_code = 1
	END

	RETURN @RESULT
END