/*
Procedure to add more workers by cloning existing one.

--dbo.CloneWorkers 'AW',2 --add two workers to AW
--dbo.CloneWorkers @worker_name= 'AW',@workers_num = 6, @already_have =2

*/

CREATE PROCEDURE dbo.CloneWorkers (
@worker_name nvarchar(255) --worker to use as a template,
,@workers_num int --number of workers to add
,@already_have int = 1 --numbers of workers, that we already got. Used to avoid PK violation.,
,@name_postfix nvarchar(20) = '_' --postfix to use between the worker name and it's number. Ex: worker_1, worker_2
)
AS
SET NOCOUNT ON
DECLARE @i int 
 
SET @i = @already_have+1

WHILE @i <= @already_have+@workers_num
	BEGIN
	BEGIN TRY
	INSERT INTO [dbo].[Workers]
           ([worker_name]
           ,[date_added]
           ,[owner]
           ,[comment]
           ,[use_user_db]
           ,[use_system_db]
           ,[dbname_list]
           ,[except_list]
           ,[indexes]
           ,[stats]
           ,[stats_sample]
           ,[checkall]
           ,[checktable]
           ,[checkalloc]
           ,[checkcatalog]
           ,[online_only]
           ,[afterparty]
           ,[add_stats_runtime]
           ,[totaltimemin]
           ,[indextime]
           ,[stattime]
           ,[checktime]
           )
     SELECT 
			@worker_name+@name_postfix+CAST(@i as nvarchar(5))
	       ,[date_added]
           ,[owner]
           ,[comment]
           ,[use_user_db]
           ,[use_system_db]
           ,[dbname_list]
           ,[except_list]
           ,[indexes]
           ,[stats]
           ,[stats_sample]
           ,[checkall]
           ,[checktable]
           ,[checkalloc]
           ,[checkcatalog]
           ,[online_only]
           ,[afterparty]
           ,0 --in parallel mode it is not recommended to use add_stats_runtime in several threads or duplicate tasks can be generated.
           ,[totaltimemin]
           ,[indextime]
           ,[stattime]
           ,[checktime]
	FROM dbo.Workers
	WHERE worker_name = @worker_name
	END TRY
	BEGIN CATCH
		THROW 51000,'Error during worker creation. Please check if it is already registered. If you need to add more workers please use starting_num parameter to set the starting position in naming to avoid duplicates.',1
	END CATCH
	SET @i = @i+1
	END
PRINT 'Workers have been created! Please note, that add_stats_runtime has been set to 0 for the added workers! Only one worker in the pack can have it set to 1 to avoid duplicate stats tasks creation during the execution.'





