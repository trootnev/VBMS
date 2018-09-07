CREATE VIEW [dbo].[FragAnalysisStatus]
	AS
	select 
       batch_id,
       MAX(collection_date) as MaxDateAdded,
       MIN(analysis_started) as FirstAnalysisStarted,
       CASE WHEN analysis_status = 0 then NULL ELSE  max(analysis_completed) END as RecentAnalysisCompleted, 
       ISNULL(DATEDIFF(SECOND,MIN(analysis_started),COALESCE(MAX(analysis_completed),getdate())),0) as Duration,
       CASE analysis_status       
             WHEN 0 THEN 'Scheduled' 
             WHEN 1 THEN 'Analized' 
             WHEN 2 THEN 'Completed'
             ELSE 'Error' END as [Status],
       exit_code,
       count(*) as [TasksCount],
       SUM(size_mb) as SizeMB,
       SUM(size_mb)*100.00/SUM(SUM(size_mb)) over (PARTITION BY batch_id) as [SizePercentTotal]
       ,COUNT(*)*100.00/SUM(count(*)) over(PARTITION BY batch_id) as [CountPercentTotal]
  
  FROM [dbo].[FragmentationData]
  GROUP BY batch_id,analysis_status,exit_code
  
