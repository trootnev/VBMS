# VBMS
SLA-aware SQL Server Maintenance Framework (Index rebuild, stats update, partial integrity checks)
## System purpose
System has been created to solve the problem of SLA violation during SQL Server Database Maintenance, that often been a reason why some of our customers gave up database maintenance at all. System has necessary capabilities to solve this problem:
+	Executes only needed maintenance tasks, evaluating database state
+	Prioritizes tasks to ensure the most important tasks are executed
+	Predicts execution time
+	Keeps the track of time during execution and does not start next task if it probably won’t fit remaining timeframe.
## Supported Maintenance Tasks
Current version of system can perform various types of maintenance tasks
### Index maintenance:
+	Offline index rebuild,
+	Online index rebuild (if it is possible due to columns data type, SQL Server version, edition and system settings),
+	Index reorganization,
### Statistics update
+	Update statistics with FULLSCAN,
+	With limited SAMPLE, 
+	With previously used SAMPLE (RESAMPLE).

### Integrity checks
+	Metadata checks (CHECKCATALOG),
+	Allocation checks (CHECKALLOC)
+	Single table integrity check (CHECKTABLE)

## Key features
+	Fully modular execution model can help adapt to any constraints
+	Scalability – most resource-intensive tasks can be configured to run in several threads – Frag evaluation tasks, execution of maintenance tasks (index maintenance, stats update, integrity checks)
+	Prediction – system uses ML algorithm to predict how much time each task can take based on previous executions
+	SLA-awareness – system will skip tasks that probably won’t fit in the remaining maintenance window 
+	 Time budget – Each phase (Tasks generation, Index maintenance, Stats Update and Integrity Checks) has its separate time budget. If some time remains then system will redistribute it, use for retries of failed or skipped tasks.
+	Locking control – utilizes WAIT_AT_LOW_PRIORITY feature or LOCK_TIMEOUT to avoid long blocking.
+	Soft stopping feature – at any time execution can be stopped either soft (finish current task and stop) or hard (just stop).
+	Granular Integrity Checks – instead of running full DBCC CHECKDB more granular checks can be run (CHECK CATALOG, CHECKALLOC, CHECKTABLE). All with time control.
+	Monitoring of AG synchronization. If redo queue exceeds the given threshold or AG is unhealthy then index maintenance task is skipped for this database. 
+	Monitoring of running backups – can configure to skip index maintenance tasks for a database if running backup is detected to avoid log overflowing.
+	Log space control – system can control the amount of space left for transaction logging (including in ldf file and the volume it resides on)
+	Blacklisting of some operations or objects (exclusion from maintenance)

## System components Overview
System is SQL Server database and a set of SQL Server Agent jobs.
Database name is VBMS (Value Based Maintenance System – because system evaluates every task and executes the most valuable ones first). Database name should not be changed as it is used in code.
### SQL Server Agent Jobs can be of three types:
+	Tasks generators (FillQueueAll)
+	Workers (StartWorker)
+	Killers (KillWorkers)
Tasks generation can be executed before maintenance time as it probably may not interrupt normal work. Needs to be tested on each solution if implied performance impact is acceptable.
