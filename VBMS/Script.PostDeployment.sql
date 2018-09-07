DECLARE @Ops TABLE ([subsystem_id] int, [action_type_id]int, [subsystem_name] nvarchar(255), [action_type_name] nvarchar(255))
DECLARE @Params TABLE ([parameter] nvarchar(50), [string_value] nvarchar(150), [int_value] BIGINT, [float_value] FLOAT (53), [description] NVARCHAR (MAX) )

INSERT @Ops ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name]) VALUES (0, 1, N'Queue Generation', N'Generation of tasks for further execution')

INSERT @Ops ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name]) VALUES (1, 1, N'Index Maintenance\Обслуживание индексов', N'Offline rebuild\Перестройка индекса оффлайн')

INSERT @Ops ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name]) VALUES (1, 2, N'Index Maintenance\Обслуживание индексов', N'Online rebuild\Перестройка индекса онлайн')

INSERT @Ops ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name]) VALUES (1, 3, N'Index Maintenance\Обслуживание индексов', N'Reorganize\Реорганизация индекса')

INSERT @Ops ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name]) VALUES (2, 1, N'Statistics maintenance\Обслуживание статистики', N'Update statistics\Пересчёт статистки')

INSERT @Ops ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name]) VALUES (3, 1, N'Integrity check\Проверка целостности', N'CHECKCATALOG\Проверка целостности системного каталога')

INSERT @Ops ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name]) VALUES (3, 2, N'Integrity check\Проверка целостности', N'CHECKALLOC\Проверка целостности в части размещения (DBCC CHECKALLOC)')

INSERT @Ops ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name]) VALUES (3, 3, N'Integrity check\Проверка целостности', N'CHECKTABLE\Проверка целостности отдельной таблицы')


INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'CheckTableIntervalDays', NULL, 7, NULL, N'Days between CHECKTABLE\Периодичность проверки целостности отдельно взятой таблицы')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'MaxDop', NULL, 2, NULL, N'Max Degree of parallelism for index operations\Количество ядер, которые могут быть использованы для операций с индексами')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'CheckCatalogIntervalDays', NULL, 7, NULL, N'Days between CHECKCATALOG\Периодичность проверки целостности системных таблиц БД')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'HistoryRetentionDays', NULL, 90, NULL, N'Old tasks will be removed after this number of days\Продолжительность хранения журнала заданий')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'IndexReorgThresholdPercent', NULL, 30, NULL, N'Maximum index fragmentation when reorganize is prefered instead of rebuild\Максимальное значение фрагментации индекса для использования REORGANIZE')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'IndexOnlineRebuild', NULL, 1, NULL, N'Use or not ONLINE where possible. Online operations are slower\Флаг использования ONLINE при перестройке индексов. Его использование увеличивает продолжительность операции, но сохраняет индекс доступным по время обслуживания')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'TotalMaintenanceWindowMm', NULL, 60, NULL, N'Total amount of time (minutes) for maintenance\Общая продолжительность технологического окна в минутах')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'IndexMaintenanceWindowPercent', NULL, 30, NULL, N'Percent of time to spend on index maintenance\Процент времени, отведённого под обслуживание индексов')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'StatMaintenanceWindowPercent', NULL, 30, NULL, N'Percent of time to spend on statistics maintenance\ Процент времени, отведённого под обслуживание статистики')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'CheckWindowPercent', NULL, 30, NULL, N'Percent of time to spend on integrity checks\Процент времени, отведённого под проверку целостности')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'CheckAllocIntervalDays', NULL, 7, NULL, N'Days between CHECKALLOC\Периодичность проверки целостноси в части аллокации экстентов')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'LockTimeoutMs', NULL,20000 , NULL, N'How long (ms) should index operation wait being locked\Максимальное время в заблокированном состоянии. При превышении задание будет остановлено.')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'SortInTempdb', NULL,0 , NULL, N'Should indexes be sorted in tempdb during index rebuild?\Использование tempdb для сортировки индекса во время перестройки')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'IndexFragLowerThreshold', NULL,10 , NULL, N'Index fragmentation lower limit. Index with fragmentation lower than this wont be maintaintes \ Нижняя граница фрагментации. Индекс с меньшей фрагментацией обслуживаться не будет')
INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'TranLogSpaceThresholdMb', NULL,500 , NULL, N'Transaction log space left threshold(mountpoints not supported yet)/Минимальный размер свободного места под лог транзакций для операций обслуживания индексов (mountpoint не поддерживается пока)')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'IndexMinimumSizeMB', NULL,10 , NULL, N'Minimum size of index that can be rebuilt or reorganized/Минимальный размер индекса, подлежащего обслуживанию')

INSERT @Params ([parameter], [string_value], [int_value], [float_value], [description]) VALUES (N'FragCollectionTimeLimitS', NULL,3600 , NULL, N'Maximum duration of index fragmentation analysis phase/Максимальная продолжительность фазы анализа фрагментации индексов')

MERGE dbo.Parameters as TARGET
USING (SELECT * FROM @Params) as SOURCE ([parameter], [string_value], [int_value], [float_value], [description])
ON (TARGET.parameter COLLATE DATABASE_DEFAULT = SOURCE.parameter COLLATE DATABASE_DEFAULT)
WHEN MATCHED THEN UPDATE SET description = SOURCE.description
WHEN NOT MATCHED THEN INSERT ([parameter], [string_value], [int_value], [float_value], [description])
VALUES (SOURCE.[parameter], SOURCE.[string_value], SOURCE.[int_value], SOURCE.[float_value], SOURCE.[description]);



MERGE dbo.OperationTypes as TARGET
USING (SELECT * FROM @Ops) as SOURCE ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name])
ON (TARGET.subsystem_id = SOURCE.subsystem_id and TARGET.action_type_id = SOURCE.action_type_id)

WHEN MATCHED THEN UPDATE SET subsystem_name = SOURCE.subsystem_name, action_type_name = SOURCE.action_type_name
WHEN NOT MATCHED THEN INSERT ([subsystem_id], [action_type_id], [subsystem_name], [action_type_name])
VALUES (SOURCE.[subsystem_id], SOURCE.[action_type_id], SOURCE.[subsystem_name], SOURCE.[action_type_name]);



INSERT dbo.DbVersion (Version,DateInstalled) VALUES ('2.0.2.5 Beta',getdate())
GO

UPDATE VBMS.dbo.Tasks 
SET exit_code = -1
WHERE result = 'Skipped. Not enough time left'
and exit_code is null


/*

changelog:
2.0.2.5b
	- Increased ag_queue_size to 200000000

2.0.2.5a
	- FIX: 3 entries for a non-partitioned clustered index
	- FIX: Clustered columnstore index statistics update tasks where generated which are not possible to execute.
	- FIX: Statistics update tasks are generated with no regard to personal blacklist entries (worker_name field is ignored)
	- FIX: Stats that were added during execution loop has unrealisticly prescise time prognosis due to post-execution evaluation

2.0.2.4b
	- Added volume_mount_point to collection to improve future prediction model
	- FillQueueAll - added @debug parameter. If @debug = 1 procedure returns the list of frag analysis results and tasks generated
2.0.2.3b
	- FIX - afterparty does not work. at all. Old stupid typo.
	- dbo.StartWorker - added NOCOUNT ON.

2.0.2.2b
	- Added view dbo.FragAnalysisStatus to view statistics of framentation analysis tasks
	- Added support of creating blacklist items for a specific workers.
	- FIX - apriori timefactors too high 
	- FIX - time_prognosis_s column type changed to bigint to avoid overflow 
	- Tables with 0 records are excluded from checks
	- FillQueue is now managed as a worker
	- FillQueue - frag collection tasks now support LOCK_TIMEOUT feature to avoid hanging on frag collection
	- FIllQueue - frag collection now supports blacklists

*/