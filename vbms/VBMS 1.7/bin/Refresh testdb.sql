
SET NOCOUNT ON

 TRUNCATE TABLE TinyTable


INSERT INTO TinyTable VALUES(1)
GO

TRUNCATE TABLE SmallHeap
GO

INSERT INTO SmallHeap VALUES (NEWID(),REPLICATE(N'a',100))

DECLARE @i int
SET @i = 0

WHILE @i < 16
BEGIN
	INSERT INTO SmallHeap SELECT NEWID(),REPLICATE(N'a',100) FROM SmallHeap
	SET @i = @i + 1
END

TRUNCATE TABLE SmallClustered

GO

INSERT INTO SmallClustered VALUES (NEWID(),REPLICATE(N'a',100))

DECLARE @i int
SET @i = 0

WHILE @i < 16
BEGIN
	INSERT INTO SmallClustered SELECT NEWID(),REPLICATE(N'a',100) FROM SmallClustered
	SET @i = @i + 1
END
GO

TRUNCATE TABLE SmallClusteredLowFrag

GO
INSERT INTO SmallClusteredLowFrag VALUES (DEFAULT,REPLICATE(N'a',100))

DECLARE @i int
SET @i = 0

WHILE @i < 16
BEGIN
	INSERT INTO SmallClusteredLowFrag(Payload) SELECT REPLICATE(N'a',100) FROM SmallClusteredLowFrag
	SET @i = @i + 1
END


INSERT INTO SmallClusteredLowFrag VALUES (NEWID(),REPLICATE(N'b',100))
GO	


TRUNCATE TABLE SmallClusteredAvgFrag

GO
INSERT INTO SmallClusteredAvgFrag VALUES (DEFAULT,REPLICATE(N'a',100))

DECLARE @i int
SET @i = 0

WHILE @i < 16
BEGIN
	INSERT INTO SmallClusteredAvgFrag(Payload) SELECT REPLICATE(N'a',100) FROM SmallClusteredAvgFrag
	SET @i = @i + 1
END

SET @i = 0

WHILE @i < 10 --Ten rows for fragmentation
BEGIN
	INSERT INTO SmallClusteredAvgFrag VALUES (NEWID(),REPLICATE(N'b',100))
	SET @i = @i + 1
END
GO	

TRUNCATE TABLE TextType
GO

INSERT INTO TextType VALUES (NEWID(),REPLICATE(N'a',100))

DECLARE @i int
SET @i = 0

WHILE @i < 16
BEGIN
	INSERT INTO TextType SELECT NEWID(),REPLICATE(N'a',100) FROM TextType
	SET @i = @i + 1
END
GO

TRUNCATE TABLE XMLType

GO

INSERT INTO XMLType VALUES (NEWID(),'<tag>'+cast(NEWID() as varchar(100))+'</tag>')

DECLARE @i int
SET @i = 0

WHILE @i < 16
BEGIN
	INSERT INTO XMLType SELECT NEWID(),'<tag>'+cast(NEWID() as varchar(100))+'</tag>' FROM XMLType
	SET @i = @i + 1
END
GO



TRUNCATE TABLE XMLAndImageType
GO

INSERT INTO XMLAndImageType VALUES (NEWID(), '<tag>'+cast(NEWID() as varchar(100))+'</tag>',0x74985743985643875643587436598743)

DECLARE @i int
SET @i = 0

WHILE @i < 16
BEGIN
	INSERT INTO XMLAndImageType SELECT NEWID(),'<tag>'+cast(NEWID() as varchar(100))+'</tag>',0x74985743985643875643587436598743 FROM XMLAndImageType
	SET @i = @i + 1
END
GO

TRUNCATE TABLE BigFragmented


INSERT INTO BigFragmented VALUES (NEWID(),DATEPART(ms,getdate()),getdate(),REPLICATE(N'a',9000),REPLICATE(N'b',5000),REPLICATE(N'b',5000))

DECLARE @i int
SET @i = 0

WHILE @i < 16
BEGIN
	INSERT INTO BigFragmented SELECT NEWID(),DATEPART(ms,getdate()),getdate(),REPLICATE(N'a',9000),REPLICATE(N'b',5000),REPLICATE(N'b',5000) FROM BigFragmented
	SET @i = @i + 1
END
GO




TRUNCATE TABLE PartitionedTable


DECLARE @i int

BEGIN TRAN

SET @i = 1 --Odd

WHILE @i < 10000
BEGIN
	INSERT INTO PartitionedTable VALUES(@i,DATEPART(ms,getdate()),getdate(),REPLICATE(N'a',1000))
	SET @i = @i + 2;
END

SET @i = 2 --Even

WHILE @i < 10000
BEGIN
	INSERT INTO PartitionedTable VALUES(@i,DATEPART(ms,getdate()),getdate(),REPLICATE(N'a',1000))
	SET @i = @i + 2;
END

COMMIT TRAN