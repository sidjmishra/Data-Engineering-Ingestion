-- SQL Server Database Setup Script
-- Execute in SQL Server Management Studio (SSMS) as an admin user

-- 1. Create database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'FileIngestion')
BEGIN
    CREATE DATABASE FileIngestion;
    PRINT 'Database FileIngestion created successfully';
END
GO

-- 2. Switch to the new database
USE FileIngestion;
GO

-- 3. Create file_metadata table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'file_metadata')
BEGIN
    CREATE TABLE file_metadata (
        id BIGINT PRIMARY KEY IDENTITY(1,1),
        file_name NVARCHAR(255) NOT NULL,
        file_type NVARCHAR(50) NOT NULL,
        source_path NVARCHAR(MAX),
        file_size BIGINT,
        file_hash NVARCHAR(64) UNIQUE,
        status NVARCHAR(50) DEFAULT 'validated',
        metadata_json NVARCHAR(MAX),
        ingested_at DATETIME2 DEFAULT GETUTCDATE(),
        updated_at DATETIME2,
        
        -- CSV fields
        row_count INT,
        column_count INT,
        
        -- Image fields
        width INT,
        height INT,
        channels INT,
        format NVARCHAR(50),
        
        -- Video fields
        duration_seconds FLOAT,
        fps FLOAT,
        frame_count INT,
        codec NVARCHAR(50),
        
        CHECK (file_type IN ('csv', 'image', 'video')),
        CHECK (status IN ('validated', 'processing', 'failed'))
    );
    
    PRINT 'Table file_metadata created successfully';
END
GO

-- 4. Create process_logs table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'process_logs')
BEGIN
    CREATE TABLE process_logs (
        id BIGINT PRIMARY KEY IDENTITY(1,1),
        file_id NVARCHAR(50),
        file_name NVARCHAR(255) NOT NULL,
        file_type NVARCHAR(50),
        status NVARCHAR(50) NOT NULL,
        message NVARCHAR(MAX),
        error_details NVARCHAR(MAX),
        timestamp DATETIME2 DEFAULT GETUTCDATE(),
        
        CHECK (status IN ('success', 'failed', 'duplicate_rejected', 'validation_failed', 'metadata_extraction_failed', 'database_insert_failed'))
    );
    
    PRINT 'Table process_logs created successfully';
END
GO

-- 5. Create indexes on file_metadata
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FileHash' AND object_id = OBJECT_ID('file_metadata'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX IX_FileHash ON file_metadata(file_hash);
    PRINT 'Index IX_FileHash created';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FileName' AND object_id = OBJECT_ID('file_metadata'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FileName ON file_metadata(file_name);
    PRINT 'Index IX_FileName created';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FileType' AND object_id = OBJECT_ID('file_metadata'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FileType ON file_metadata(file_type);
    PRINT 'Index IX_FileType created';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_IngestedAt' AND object_id = OBJECT_ID('file_metadata'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_IngestedAt ON file_metadata(ingested_at);
    PRINT 'Index IX_IngestedAt created';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Status' AND object_id = OBJECT_ID('file_metadata'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_Status ON file_metadata(status);
    PRINT 'Index IX_Status created';
END
GO

-- 6. Create indexes on process_logs
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LogTimestamp' AND object_id = OBJECT_ID('process_logs'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_LogTimestamp ON process_logs(timestamp);
    PRINT 'Index IX_LogTimestamp created';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LogStatus' AND object_id = OBJECT_ID('process_logs'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_LogStatus ON process_logs(status);
    PRINT 'Index IX_LogStatus created';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FileName' AND object_id = OBJECT_ID('process_logs'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_LogFileName ON process_logs(file_name);
    PRINT 'Index IX_LogFileName created';
END
GO

-- 7. Verify setup
PRINT '';
PRINT '=== DATABASE SETUP COMPLETED ===';
PRINT 'Tables created:';
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo';

PRINT '';
PRINT 'Indexes on file_metadata:';
SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('file_metadata') AND type > 0;

PRINT '';
PRINT 'Indexes on process_logs:';
SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('process_logs') AND type > 0;
