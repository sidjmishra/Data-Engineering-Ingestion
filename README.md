# File Ingestion Pipeline - Data Engineering

## Table of Contents
1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Prerequisites](#prerequisites)
4. [Installation Guide](#installation-guide)
5. [Configuration](#configuration)
6. [Running the Application](#running-the-application)
7. [Dashboard & Monitoring](#dashboard--monitoring)
8. [File Format Specifications](#file-format-specifications)
9. [Monitoring and Logging](#monitoring-and-logging)
10. [Function Reference](#function-reference)

---

## Overview

**File Ingestion Pipeline** is a modular, production-ready file ingestion service with an interactive dashboard that:

- **Monitors** a local folder for incoming files (CSV, Images, Videos)
- **Processes** files automatically every 1 hour on a scheduled basis
- **Extracts** comprehensive metadata from each file type
- **Validates** file integrity and metadata completeness
- **Stores** metadata in MongoDB or SQL Server
- **Organizes** files into structured directories based on type and status
- **Handles** failures gracefully with detailed logging
- **Deduplicates** files using MD5 hashing
- **Visualizes** pipeline metrics with an interactive Streamlit dashboard

### Key Features

- **Multi-Format Support**: CSV, Images (JPG, PNG, BMP, GIF, TIFF), Videos (MP4, AVI, MOV, MKV, etc.)
- **Automatic Scheduling**: Runs every 1 hour (configurable)
- **Dual Database Support**: MongoDB or SQL Server
- **Rich Metadata Extraction**: Schema, dimensions, codecs, EXIF data, etc.
- **Deduplication**: Prevents duplicate file ingestion
- **Error Recovery**: Failed files moved to dedicated folder with reason
- **Comprehensive Logging**: Detailed logs for auditing and debugging
- **Modular Design**: Easy to extend with new file types
- **Interactive Dashboard**: Real-time monitoring with Streamlit web interface
- **Advanced Analytics**: 7-day trends, file statistics, and performance metrics

---

## System Architecture

### High-Level Flow

```
Incoming Files (datasets/incoming)
        ↓
   File Detection
        ↓
   Validation (by type)
        ↓
   Metadata Extraction
        ↓
   Deduplication Check
        ↓
   Database Storage
        ↓
   File Organization
        ├─→ Validated Storage (datasets/validated)
        ├─→ Raw Storage (datasets/raw)
        └─→ Failed Storage (datasets/failed)
```

### Project Structure

```
Data Ingestion/
├── src/
│   ├── core/                    # Core ingestion logic
│   │   ├── processor.py        # Main file processor
│   │   ├── scheduler.py        # Job scheduler
│   │   ├── deduplication.py    # Deduplication service
│   │   └── __init__.py
│   ├── database/                # Database layer
│   │   ├── base.py             # Abstract interface
│   │   ├── mongodb.py          # MongoDB implementation
│   │   ├── sqlserver.py        # SQL Server implementation
│   │   ├── factory.py          # Factory pattern
│   │   └── __init__.py
│   ├── extractors/              # Metadata extraction
│   │   ├── csv_extractor.py    # CSV extractor
│   │   ├── image_extractor.py  # Image extractor
│   │   ├── video_extractor.py  # Video extractor
│   │   ├── factory.py          # Extractor factory
│   │   └── __init__.py
│   ├── utils/                   # Utility modules
│   │   ├── logger.py           # Logging configuration
│   │   ├── config.py           # Configuration management
│   │   ├── file_operations.py  # File operations
│   │   └── __init__.py
│   └── __init__.py
├── config/
│   └── config.yaml              # Main configuration file
├── db_scripts/
│   ├── mongodb_setup.py         # MongoDB setup script
│   └── sqlserver_setup.sql      # SQL Server setup script
├── datasets/
│   ├── incoming/                # Source files (MONITORED)
│   ├── raw/                     # Original files after processing
│   │   ├── structured/          # CSV files
│   │   ├── images/              # Image files
│   │   └── videos/              # Video files
│   ├── validated/               # Processed files
│   │   └── {YYYYMMDD_HHMM}/     # Batch directory
│   │       ├── structured/
│   │       ├── images/
│   │       └── videos/
│   └── failed/                  # Failed files
│       └── {YYYYMMDD_HHMM}/     # Batch directory
│           ├── structured/
│           ├── images/
│           └── videos/
├── logs/                        # Application logs
├── main.py                      # Pipeline entry point
├── dashboard.py                 # Streamlit dashboard app
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment template
│
├── README.md
└── generate_test_data.py        # Test data generator
```

---

## Prerequisites

### System Requirements
- **OS**: Windows 10/11 or Windows Server 2019+
- **Python**: 3.9 or higher
- **RAM**: Minimum 4GB (8GB recommended)
- **Disk Space**: Depends on file volume

### Required Software

1. **Python 3.9+**
   - Download from [python.org](https://www.python.org/downloads/)
   - Ensure "Add Python to PATH" is checked during installation

2. **MongoDB** (Option 1)
   - Download from [mongodb.com](https://www.mongodb.com/try/download/community)
   - Community Edition is sufficient
   - Default: `mongodb://localhost:27017`

3. **SQL Server** (Option 2)
   - Download from [microsoft.com](https://www.microsoft.com/sql-server/sql-server-2022)
   - SQL Server Express is free and sufficient
   - SQL Server Management Studio (SSMS) for administration

4. **Visual Studio Code** (Recommended)
   - For development and testing
   - Install Python extension for better experience

---

## Installation Guide

### Step 1: Clone/Download Project


### Step 2: Create Virtual Environment

```powershell
# Navigate to project directory
cd "Data Engineering\Data Ingestion"

# Create virtual environment
python -m venv .venv

# Activate virtual environment
.\.venv\Scripts\activate
```

### Step 3: Install Dependencies

```powershell
# Upgrade pip
python -m pip install --upgrade pip

# Install required packages
pip install -r requirements.txt
```

### Step 4: Copy Environment File

```powershell
# Copy example to actual
.env.example -> .env

# Edit .env with your credentials
Update `.env` with your database credentials.
```

---

## Configuration

### Main Configuration File: `config/config.yaml`

Edit `config/config.yaml` to customize:

```yaml
# Folder Paths - AUTOMATICALLY SET
folders:
  incoming: "${BASE_PATH}/datasets/incoming"
  raw: "${BASE_PATH}/datasets/raw"
  validated: "${BASE_PATH}/datasets/validated"
  failed: "${BASE_PATH}/datasets/failed"
  logs: "${BASE_PATH}/logs"

# Database Selection
database:
  type: "mongodb"  # or "sqlserver"
  
  # MongoDB Configuration
  mongodb:
    uri: "mongodb://localhost:27017"
    database: "file_ingestion"
  
  # SQL Server Configuration
  sqlserver:
    server: "localhost"
    database: "FileIngestion"
    username: "sa"
    password: "YourPassword123!"

# Scheduler Settings
scheduler:
  interval_minutes: 60  # Run every hour

# File Type Extensions
processing:
  csv_extensions: ['.csv']
  image_extensions: ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff']
  video_extensions: ['.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.webm']

# Logging
logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR
  console_output: true
  file_output: true
```

---

## Running the Application

### Quick Start

```powershell
# 1. Navigate to project
cd "Data Engineering\Data Ingestion"

# 2. Activate virtual environment
.\.venv\Scripts\activate

# 3. Setup the database (MongoDB)
python mongodb_setup.py

# 4. Run application
python main.py
```

### Expected Output

```
======================================================================
FILE INGESTION PIPELINE - APPLICATION STARTUP
======================================================================
Base Path: Data Engineering\Data Ingestion
Config Path: Data Engineering\Data Ingestion\config\config.yaml
Connected to database...
Database connected successfully
Verifying folder structure...
  incoming: Data Engineering\Data Ingestion\datasets\incoming
  raw: Data Engineering\Data Ingestion\datasets\raw
  validated: Data Engineering\Data Ingestion\datasets\validated
  failed: Data Engineering\Data Ingestion\datasets\failed

Starting file ingestion scheduler...
======================================================================
APPLICATION READY
======================================================================
Scheduler is running. Press Ctrl+C to stop.
======================================================================
```

### Testing the Pipeline

1. **Prepare Test Files**

```
# Copy test files to incoming folder
"path\to\test.csv" -> "datasets\incoming\"
"path\to\image.jpg" -> "datasets\incoming\"
"path\to\video.mp4" -> "datasets\incoming\"
```

2. **Wait or Trigger Manual Run**

The scheduler will automatically run every hour, but you can also trigger manually by waiting for the next `:00` minute mark.

3. **Monitor Logs**
4. **Verify in Database**

---

## Dashboard & Monitoring

### Streamlit Dashboard

A fully-featured web dashboard for monitoring, analyzing, and visualizing the file ingestion pipeline in real-time.

#### Quick Start
```powershell
# Ensure virtual environment is activated
.\.venv\Scripts\activate

# Start dashboard
streamlit run dashboard.py
```

The dashboard automatically opens in your default browser at `http://localhost:8501`

#### Dashboard Features

**6 Interactive Tabs:**

1. **📊 Dashboard (Home)**
   - Real-time KPI metrics (Validated, Raw, Failed, Total files)
   - File distribution charts (pie & bar)
   - Recent activity log (last 20 entries)
   - **Best for:** Daily health check, quick overview

2. **📝 Logs**
   - Searchable log table
   - Date range filter (1-30 days)
   - Log level filter (INFO, DEBUG, WARNING, ERROR)
   - Full-text search across logs
   - CSV export button
   - **Best for:** Debugging, error investigation

3. **📁 Files**
   - Browse files in different folders (Validated, Raw, Failed, Incoming)
   - File statistics (count, total size, average size)
   - Detailed file listing with size and modified date
   - Type distribution charts
   - **Best for:** File exploration, status monitoring

4. **💾 Database**
   - Three sub-tabs: Statistics, File Metadata, Process Logs
   - View all indexed files with metadata
   - Query processing history
   - Record count statistics
   - **Best for:** Data verification, audit trail

5. **📈 Analytics**
   - 7-day activity timeline
   - Log entries by level (INFO, WARNING, ERROR, DEBUG)
   - Trend analysis and peak time identification
   - Error pattern detection
   - **Best for:** Performance analysis, trend spotting

6. **⚙️ Settings**
   - Database configuration display
   - Scheduler settings
   - Logging configuration
   - Full config.yaml viewer
   - **Best for:** Configuration verification

#### Running Both Applications Together

You can run the pipeline and dashboard simultaneously in different terminals:

**Terminal 1: Pipeline**
```powershell
cd "Data Engineering\Data Ingestion"
.\.venv\Scripts\activate
python main.py
```

**Terminal 2: Dashboard**
```powershell
cd "Data Engineering\Data Ingestion"
streamlit run dashboard.py
```

Now you can monitor real-time processing in the dashboard as files are ingested!

---

## File Format Specifications

### CSV Files

**Supported Extensions**: `.csv`

**Extracted Metadata**:
- Row count
- Column count
- Column names and types
- Nullable flags
- Unique value counts
- File size and hash

**Validation Rules**:
- Must be valid CSV format
- Cannot be empty
- UTF-8 encoding supported

**Example CSV**:
```csv
Name,Age,City,Salary
John,28,New York,75000
Jane,34,Boston,85000
```

### Image Files

**Supported Extensions**: `.jpg`, `.jpeg`, `.png`, `.bmp`, `.gif`, `.tiff`

**Extracted Metadata**:
- Dimensions (width × height)
- Color mode (RGB, RGBA, L, etc.)
- Number of channels
- Format
- Size in megapixels
- File size and hash
- EXIF data (if available)

**Validation Rules**:
- Must be valid image format
- Non-zero dimensions
- Readable by Pillow library

**Example Metadata**:
```json
{
  "width": 1920,
  "height": 1080,
  "channels": 3,
  "format": "JPEG",
  "size_mp": 2.07
}
```

### Video Files

**Supported Extensions**: `.mp4`, `.avi`, `.mov`, `.mkv`, `.flv`, `.wmv`, `.webm`

**Extracted Metadata**:
- Duration (seconds and formatted HH:MM:SS)
- Frame rate (FPS)
- Resolution (width × height)
- Total frame count
- Codec
- File size and hash

**Validation Rules**:
- Must be valid video format
- Must have readable frames
- Must have valid FPS

**Example Metadata**:
```json
{
  "duration_seconds": 120.5,
  "duration_formatted": "00:02:00",
  "fps": 30,
  "width": 1920,
  "height": 1080,
  "resolution": "1920x1080",
  "frame_count": 3615,
  "codec": "h264"
}
```

---

## Monitoring and Logging

### Dashboard Monitoring

The **Streamlit Dashboard** is the recommended way to monitor the pipeline:

- **Real-time metrics** in the Dashboard tab
- **Log search and filtering** in the Logs tab
- **File statistics** in the Files tab
- **Database records** in the Database tab
- **Performance analytics** in the Analytics tab

See **Dashboard & Monitoring** section above for detailed instructions.

### Log Files

Logs are saved in: `logs/ingestion_YYYYMMDD.log`

Each day gets a new log file with rotation.

### Log Levels

- **DEBUG**: Detailed diagnostic information
- **INFO**: General informational messages
- **WARNING**: Warning messages (like duplicates)
- **ERROR**: Error messages (failures, exceptions)
- **CRITICAL**: Critical system errors

### Sample Log Output

```
2024-01-15 10:00:00,123 - FileIngestionPipeline - INFO - ============================================================
2024-01-15 10:00:00,124 - FileIngestionPipeline - INFO - Starting file ingestion cycle at 2024-01-15T10:00:00
2024-01-15 10:00:00,125 - FileIngestionPipeline - INFO - Found 3 file(s) to process
2024-01-15 10:00:01,234 - FileIngestionPipeline - INFO - SUCCESS: datasets/incoming/sales_data.csv
2024-01-15 10:00:05,456 - FileIngestionPipeline - INFO - SUCCESS: datasets/incoming/photo.jpg
2024-01-15 10:00:10,789 - FileIngestionPipeline - WARNING - ⊗ DUPLICATE: datasets/incoming/old_video.mp4
2024-01-15 10:00:10,790 - FileIngestionPipeline - INFO - Ingestion Cycle Statistics:
2024-01-15 10:00:10,791 - FileIngestionPipeline - INFO -   Successfully Processed: 2
2024-01-15 10:00:10,792 - FileIngestionPipeline - INFO -   Failed: 0
2024-01-15 10:00:10,793 - FileIngestionPipeline - INFO -   Duplicates Detected: 1
```

---

## API Reference

### Main Entry Point: `main.py`

```python
# Start application
python main.py

# Runs continuously until Ctrl+C
# Automatically ingest files every hour
# Provides real-time logging output
```

### Database Interface

All database operations go through `DatabaseFactory`:

```python
from src.database.factory import DatabaseFactory

# Get database handler
db = DatabaseFactory.create_handler()

# Insert metadata
metadata_id = db.insert_metadata({
    'file_name': 'example.csv',
    'file_type': 'csv',
    ...
})

# Query by hash (deduplication)
result = db.find_by_hash('abc123...hash...')

# Update metadata
db.update_metadata(metadata_id, {'status': 'processed'})

# Get metadata
metadata = db.get_metadata(metadata_id)

# Log process result
db.insert_process_log({
    'file_name': 'example.csv',
    'status': 'success',
    'message': '...'
})
```

### Metadata Extraction

```python
from src.extractors.factory import MetadataExtractorFactory

# Extract metadata
metadata, success, error = MetadataExtractorFactory.extract_metadata(
    file_path='path/to/file.csv'
)

# Validate file
is_valid, error_msg = MetadataExtractorFactory.validate_file(
    file_path='path/to/file.csv',
    file_type='csv'  # optional
)
```

### File Operations

```python
from src.utils.file_operations import FileOperations

# Detect file type
file_type = FileOperations.detect_file_type('file.mp4')  # Returns 'video'

# Compute hash
hash_value = FileOperations.compute_file_hash('file.csv')

# Get file size
size = FileOperations.get_file_size('file.csv')  # Returns bytes

# Move file
FileOperations.move_file('from/path', 'to/path')

# List files
files = FileOperations.list_files('directory/', ['.csv', '.jpg'])
```

---

## Advanced Configuration

### Change Ingestion Interval

Edit `config/config.yaml`:
```yaml
scheduler:
  interval_minutes: 30  # Run every 30 minutes instead of 60
```

### Add New File Type

1. Create extractor: `src/extractors/your_extractor.py`
2. Add to factory: `MetadataExtractorFactory.EXTRACTORS`
3. Update config: `config/config.yaml`

---

## License

This project is provided as-is for local use.

---

**Last Updated**: February 2026  
**Version**: 1.0.0  
**Dashboard Version**: 1.0  
