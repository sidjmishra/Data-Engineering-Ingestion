"""
MongoDB Database Setup Script
Run this to initialize MongoDB database and collections
"""

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ServerSelectionTimeoutError
import json
import sys


def setup_mongodb(uri="mongodb://localhost:27017", database="file_ingestion"):
    """
    Setup MongoDB database with collections and indexes
    
    Args:
        uri: MongoDB connection URI
        database: Database name
    """
    try:
        print("Connecting to MongoDB...")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        client.admin.command('ping')
        print(f"Connected to MongoDB at {uri}")
        
        db = client[database]
        print(f"Using database: {database}")
        
        print("\nDropping existing collections...")
        if "file_metadata" in db.list_collection_names():
            db["file_metadata"].drop()
            print("Dropped: file_metadata")
        if "process_logs" in db.list_collection_names():
            db["process_logs"].drop()
            print("Dropped: process_logs")
        
        print("\nCreating file_metadata collection...")
        db.create_collection("file_metadata", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["file_name", "file_type", "file_hash", "status"],
                "properties": {
                    "_id": {"bsonType": "objectId"},
                    "file_name": {
                        "bsonType": "string",
                        "description": "Name of the file"
                    },
                    "file_type": {
                        "enum": ["csv", "image", "video"],
                        "description": "Type of file"
                    },
                    "source_path": {
                        "bsonType": "string",
                        "description": "Original source path"
                    },
                    "file_size": {
                        "bsonType": "int",
                        "description": "File size in bytes"
                    },
                    "file_hash": {
                        "bsonType": "string",
                        "description": "MD5 hash of file for deduplication"
                    },
                    "status": {
                        "enum": ["validated", "processing", "failed"],
                        "description": "Current status"
                    },
                    "ingested_at": {
                        "bsonType": "date",
                        "description": "When file was ingested"
                    },
                    "updated_at": {
                        "bsonType": "date",
                        "description": "When file was last updated"
                    },

                    "row_count": {"bsonType": "int"},
                    "column_count": {"bsonType": "int"},
                    "columns": {"bsonType": "array"},
                    "schema": {"bsonType": "object"},

                    "width": {"bsonType": "int"},
                    "height": {"bsonType": "int"},
                    "channels": {"bsonType": "int"},
                    "format": {"bsonType": "string"},
                    "mode": {"bsonType": "string"},
                    "size_mp": {"bsonType": "double"},
                    "exif": {"bsonType": "object"},

                    "duration_seconds": {"bsonType": "double"},
                    "duration_formatted": {"bsonType": "string"},
                    "fps": {"bsonType": "double"},
                    "resolution": {"bsonType": "string"},
                    "frame_count": {"bsonType": "int"},
                    "codec": {"bsonType": "string"}
                }
            }
        })
        print("Created collection: file_metadata")
        
        print("Creating process_logs collection...")
        db.create_collection("process_logs", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["file_name", "status", "timestamp"],
                "properties": {
                    "_id": {"bsonType": "objectId"},
                    "file_id": {"bsonType": "string"},
                    "file_name": {
                        "bsonType": "string",
                        "description": "Name of the file"
                    },
                    "file_type": {"bsonType": "string"},
                    "status": {
                        "bsonType": "string",
                        "description": "Process status"
                    },
                    "message": {
                        "bsonType": "string",
                        "description": "Log message"
                    },
                    "timestamp": {
                        "bsonType": "date",
                        "description": "When log was created"
                    },
                    "error_details": {"bsonType": "string"}
                }
            }
        })
        print("Created collection: process_logs")
        
        print("\nCreating indexes for file_metadata...")
        file_metadata = db["file_metadata"]
        
        file_metadata.create_index([("file_hash", ASCENDING)], unique=True, sparse=True)
        print("Index: file_hash (unique)")
        
        file_metadata.create_index([("file_name", ASCENDING)])
        print("Index: file_name")
        
        file_metadata.create_index([("file_type", ASCENDING)])
        print("Index: file_type")
        
        file_metadata.create_index([("ingested_at", ASCENDING)])
        print("Index: ingested_at")
        
        file_metadata.create_index([("status", ASCENDING)])
        print("Index: status")
        
        file_metadata.create_index([("source_path", ASCENDING)])
        print("Index: source_path")
        
        print("\nCreating indexes for process_logs...")
        process_logs = db["process_logs"]
        
        process_logs.create_index([("timestamp", ASCENDING)])
        print("Index: timestamp")
        
        process_logs.create_index([("file_id", ASCENDING)])
        print("Index: file_id")
        
        process_logs.create_index([("status", ASCENDING)])
        print("Index: status")
        
        process_logs.create_index([("file_name", ASCENDING)])
        print("Index: file_name")
        
        print("\n" + "=" * 60)
        print("DATABASE SETUP COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
        collections = db.list_collection_names()
        print(f"\nCollections created: {', '.join(collections)}")
        
        print("\nfile_metadata indexes:")
        for index in file_metadata.list_indexes():
            print(f"  - {index['name']}")
        
        print("\nprocess_logs indexes:")
        for index in process_logs.list_indexes():
            print(f"  - {index['name']}")
        
        print("\nMongoDB setup complete and ready to use!")
        
        client.close()
        return True
        
    except ServerSelectionTimeoutError:
        print("ERROR: Could not connect to MongoDB")
        print(f"URI: {uri}")
        print("Make sure MongoDB is running:")
        print("    Windows: Start-Service MongoDB")
        print("    Or: mongod.exe --dbpath <path>")
        return False
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Setup MongoDB database for file ingestion pipeline"
    )
    parser.add_argument(
        "--uri",
        default="mongodb://localhost:27017",
        help="MongoDB connection URI (default: mongodb://localhost:27017)"
    )
    parser.add_argument(
        "--database",
        default="file_ingestion",
        help="Database name (default: file_ingestion)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("MONGODB DATABASE SETUP")
    print("=" * 60)
    print()
    
    success = setup_mongodb(args.uri, args.database)
    
    sys.exit(0 if success else 1)
