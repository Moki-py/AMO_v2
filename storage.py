"""
Storage module for handling MongoDB collections
"""

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from typing import Any
from datetime import datetime
import pymongo

import config
from logger import log_event


class Storage:
    """Storage manager for MongoDB collections"""

    def __init__(self):
        """Initialize the storage manager and MongoDB client"""
        self.client = MongoClient(config.settings.mongodb_uri)
        self.db = self.client[config.settings.mongodb_db]
        self._ensure_indexes()

        # Create a test log entry to verify logging is working
        try:
            log_collection = self.db['logs']
            test_log = {
                "timestamp": datetime.now().isoformat(),
                "component": "storage",
                "level": "info",
                "message": "Test log entry created during initialization"
            }
            log_collection.insert_one(test_log)
            print("Created test log entry in MongoDB")
        except Exception as e:
            print(f"Error creating test log entry: {e}")

    def _ensure_indexes(self):
        """Ensure indexes for entity collections (e.g., on 'id')"""
        for entity_type in [
            "leads", "deals", "contacts", "companies", "events", "logs"
        ]:
            collection_name = self._get_collection_name(entity_type)
            print(f"Creating index for collection: {collection_name}")
            self.db[collection_name].create_index(
                [("id", ASCENDING)], unique=True, sparse=True
            )
            if entity_type == "logs":
                self.db[collection_name].create_index(
                    [("timestamp", ASCENDING)]
                )

    def save_entities(self, entity_type: str, entities: list[dict[str, Any]]) -> bool:
        """Replace all entities of a type in the collection"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]

            # Instead of delete-then-insert, use ordered=False bulk write with upserts
            # This will handle duplicate keys gracefully
            if entities:
                # First, delete all existing documents
                collection.delete_many({})

                # Group entities in batches of 500 to avoid too large operations
                batch_size = 500
                for i in range(0, len(entities), batch_size):
                    batch = entities[i:i+batch_size]

                    # Use bulk write with ordered=False to continue on error
                    operations = []
                    for entity in batch:
                        if "id" in entity:
                            operations.append(
                                pymongo.UpdateOne(
                                    {"id": entity["id"]},
                                    {"$set": entity},
                                    upsert=True
                                )
                            )
                        else:
                            operations.append(pymongo.InsertOne(entity))

                    if operations:
                        collection.bulk_write(operations, ordered=False)

            log_event(
                "storage", "info",
                f"Saved {len(entities)} {entity_type} to MongoDB"
            )
            return True
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in save_entities: {e}"
            )
            return False

    def append_entities(self, entity_type: str, entities: list[dict[str, Any]]) -> bool:
        """Append entities to the collection (insert or update by id)"""
        if not entities:
            return True
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            for entity in entities:
                if "id" in entity:
                    collection.replace_one({"id": entity["id"]}, entity, upsert=True)
                else:
                    collection.insert_one(entity)
            log_event(
                "storage", "info",
                f"Appended {len(entities)} {entity_type} to MongoDB"
            )
            return True
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in append_entities: {e}"
            )
            return False

    def get_entities(self, entity_type: str) -> list[dict[str, Any]]:
        """Get all entities of a specific type from MongoDB"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            return list(
                collection.find({}, {"_id": 0})
            )
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in get_entities: {e}"
            )
            return []

    def update_entity(self, entity_type: str, entity_id: int, entity_data: dict[str, Any]) -> bool:
        """Update a specific entity or add it if it doesn't exist"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            result = collection.replace_one({"id": entity_id}, entity_data, upsert=True)
            if result.matched_count:
                log_event(
                    "storage", "info",
                    f"Updated {entity_type} with ID {entity_id}"
                )
            else:
                log_event(
                    "storage", "info",
                    f"Added new {entity_type} with ID {entity_id}"
                )
            return True
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in update_entity: {e}"
            )
            return False

    def delete_entity(self, entity_type: str, entity_id: int) -> bool:
        """Delete a specific entity by id"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            result = collection.delete_one({"id": entity_id})
            if result.deleted_count:
                log_event(
                    "storage", "info",
                    f"Deleted {entity_type} with ID {entity_id}"
                )
                return True
            else:
                log_event(
                    "storage", "warning",
                    f"No {entity_type} with ID {entity_id} found to delete"
                )
                return False
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in delete_entity: {e}"
            )
            return False

    def get_entity_count(self, entity_type: str) -> int:
        """Get the count of entities of a specific type"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            return collection.count_documents({})
        except PyMongoError as e:
            log_event("storage", "error", f"MongoDB error in get_entity_count: {e}")
            return 0

    def _get_collection_name(self, entity_type: str) -> str:
        """Map entity type to MongoDB collection name"""
        if entity_type == "leads" or entity_type == "deals":
            return "deals"
        elif entity_type == "contacts":
            return "contacts"
        elif entity_type == "companies":
            return "companies"
        elif entity_type == "events":
            return "events"
        elif entity_type == "logs":
            return "logs"
        else:
            return entity_type

    def add_log_entry(self, entry: dict[str, Any]) -> bool:
        """Add an entry to the logs collection"""
        try:
            collection = self.db[self._get_collection_name("logs")]
            if "timestamp" not in entry:
                entry["timestamp"] = datetime.now().isoformat()
            collection.insert_one(entry)
            self._clean_old_logs()
            return True
        except PyMongoError as e:
            # Don't use log_event to avoid circular reference
            print(f"ERROR: MongoDB error in add_log_entry: {e}")
            return False

    def _clean_old_logs(self):
        """Remove log entries older than the retention period"""
        try:
            retention_date = datetime.now().timestamp() - (
                config.settings.log_retention_days * 24 * 60 * 60
            )
            cutoff = datetime.fromtimestamp(retention_date).isoformat()
            collection = self.db[self._get_collection_name("logs")]
            collection.delete_many({"timestamp": {"$lt": cutoff}})
        except Exception as e:
            log_event("storage", "error", f"MongoDB error in _clean_old_logs: {e}")

    def get_statistics(self) -> dict[str, Any]:
        """Get statistics about stored data"""
        return {
            "deals": self.get_entity_count("leads"),
            "contacts": self.get_entity_count("contacts"),
            "companies": self.get_entity_count("companies"),
            "events": self.get_entity_count("events"),
            "logs": self.get_entity_count("logs"),
        }
