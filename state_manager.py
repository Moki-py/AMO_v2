"""
State manager for tracking export progress using MongoDB
"""

from datetime import datetime
from typing import Any, List

import config
from logger import log_event
from pymongo import MongoClient
from pymongo.errors import PyMongoError


class StateManager:
    """Manages the state of exports to enable resume functionality using MongoDB"""

    def __init__(self):
        """Initialize the state manager with MongoDB connection"""
        # Keep state_file for backward compatibility
        self.state_file = config.settings.state_file

        # Connect to MongoDB
        self.client = MongoClient(config.settings.mongodb_uri)
        self.db = self.client[config.settings.mongodb_db]
        self.state_collection = self.db['export_state']

        # Initialize state if not exists in MongoDB
        self._ensure_state()

        # Load state from MongoDB
        self.state = self._load_state()

        # Don't automatically mark exports as stopped on init,
        # let the exporter check and decide which ones to continue

    def _ensure_state(self):
        """Ensure state document exists in MongoDB"""
        # Check if state document exists
        if self.state_collection.count_documents({"_id": "state"}) == 0:
            # Create default state document
            default_state = {
                "_id": "state",
                "leads": {"last_page": 0, "completed": False, "last_update": None},
                "contacts": {
                    "last_page": 0,
                    "completed": False,
                    "last_update": None,
                },
                "companies": {
                    "last_page": 0,
                    "completed": False,
                    "last_update": None,
                },
                "events": {
                    "last_page": 0,
                    "completed": False,
                    "last_update": None,
                },
                "global": {"last_full_sync": None, "running_exports": []},
            }

            # Insert the default state
            try:
                self.state_collection.insert_one(default_state)
                log_event("state", "info", "Initialized default state in MongoDB")
            except PyMongoError as e:
                log_event("state", "error", f"Error initializing MongoDB state: {e}")

    def _load_state(self) -> dict[str, Any]:
        """Load the export state from MongoDB"""
        try:
            state_doc = self.state_collection.find_one({"_id": "state"})
            if state_doc:
                # Remove MongoDB _id field before returning
                if "_id" in state_doc:
                    del state_doc["_id"]
                return state_doc
        except PyMongoError as e:
            log_event("state", "error", f"Error loading state from MongoDB: {e}")

        # Return default state if MongoDB operation failed
        return {
            "leads": {"last_page": 0, "completed": False, "last_update": None},
            "contacts": {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            },
            "companies": {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            },
            "events": {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            },
            "global": {"last_full_sync": None, "running_exports": []},
        }

    def save_state(self):
        """Save the current state to MongoDB"""
        try:
            # Create a copy of state with _id for MongoDB
            state_to_save = self.state.copy()
            state_to_save["_id"] = "state"

            # Use replace_one with upsert to update or create the document
            self.state_collection.replace_one(
                {"_id": "state"}, state_to_save, upsert=True
            )
        except PyMongoError as e:
            log_event("state", "error", f"Error saving state to MongoDB: {e}")

    def update_export_progress(
        self, entity_type: str, page: int, completed: bool = False
    ):
        """Update the progress of an export"""
        if entity_type not in self.state:
            self.state[entity_type] = {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            }

        self.state[entity_type]["last_page"] = page
        self.state[entity_type]["completed"] = completed
        self.state[entity_type]["last_update"] = datetime.now().isoformat()

        self.save_state()

    def get_last_page(self, entity_type: str) -> int:
        """Get the last processed page for an entity type"""
        if entity_type in self.state:
            return self.state[entity_type]["last_page"]
        return 0

    def is_export_completed(self, entity_type: str) -> bool:
        """Check if an export is completed"""
        if entity_type in self.state:
            return self.state[entity_type]["completed"]
        return False

    def reset_export_state(self, entity_type: str):
        """Reset the state for an entity type"""
        if entity_type in self.state:
            self.state[entity_type] = {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            }
            self.save_state()

    def mark_export_running(self, entity_type: str):
        """Mark an export as currently running"""
        if "global" not in self.state:
            self.state["global"] = {"running_exports": []}

        if "running_exports" not in self.state["global"]:
            self.state["global"]["running_exports"] = []

        if entity_type not in self.state["global"]["running_exports"]:
            self.state["global"]["running_exports"].append(entity_type)
            self.save_state()

    def mark_export_stopped(self, entity_type: str):
        """Mark an export as stopped"""
        if (
            "global" in self.state
            and "running_exports" in self.state["global"]
            and entity_type in self.state["global"]["running_exports"]
        ):
            self.state["global"]["running_exports"].remove(entity_type)
            self.save_state()

    def get_running_exports(self) -> List[str]:
        """Get list of currently running exports"""
        if (
            "global" in self.state
            and "running_exports" in self.state["global"]
        ):
            return self.state["global"]["running_exports"]
        return []

    def is_export_running(self, entity_type: str) -> bool:
        """Check if an export is currently running"""
        running_exports = self.get_running_exports()
        return entity_type in running_exports

    def clear_running_exports(self):
        """Clear all running exports, useful when restarting the server"""
        if "global" in self.state:
            self.state["global"]["running_exports"] = []
            self.save_state()
            log_event("state", "info", "Cleared all running exports from state")

    def reset_all_state(self):
        """Reset all export state, including running exports"""
        # Set default state for all entity types
        for entity_type in ["leads", "contacts", "companies", "events"]:
            self.state[entity_type] = {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            }

        # Clear running exports
        if "global" in self.state:
            self.state["global"]["running_exports"] = []
            self.state["global"]["last_full_sync"] = None

        self.save_state()
        log_event("state", "info", "Reset all export state to default")

    def verify_running_exports(self, valid_exports: List[str]) -> List[str]:
        """
        Verify running exports, cleaning any that are not in the valid_exports list.
        Used during initialization to make sure we don't have orphaned running exports.
        Returns list of actually valid running exports.
        """
        running_exports = self.get_running_exports()
        valid_running = []

        for export in running_exports:
            if export in valid_exports:
                valid_running.append(export)
            else:
                # This export was marked as running but isn't in the valid list
                # It might be from a crashed session, so mark it as stopped
                self.mark_export_stopped(export)
                log_event(
                    "state", "warning",
                    f"Export {export} was marked as running but is not valid - marked as stopped"
                )

        return valid_running

    def is_export_running_in_db(self, entity_type: str) -> bool:
        """Check if an export is actually running in the database, not just in memory"""
        try:
            # Query MongoDB directly to get the most up-to-date state
            result = self.state_collection.find_one(
                {"_id": "state"},
                {"global.running_exports": 1}
            )

            if (result and
                "global" in result and
                "running_exports" in result["global"]):
                running_exports = result["global"]["running_exports"]
                is_running = entity_type in running_exports

                # Log for debugging
                if is_running:
                    log_event(
                        "state", "debug",
                        f"MongoDB shows {entity_type} is running"
                    )

                return is_running

            return False
        except PyMongoError as e:
            log_event(
                "state", "error",
                f"Error checking if {entity_type} is running in DB: {e}"
            )
            # Fall back to in-memory state
            return self.is_export_running(entity_type)
