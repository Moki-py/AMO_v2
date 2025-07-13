"""
State manager for tracking export progress using MongoDB
"""

from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional
import json
import traceback

from ..core import config
from ..core.logger import log_event
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import pymongo


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

        # Initialize task collection
        self.task_collection = self.db['export_tasks']

        # Initialize exported IDs collection for tracking duplicates
        self.exported_ids_collection = self.db['exported_ids']
        self._ensure_exported_ids_indexes()

        # Initialize backup collection for state recovery
        self.backup_collection = self.db['state_backups']
        self._ensure_backup_indexes()

        # Initialize heartbeat collection for tracking active exports
        self.heartbeat_collection = self.db['export_heartbeats']
        self._ensure_heartbeat_indexes()

        # Initialize state if not exists in MongoDB
        self._ensure_state()

        # Load state from MongoDB
        self.state = self._load_state()

        # Validate state integrity on startup
        validation_result = self.validate_state_integrity()
        if not validation_result["valid"]:
            log_event("state", "error", f"State validation failed: {validation_result['errors']}")
            # Create backup before attempting to fix
            self.create_backup("corrupted_state", {"validation_errors": validation_result["errors"]})
            # Try to restore from latest checkpoint
            if not self.restore_from_backup(backup_type="checkpoint"):
                log_event("state", "warning", "Could not restore from checkpoint, using current state")
        elif validation_result["warnings"]:
            log_event("state", "warning", f"State validation warnings: {validation_result['warnings']}")

        # Don't automatically mark exports as stopped on init,
        # let the exporter check and decide which ones to continue

        # Track last checkpoint time for time-based saves
        self.last_checkpoint_time = datetime.now()

    def _ensure_exported_ids_indexes(self):
        """Ensure indexes exist for exported_ids collection"""
        try:
            # Create index on entity_type for fast lookups
            self.exported_ids_collection.create_index([("entity_type", 1)])
            # Create index on exported_ids for fast membership checks
            self.exported_ids_collection.create_index([("exported_ids", 1)])
            # Create compound index for efficient queries
            self.exported_ids_collection.create_index([("entity_type", 1), ("exported_ids", 1)])
            log_event("state", "info", "Created indexes for exported_ids collection")
        except PyMongoError as e:
            log_event("state", "error", f"Error creating exported_ids indexes: {e}")

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
                "users": {
                    "last_page": 0,
                    "completed": False,
                    "last_update": None,
                },
                "pipelines": {
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
            "users": {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            },
            "pipelines": {
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

    def _check_transactions_support(self) -> bool:
        """Check if MongoDB instance supports transactions"""
        try:
            # Check if this is a replica set or sharded cluster
            is_master = self.client.admin.command("ismaster")

            # Transactions are supported on replica sets and sharded clusters
            if is_master.get("setName") or is_master.get("msg") == "isdbgrid":
                return True

            # For standalone instances, transactions are not supported
            return False
        except Exception as e:
            log_event("state", "debug", f"Error checking transaction support: {e}")
            return False

    def save_state_atomic(self, exported_ids_updates: Optional[Dict[str, List[int]]] = None):
        """
        Atomically save state and exported IDs using MongoDB transactions when supported

        Args:
            exported_ids_updates: Dict mapping entity_type to list of new exported IDs
        """
        # Check if transactions are supported
        if not self._check_transactions_support():
            log_event("state", "debug", "Transactions not supported, using regular save operations")
            return self._save_state_non_atomic(exported_ids_updates)

        # Try to use transactions
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    # Save state
                    state_to_save = self.state.copy()
                    state_to_save["_id"] = "state"
                    state_to_save["last_atomic_update"] = datetime.now().isoformat()

                    self.state_collection.replace_one(
                        {"_id": "state"}, state_to_save, upsert=True, session=session
                    )

                    # Update exported IDs if provided
                    if exported_ids_updates:
                        for entity_type, entity_ids in exported_ids_updates.items():
                            if entity_ids:
                                entity_type = self._normalize_entity_type(entity_type)
                                self.exported_ids_collection.update_one(
                                    {"entity_type": entity_type},
                                    {"$addToSet": {"exported_ids": {"$each": entity_ids}}},
                                    upsert=True,
                                    session=session
                                )

                    # Transaction will be committed automatically
                    log_event("state", "debug", "Atomic state save completed successfully")
                    return True

        except PyMongoError as e:
            # Fall back to non-atomic save if transactions fail
            log_event("state", "warning", f"Atomic save failed, falling back to regular save: {e}")
            return self._save_state_non_atomic(exported_ids_updates)

    def _save_state_non_atomic(self, exported_ids_updates: Optional[Dict[str, List[int]]] = None) -> bool:
        """
        Save state and exported IDs without transactions (fallback method)

        Args:
            exported_ids_updates: Dict mapping entity_type to list of new exported IDs
        """
        try:
            # Save state
            self.save_state()

            # Update exported IDs separately
            if exported_ids_updates:
                for entity_type, entity_ids in exported_ids_updates.items():
                    if entity_ids:
                        self.add_exported_ids(entity_type, entity_ids)

            log_event("state", "debug", "Non-atomic state save completed successfully")
            return True

        except Exception as e:
            log_event("state", "error", f"Non-atomic save failed: {e}")
            return False

    def create_backup(self, backup_type: str = "checkpoint", metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create a backup of the current state

        Args:
            backup_type: Type of backup (checkpoint, manual, pre_operation)
            metadata: Additional metadata to store with backup
        """
        try:
            backup_data = {
                "backup_type": backup_type,
                "timestamp": datetime.now().isoformat(),
                "state": self.state.copy(),
                "metadata": metadata or {},
                "version": 1
            }

            # Add exported IDs snapshot to backup
            exported_ids_snapshot = {}
            for entity_type in ["leads", "contacts", "companies", "events", "users", "pipelines"]:
                exported_ids_snapshot[entity_type] = self.get_exported_ids(entity_type)

            backup_data["exported_ids"] = exported_ids_snapshot

            # Insert backup
            result = self.backup_collection.insert_one(backup_data)
            backup_id = str(result.inserted_id)

            log_event("state", "info", f"Created {backup_type} backup with ID: {backup_id}")

            # Clean old backups to prevent storage bloat
            self._cleanup_old_backups()

            return True

        except PyMongoError as e:
            log_event("state", "error", f"Error creating backup: {e}")
            return False

    def restore_from_backup(self, backup_id: Optional[str] = None, backup_type: Optional[str] = None) -> bool:
        """
        Restore state from a backup

        Args:
            backup_id: Specific backup ID to restore from
            backup_type: Type of backup to restore (uses latest if backup_id not provided)
        """
        try:
            # Find backup to restore
            if backup_id:
                # Convert string ID to ObjectId
                from bson import ObjectId
                backup = self.backup_collection.find_one({"_id": ObjectId(backup_id)})
            else:
                # Find latest backup of specified type
                query = {"backup_type": backup_type} if backup_type else {}
                backup = self.backup_collection.find_one(query, sort=[("timestamp", -1)])

            if not backup:
                log_event("state", "error", f"No backup found for restore (ID: {backup_id}, type: {backup_type})")
                return False

            # Create current state backup before restore
            self.create_backup("pre_restore", {"restored_from": str(backup["_id"])})

            # Restore state
            self.state = backup["state"].copy()
            self.save_state()

            # Restore exported IDs if available
            if "exported_ids" in backup:
                for entity_type, entity_ids in backup["exported_ids"].items():
                    if entity_ids:
                        # Clear existing exported IDs
                        self.clear_exported_ids(entity_type)
                        # Add restored IDs
                        self.add_exported_ids(entity_type, entity_ids)

            log_event("state", "info", f"Successfully restored state from backup: {backup['_id']}")
            return True

        except Exception as e:
            log_event("state", "error", f"Error restoring from backup: {e}")
            return False

    def validate_state_integrity(self) -> Dict[str, Any]:
        """
        Validate the integrity of the current state

        Returns:
            Dict with validation results
        """
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "checked_at": datetime.now().isoformat()
        }

        try:
            # Check state structure
            required_fields = ["leads", "contacts", "companies", "events", "users", "pipelines", "global"]
            for field in required_fields:
                if field not in self.state:
                    validation_result["errors"].append(f"Missing required field: {field}")
                    validation_result["valid"] = False
                elif field != "global":
                    # Check entity state structure
                    entity_state = self.state[field]
                    if not isinstance(entity_state, dict):
                        validation_result["errors"].append(f"Invalid type for {field}: expected dict")
                        validation_result["valid"] = False
                    else:
                        required_entity_fields = ["last_page", "completed", "last_update"]
                        for entity_field in required_entity_fields:
                            if entity_field not in entity_state:
                                validation_result["errors"].append(f"Missing field {entity_field} in {field}")
                                validation_result["valid"] = False

            # Check global state
            if "global" in self.state:
                global_state = self.state["global"]
                if "running_exports" not in global_state:
                    validation_result["warnings"].append("Missing running_exports in global state")
                elif not isinstance(global_state["running_exports"], list):
                    validation_result["errors"].append("running_exports should be a list")
                    validation_result["valid"] = False

            # Check for orphaned running exports
            running_exports = self.get_running_exports()
            valid_entity_types = ["leads", "contacts", "companies", "events", "users", "pipelines"]
            for export in running_exports:
                if export not in valid_entity_types:
                    validation_result["warnings"].append(f"Unknown entity type in running exports: {export}")

            # Check page numbers consistency
            for entity_type in ["leads", "contacts", "companies", "events", "users", "pipelines"]:
                if entity_type in self.state:
                    last_page = self.state[entity_type].get("last_page", 0)
                    if last_page < 0:
                        validation_result["errors"].append(f"Invalid last_page for {entity_type}: {last_page}")
                        validation_result["valid"] = False

            # Check exported IDs consistency
            for entity_type in ["leads", "contacts", "companies", "events", "users", "pipelines"]:
                try:
                    exported_count = self.get_exported_ids_count(entity_type)
                    if exported_count < 0:
                        validation_result["warnings"].append(f"Negative exported IDs count for {entity_type}: {exported_count}")
                except Exception as e:
                    validation_result["warnings"].append(f"Error checking exported IDs for {entity_type}: {e}")

        except Exception as e:
            validation_result["valid"] = False
            validation_result["errors"].append(f"Validation error: {e}")

        return validation_result

    def should_create_checkpoint(self) -> bool:
        """
        Check if it's time to create a time-based checkpoint

        Returns:
            True if checkpoint should be created
        """
        time_since_last = datetime.now() - self.last_checkpoint_time
        return time_since_last >= timedelta(minutes=5)

    def create_checkpoint_if_needed(self, force: bool = False) -> bool:
        """
        Create a checkpoint if needed based on time or force flag

        Args:
            force: Force checkpoint creation regardless of time
        """
        if force or self.should_create_checkpoint():
            success = self.create_backup("checkpoint", {
                "auto_created": True,
                "running_exports": self.get_running_exports()
            })
            if success:
                self.last_checkpoint_time = datetime.now()
            return success
        return False

    def _cleanup_old_backups(self, max_backups: int = 50, max_age_days: int = 30):
        """
        Clean up old backups to prevent storage bloat

        Args:
            max_backups: Maximum number of backups to keep per type
            max_age_days: Maximum age of backups in days
        """
        try:
            # Clean by age
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
            age_result = self.backup_collection.delete_many({
                "timestamp": {"$lt": cutoff_date.isoformat()}
            })

            # Clean by count for each backup type
            backup_types = ["checkpoint", "manual", "pre_operation", "pre_restore"]
            count_deleted = 0

            for backup_type in backup_types:
                # Get backups sorted by timestamp (newest first)
                backups = list(self.backup_collection.find(
                    {"backup_type": backup_type}
                ).sort("timestamp", -1))

                # Delete excess backups
                if len(backups) > max_backups:
                    excess_backups = backups[max_backups:]
                    for backup in excess_backups:
                        self.backup_collection.delete_one({"_id": backup["_id"]})
                        count_deleted += 1

            total_deleted = age_result.deleted_count + count_deleted
            if total_deleted > 0:
                log_event("state", "info", f"Cleaned up {total_deleted} old backups")

        except PyMongoError as e:
            log_event("state", "error", f"Error cleaning up old backups: {e}")

    def get_backup_list(self, backup_type: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get list of available backups

        Args:
            backup_type: Filter by backup type
            limit: Maximum number of backups to return
        """
        try:
            query = {"backup_type": backup_type} if backup_type else {}
            backups = list(self.backup_collection.find(
                query,
                {"state": 0, "exported_ids": 0}  # Exclude large fields
            ).sort("timestamp", -1).limit(limit))

            # Convert ObjectId to string for JSON serialization
            for backup in backups:
                backup["_id"] = str(backup["_id"])

            return backups

        except PyMongoError as e:
            log_event("state", "error", f"Error getting backup list: {e}")
            return []

    def rollback_to_checkpoint(self) -> bool:
        """
        Rollback to the latest checkpoint backup
        """
        return self.restore_from_backup(backup_type="checkpoint")

    def create_recovery_point(self, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create a recovery point before critical operations

        Args:
            metadata: Additional metadata about the operation
        """
        return self.create_backup("recovery_point", metadata)

    def emergency_recovery(self) -> bool:
        """
        Emergency recovery procedure - try multiple recovery strategies

        Returns:
            True if recovery was successful
        """
        log_event("state", "warning", "Starting emergency recovery procedure")

        # Strategy 1: Try to restore from latest checkpoint
        if self.restore_from_backup(backup_type="checkpoint"):
            log_event("state", "info", "Emergency recovery: restored from checkpoint")
            return True

        # Strategy 2: Try to restore from latest manual backup
        if self.restore_from_backup(backup_type="manual"):
            log_event("state", "info", "Emergency recovery: restored from manual backup")
            return True

        # Strategy 3: Try to restore from any recent backup
        recent_backups = self.get_backup_list(limit=5)
        for backup in recent_backups:
            if self.restore_from_backup(backup_id=backup["_id"]):
                log_event("state", "info", f"Emergency recovery: restored from backup {backup['_id']}")
                return True

        # Strategy 4: Reset to default state as last resort
        log_event("state", "error", "Emergency recovery: all backup restore attempts failed, resetting to default state")
        self.create_backup("pre_reset", {"reason": "emergency_recovery_fallback"})
        self.reset_all_state()
        return False

    def repair_state(self) -> bool:
        """
        Attempt to repair corrupted state data

        Returns:
            True if repair was successful
        """
        log_event("state", "info", "Starting state repair procedure")

        # Create backup before repair
        self.create_backup("pre_repair", {"reason": "state_repair"})

        try:
            # Repair missing entity states
            entity_types = ["leads", "contacts", "companies", "events", "users", "pipelines"]
            for entity_type in entity_types:
                if entity_type not in self.state:
                    self.state[entity_type] = {
                        "last_page": 0,
                        "completed": False,
                        "last_update": None,
                    }
                    log_event("state", "info", f"Repaired missing entity state: {entity_type}")
                else:
                    # Repair missing fields in entity state
                    entity_state = self.state[entity_type]
                    if "last_page" not in entity_state:
                        entity_state["last_page"] = 0
                    if "completed" not in entity_state:
                        entity_state["completed"] = False
                    if "last_update" not in entity_state:
                        entity_state["last_update"] = None

                    # Fix invalid values
                    if not isinstance(entity_state["last_page"], int) or entity_state["last_page"] < 0:
                        entity_state["last_page"] = 0
                    if not isinstance(entity_state["completed"], bool):
                        entity_state["completed"] = False

            # Repair global state
            if "global" not in self.state:
                self.state["global"] = {"running_exports": [], "last_full_sync": None}
            else:
                global_state = self.state["global"]
                if "running_exports" not in global_state:
                    global_state["running_exports"] = []
                elif not isinstance(global_state["running_exports"], list):
                    global_state["running_exports"] = []

                # Remove invalid running exports
                valid_entity_types = ["leads", "contacts", "companies", "events", "users", "pipelines"]
                global_state["running_exports"] = [
                    export for export in global_state["running_exports"]
                    if export in valid_entity_types
                ]

            # Save repaired state
            self.save_state()
            log_event("state", "info", "State repair completed successfully")
            return True

        except Exception as e:
            log_event("state", "error", f"State repair failed: {e}")
            return False

    def get_recovery_status(self) -> Dict[str, Any]:
        """
        Get current recovery status and available recovery options

        Returns:
            Dict with recovery status information
        """
        status = {
            "state_valid": True,
            "last_validation": None,
            "available_backups": 0,
            "latest_checkpoint": None,
            "recovery_options": []
        }

        try:
            # Check current state validity
            validation_result = self.validate_state_integrity()
            status["state_valid"] = validation_result["valid"]
            status["last_validation"] = validation_result["checked_at"]

            # Count available backups
            backups = self.get_backup_list(limit=100)
            status["available_backups"] = len(backups)

            # Find latest checkpoint
            checkpoints = [b for b in backups if b["backup_type"] == "checkpoint"]
            if checkpoints:
                status["latest_checkpoint"] = checkpoints[0]["timestamp"]

            # Determine recovery options
            if not status["state_valid"]:
                if checkpoints:
                    status["recovery_options"].append("restore_from_checkpoint")
                if any(b["backup_type"] == "manual" for b in backups):
                    status["recovery_options"].append("restore_from_manual_backup")
                if backups:
                    status["recovery_options"].append("restore_from_any_backup")
                status["recovery_options"].append("repair_state")
                status["recovery_options"].append("emergency_recovery")

        except Exception as e:
            log_event("state", "error", f"Error getting recovery status: {e}")
            status["error"] = str(e)

        return status

    def get_export_diagnostics(self, entity_type: str | None = None) -> Dict[str, Any]:
        """Get detailed diagnostics for exports"""
        diagnostics = {
            "timestamp": datetime.now().isoformat(),
            "system_info": {
                "hostname": self._get_hostname(),
                "process_id": self._get_process_id()
            },
            "exports": {}
        }

        # Get entity types to diagnose
        entity_types = [entity_type] if entity_type else ["leads", "contacts", "companies", "events", "users", "pipelines"]

        for etype in entity_types:
            export_diag = {
                "entity_type": etype,
                "state": {},
                "heartbeat": {},
                "exported_ids_count": 0,
                "issues": [],
                "recommendations": []
            }

            # Get state information
            try:
                export_diag["state"] = {
                    "running": self.is_export_running(etype),
                    "completed": self.is_export_completed(etype),
                    "last_page": self.get_last_page(etype),
                    "last_update": self.state.get(etype, {}).get("last_update")
                }
            except Exception as e:
                export_diag["issues"].append(f"Error getting state: {e}")

            # Get heartbeat information
            try:
                heartbeat = self.get_heartbeat_status(etype)
                if heartbeat:
                    export_diag["heartbeat"] = {
                        "last_heartbeat": heartbeat.get("last_heartbeat"),
                        "thread_id": heartbeat.get("thread_id"),
                        "hostname": heartbeat.get("hostname"),
                        "process_id": heartbeat.get("process_id"),
                        "metadata": heartbeat.get("metadata", {})
                    }

                    # Check if heartbeat is stale
                    if isinstance(heartbeat.get("last_heartbeat"), datetime):
                        time_since_heartbeat = datetime.now() - heartbeat["last_heartbeat"]
                        if time_since_heartbeat.total_seconds() > 600:  # 10 minutes
                            export_diag["issues"].append(f"Stale heartbeat: {time_since_heartbeat}")
                            export_diag["recommendations"].append("Consider cleaning up stale export")
                else:
                    if export_diag["state"]["running"]:
                        export_diag["issues"].append("Export marked as running but no heartbeat found")
                        export_diag["recommendations"].append("Check if export process is actually running")
            except Exception as e:
                export_diag["issues"].append(f"Error getting heartbeat: {e}")

            # Get exported IDs count
            try:
                export_diag["exported_ids_count"] = self.get_exported_ids_count(etype)
            except Exception as e:
                export_diag["issues"].append(f"Error getting exported IDs count: {e}")

            # State consistency checks
            if export_diag["state"]["running"] and export_diag["state"]["completed"]:
                export_diag["issues"].append("Export marked as both running and completed")
                export_diag["recommendations"].append("Fix state inconsistency")

            if export_diag["state"]["last_page"] > 0 and export_diag["exported_ids_count"] == 0:
                export_diag["issues"].append("Pages processed but no exported IDs tracked")
                export_diag["recommendations"].append("Check exported IDs tracking")

            diagnostics["exports"][etype] = export_diag

        # Global diagnostics
        try:
            diagnostics["global"] = {
                "total_backups": len(self.get_backup_list(limit=1000)),
                "running_exports_count": len(self.get_running_exports()),
                "stale_exports": self.check_stale_exports(),
                "state_valid": self.validate_state_integrity()["valid"]
            }
        except Exception as e:
            diagnostics["global"] = {"error": str(e)}

        return diagnostics

    def create_diagnostic_report(self, entity_type: str | None = None) -> str:
        """Create a formatted diagnostic report"""
        diagnostics = self.get_export_diagnostics(entity_type)

        report = f"=== Export Diagnostics Report ===\n"
        report += f"Generated: {diagnostics['timestamp']}\n"
        report += f"System: {diagnostics['system_info']['hostname']} (PID: {diagnostics['system_info']['process_id']})\n\n"

        # Global status
        if "global" in diagnostics:
            global_info = diagnostics["global"]
            report += f"Global Status:\n"
            report += f"  - Running exports: {global_info.get('running_exports_count', 'N/A')}\n"
            report += f"  - Total backups: {global_info.get('total_backups', 'N/A')}\n"
            report += f"  - State valid: {global_info.get('state_valid', 'N/A')}\n"
            report += f"  - Stale exports: {global_info.get('stale_exports', [])}\n\n"

        # Export details
        for etype, export_diag in diagnostics["exports"].items():
            report += f"{etype.upper()} Export:\n"
            state = export_diag["state"]
            report += f"  State: Running={state.get('running')}, Completed={state.get('completed')}, Last Page={state.get('last_page')}\n"
            report += f"  Exported IDs: {export_diag['exported_ids_count']}\n"

            if export_diag["heartbeat"]:
                hb = export_diag["heartbeat"]
                report += f"  Heartbeat: {hb.get('last_heartbeat')} (Thread: {hb.get('thread_id')})\n"
            else:
                report += f"  Heartbeat: None\n"

            if export_diag["issues"]:
                report += f"  Issues: {', '.join(export_diag['issues'])}\n"

            if export_diag["recommendations"]:
                report += f"  Recommendations: {', '.join(export_diag['recommendations'])}\n"

            report += "\n"

        return report

    def send_diagnostic_notification(self, entity_type: str, issue: str, level: str = "warning"):
        """Send diagnostic notification (can be extended to email, Slack, etc.)"""
        log_event("diagnostics", level, f"Export {entity_type}: {issue}")

        # Here you could add integrations with:
        # - Email notifications
        # - Slack/Discord webhooks
        # - SMS alerts
        # - Monitoring systems (Prometheus, etc.)

        # For now, just log to MongoDB
        try:
            notification = {
                "timestamp": datetime.now().isoformat(),
                "entity_type": entity_type,
                "issue": issue,
                "level": level,
                "hostname": self._get_hostname(),
                "process_id": self._get_process_id()
            }

            # Store in a notifications collection
            notifications_collection = self.db['export_notifications']
            notifications_collection.insert_one(notification)

        except Exception as e:
            log_event("diagnostics", "error", f"Failed to store notification: {e}")

    def check_and_notify_issues(self):
        """Check for issues and send notifications"""
        try:
            # Check for stale exports
            stale_exports = self.check_stale_exports()
            for entity_type in stale_exports:
                self.send_diagnostic_notification(
                    entity_type,
                    "Export appears to be stale (no heartbeat)",
                    "warning"
                )

            # Check state validity
            validation_result = self.validate_state_integrity()
            if not validation_result["valid"]:
                for error in validation_result["errors"]:
                    self.send_diagnostic_notification(
                        "system",
                        f"State validation error: {error}",
                        "error"
                    )

            # Check for exports running too long
            running_exports = self.get_running_exports()
            for entity_type in running_exports:
                state = self.state.get(entity_type, {})
                last_update = state.get("last_update")
                if last_update:
                    try:
                        last_update_dt = datetime.fromisoformat(last_update)
                        time_running = datetime.now() - last_update_dt
                        if time_running.total_seconds() > 3600:  # 1 hour
                            self.send_diagnostic_notification(
                                entity_type,
                                f"Export running for {time_running} - may be stuck",
                                "warning"
                            )
                    except Exception:
                        pass

        except Exception as e:
            log_event("diagnostics", "error", f"Error checking for issues: {e}")

    # Heartbeat methods for tracking active exports

    def send_heartbeat(self, entity_type: str, thread_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None):
        """Send heartbeat for an active export"""
        entity_type = self._normalize_entity_type(entity_type)

        try:
            heartbeat_data = {
                "entity_type": entity_type,
                "last_heartbeat": datetime.now(),
                "thread_id": thread_id,
                "metadata": metadata or {},
                "hostname": self._get_hostname(),
                "process_id": self._get_process_id()
            }

            # Upsert heartbeat document
            self.heartbeat_collection.replace_one(
                {"entity_type": entity_type},
                heartbeat_data,
                upsert=True
            )

        except PyMongoError as e:
            log_event("state", "error", f"Error sending heartbeat for {entity_type}: {e}")

    def get_heartbeat_status(self, entity_type: str) -> Optional[Dict[str, Any]]:
        """Get heartbeat status for an entity type"""
        entity_type = self._normalize_entity_type(entity_type)

        try:
            heartbeat = self.heartbeat_collection.find_one({"entity_type": entity_type})
            if heartbeat:
                # Convert ObjectId to string for JSON serialization
                heartbeat["_id"] = str(heartbeat["_id"])
                return heartbeat
            return None
        except PyMongoError as e:
            log_event("state", "error", f"Error getting heartbeat status for {entity_type}: {e}")
            return None

    def check_stale_exports(self, timeout_minutes: int = 10) -> List[str]:
        """Check for exports that haven't sent heartbeat within timeout"""
        try:
            cutoff_time = datetime.now() - timedelta(minutes=timeout_minutes)

            # Find exports that are marked as running but haven't sent heartbeat
            running_exports = self.get_running_exports()
            stale_exports = []

            for entity_type in running_exports:
                heartbeat = self.get_heartbeat_status(entity_type)
                if not heartbeat:
                    # No heartbeat found - export is stale
                    stale_exports.append(entity_type)
                    log_event("state", "warning", f"Export {entity_type} has no heartbeat")
                elif heartbeat["last_heartbeat"] < cutoff_time:
                    # Heartbeat is too old - export is stale
                    stale_exports.append(entity_type)
                    log_event("state", "warning", f"Export {entity_type} heartbeat is stale: {heartbeat['last_heartbeat']}")

            return stale_exports

        except Exception as e:
            log_event("state", "error", f"Error checking stale exports: {e}")
            return []

    def cleanup_stale_exports(self, timeout_minutes: int = 10) -> List[str]:
        """Clean up stale exports and return list of cleaned exports"""
        stale_exports = self.check_stale_exports(timeout_minutes)

        for entity_type in stale_exports:
            log_event("state", "warning", f"Cleaning up stale export: {entity_type}")

            # Mark export as stopped
            self.mark_export_stopped(entity_type)

            # Remove heartbeat
            try:
                self.heartbeat_collection.delete_one({"entity_type": entity_type})
            except PyMongoError as e:
                log_event("state", "error", f"Error removing heartbeat for {entity_type}: {e}")

        if stale_exports:
            log_event("state", "info", f"Cleaned up {len(stale_exports)} stale exports: {stale_exports}")

        return stale_exports

    def get_all_heartbeats(self) -> List[Dict[str, Any]]:
        """Get all current heartbeats"""
        try:
            heartbeats = list(self.heartbeat_collection.find({}))
            # Convert ObjectId to string for JSON serialization
            for heartbeat in heartbeats:
                heartbeat["_id"] = str(heartbeat["_id"])
            return heartbeats
        except PyMongoError as e:
            log_event("state", "error", f"Error getting all heartbeats: {e}")
            return []

    def _get_hostname(self) -> str:
        """Get current hostname"""
        try:
            import socket
            return socket.gethostname()
        except Exception:
            return "unknown"

    def _get_process_id(self) -> int:
        """Get current process ID"""
        try:
            import os
            return os.getpid()
        except Exception:
            return 0

    def _ensure_backup_indexes(self):
        """Ensure indexes exist for backup collection"""
        try:
            # Create index on timestamp for efficient cleanup
            self.backup_collection.create_index([("timestamp", -1)])
            # Create index on backup_type for filtering
            self.backup_collection.create_index([("backup_type", 1)])
            # Create compound index for efficient queries
            self.backup_collection.create_index([("backup_type", 1), ("timestamp", -1)])
            log_event("state", "info", "Created indexes for backup collection")
        except PyMongoError as e:
            log_event("state", "error", f"Error creating backup indexes: {e}")

    def _ensure_heartbeat_indexes(self):
        """Ensure indexes exist for heartbeat collection"""
        try:
            # Create index on entity_type for fast lookups
            self.heartbeat_collection.create_index([("entity_type", 1)])
            # Create index on last_heartbeat for timeout detection
            self.heartbeat_collection.create_index([("last_heartbeat", 1)])
            # Create compound index for efficient queries
            self.heartbeat_collection.create_index([("entity_type", 1), ("last_heartbeat", -1)])
            # Create TTL index to automatically cleanup old heartbeats (after 1 hour)
            self.heartbeat_collection.create_index([("last_heartbeat", 1)], expireAfterSeconds=3600)
            log_event("state", "info", "Created indexes for heartbeat collection")
        except PyMongoError as e:
            log_event("state", "error", f"Error creating heartbeat indexes: {e}")

    def _normalize_entity_type(self, entity_type: str) -> str:
        """Normalize entity type to internal representation"""
        if entity_type in ["deals", "leads"]:
            return "leads"
        return entity_type

    def update_export_progress(
        self, entity_type: str, page: int, completed: bool = False, exported_ids: Optional[List[int]] = None
    ):
        """Update the progress of an export with optional atomic save"""
        entity_type = self._normalize_entity_type(entity_type)
        if entity_type not in self.state:
            self.state[entity_type] = {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            }

        self.state[entity_type]["last_page"] = page
        self.state[entity_type]["completed"] = completed
        self.state[entity_type]["last_update"] = datetime.now().isoformat()

        # Use atomic save if exported_ids provided
        if exported_ids:
            exported_ids_updates = {entity_type: exported_ids}
            self.save_state_atomic(exported_ids_updates)
        else:
            self.save_state()

        # Create checkpoint if needed (every 5 minutes)
        self.create_checkpoint_if_needed()

    def get_last_page(self, entity_type: str) -> int:
        """Get the last processed page for an entity type"""
        entity_type = self._normalize_entity_type(entity_type)
        if entity_type in self.state:
            return self.state[entity_type]["last_page"]
        return 0

    def is_export_completed(self, entity_type: str) -> bool:
        """Check if an export is completed"""
        entity_type = self._normalize_entity_type(entity_type)
        if entity_type in self.state:
            return self.state[entity_type]["completed"]
        return False

    def reset_export_state(self, entity_type: str):
        """Reset the state for an entity type"""
        entity_type = self._normalize_entity_type(entity_type)
        if entity_type in self.state:
            self.state[entity_type] = {
                "last_page": 0,
                "completed": False,
                "last_update": None,
            }
            self.save_state()

            # Clear exported IDs for this entity type
            self.clear_exported_ids(entity_type)

    def mark_export_running(self, entity_type: str):
        """Mark an export as currently running"""
        entity_type = self._normalize_entity_type(entity_type)
        if "global" not in self.state:
            self.state["global"] = {"running_exports": []}

        if "running_exports" not in self.state["global"]:
            self.state["global"]["running_exports"] = []

        if entity_type not in self.state["global"]["running_exports"]:
            self.state["global"]["running_exports"].append(entity_type)
            self.save_state()

    def mark_export_stopped(self, entity_type: str):
        """Mark an export as stopped"""
        entity_type = self._normalize_entity_type(entity_type)
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
        entity_type = self._normalize_entity_type(entity_type)
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
        for entity_type in ["leads", "contacts", "companies", "events", "users", "pipelines"]:
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
                return entity_type in result["global"]["running_exports"]
            return False
        except PyMongoError as e:
            log_event(
                "state", "error",
                f"Error checking if export is running in DB: {e}"
            )
            # Fall back to in-memory state
            return self.is_export_running(entity_type)

    # Task management methods

    def save_task(self, task_data: Dict[str, Any]) -> bool:
        """Save a task to the database"""
        try:
            # Add created_at and updated_at timestamps if not present
            if "created_at" not in task_data:
                task_data["created_at"] = datetime.now().isoformat()

            task_data["updated_at"] = datetime.now().isoformat()

            # Use task_id as MongoDB _id for easier lookups
            task_id = task_data["task_id"]
            task_data["_id"] = task_id

            # Insert or update the task
            self.task_collection.replace_one(
                {"_id": task_id}, task_data, upsert=True
            )

            log_event("state", "info", f"Saved task {task_id} to database")
            return True
        except PyMongoError as e:
            log_event("state", "error", f"Error saving task to database: {e}")
            return False

    def update_task_status(self, task_id: str, status: str) -> bool:
        """Update the status of a task"""
        try:
            self.task_collection.update_one(
                {"_id": task_id},
                {
                    "$set": {
                        "status": status,
                        "updated_at": datetime.now().isoformat()
                    }
                }
            )

            log_event("state", "info", f"Updated task {task_id} status to {status}")
            return True
        except PyMongoError as e:
            log_event("state", "error", f"Error updating task status: {e}")
            return False

    def get_pending_tasks(self) -> List[Dict[str, Any]]:
        """Get all pending tasks"""
        try:
            tasks = list(self.task_collection.find(
                {"status": "pending"},
                {"_id": 0}  # Exclude MongoDB _id field
            ))

            return tasks
        except PyMongoError as e:
            log_event("state", "error", f"Error getting pending tasks: {e}")
            return []

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks"""
        try:
            tasks = list(self.task_collection.find({}, {"_id": 0}))
            return tasks
        except PyMongoError as e:
            log_event("state", "error", f"Error getting all tasks: {e}")
            return []

    def get_task_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get a task by ID"""
        try:
            task = self.task_collection.find_one({"_id": task_id}, {"_id": 0})
            return task
        except PyMongoError as e:
            log_event("state", "error", f"Error getting task by ID: {e}")
            return None

    def delete_task(self, task_id: str) -> bool:
        """Delete a task"""
        try:
            self.task_collection.delete_one({"_id": task_id})
            log_event("state", "info", f"Deleted task {task_id}")
            return True
        except PyMongoError as e:
            log_event("state", "error", f"Error deleting task: {e}")
            return False

    def clean_old_tasks(self, days: int = 30) -> int:
        """Clean up old completed tasks"""
        try:
            # Calculate cutoff date
            cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)

            # Delete tasks older than cutoff date that are not pending or processing
            result = self.task_collection.delete_many({
                "status": {"$in": ["completed", "failed", "cancelled"]},
                "updated_at": {"$lt": cutoff_date}
            })

            count = result.deleted_count
            log_event("state", "info", f"Cleaned up {count} old tasks")
            return count
        except PyMongoError as e:
            log_event("state", "error", f"Error cleaning up old tasks: {e}")
            return 0

    # Exported IDs tracking methods

    def add_exported_ids(self, entity_type: str, entity_ids: List[int]) -> bool:
        """Add entity IDs to the exported IDs collection"""
        if not entity_ids:
            return True

        entity_type = self._normalize_entity_type(entity_type)

        try:
            # Use bulk operations for efficiency
            operations = []
            for entity_id in entity_ids:
                operations.append({
                    "updateOne": {
                        "filter": {"entity_type": entity_type},
                        "update": {"$addToSet": {"exported_ids": entity_id}},
                        "upsert": True
                    }
                })

            # Execute bulk operations in batches
            batch_size = 100
            for i in range(0, len(operations), batch_size):
                batch = operations[i:i+batch_size]
                self.exported_ids_collection.bulk_write([
                    pymongo.UpdateOne(
                        op["updateOne"]["filter"],
                        op["updateOne"]["update"],
                        upsert=op["updateOne"]["upsert"]
                    ) for op in batch
                ])

            log_event("state", "info", f"Added {len(entity_ids)} exported IDs for {entity_type}")
            return True
        except PyMongoError as e:
            log_event("state", "error", f"Error adding exported IDs: {e}")
            return False

    def is_entity_exported(self, entity_type: str, entity_id: int) -> bool:
        """Check if an entity ID has already been exported"""
        entity_type = self._normalize_entity_type(entity_type)

        try:
            result = self.exported_ids_collection.find_one({
                "entity_type": entity_type,
                "exported_ids": entity_id
            })
            return result is not None
        except PyMongoError as e:
            log_event("state", "error", f"Error checking if entity is exported: {e}")
            return False

    def get_exported_ids(self, entity_type: str) -> List[int]:
        """Get all exported IDs for an entity type"""
        entity_type = self._normalize_entity_type(entity_type)

        try:
            result = self.exported_ids_collection.find_one({"entity_type": entity_type})
            if result and "exported_ids" in result:
                return result["exported_ids"]
            return []
        except PyMongoError as e:
            log_event("state", "error", f"Error getting exported IDs: {e}")
            return []

    def filter_already_exported(self, entity_type: str, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out entities that have already been exported"""
        if not entities:
            return []

        entity_type = self._normalize_entity_type(entity_type)

        try:
            # Get all exported IDs for this entity type
            exported_ids = set(self.get_exported_ids(entity_type))

            # Filter out entities that have already been exported
            filtered_entities = []
            for entity in entities:
                if "id" in entity and entity["id"] not in exported_ids:
                    filtered_entities.append(entity)

            log_event("state", "info",
                     f"Filtered {len(entities) - len(filtered_entities)} already exported {entity_type}")
            return filtered_entities
        except Exception as e:
            log_event("state", "error", f"Error filtering already exported entities: {e}")
            return entities

    def clear_exported_ids(self, entity_type: str) -> bool:
        """Clear all exported IDs for an entity type (used during force_restart)"""
        entity_type = self._normalize_entity_type(entity_type)

        try:
            result = self.exported_ids_collection.delete_many({"entity_type": entity_type})
            log_event("state", "info", f"Cleared {result.deleted_count} exported ID records for {entity_type}")
            return True
        except PyMongoError as e:
            log_event("state", "error", f"Error clearing exported IDs: {e}")
            return False

    def get_exported_ids_count(self, entity_type: str) -> int:
        """Get count of exported IDs for an entity type"""
        entity_type = self._normalize_entity_type(entity_type)

        try:
            result = self.exported_ids_collection.find_one({"entity_type": entity_type})
            if result and "exported_ids" in result:
                return len(result["exported_ids"])
            return 0
        except PyMongoError as e:
            log_event("state", "error", f"Error getting exported IDs count: {e}")
            return 0
