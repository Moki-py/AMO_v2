"""
Storage module for handling JSON files
"""

import os
import json
import time
from typing import Any
from datetime import datetime

import config
from logger import log_event


class Storage:
    """Storage manager for JSON files"""

    def __init__(self):
        """Initialize the storage manager"""
        # Ensure data directory exists
        os.makedirs(config.settings.data_dir, exist_ok=True)

        # Initialize files if they don't exist
        self._initialize_files()

    def _initialize_files(self):
        """Initialize JSON files if they don't exist"""
        files = [
            config.settings.deals_file,
            config.settings.contacts_file,
            config.settings.companies_file,
            config.settings.events_file,
            config.settings.log_file,
        ]

        for file_path in files:
            if not os.path.exists(file_path):
                self._write_json_file(file_path, [])
                log_event(
                    "storage", "info", f"Initialized empty file: {file_path}"
                )

    def _read_json_file(self, file_path: str) -> list[dict[str, Any]]:
        """Read a JSON file and return its contents with retry logic"""
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return data
            except json.JSONDecodeError:
                log_event(
                    "storage",
                    "error",
                    f"Failed to decode JSON from {file_path} (attempt {attempt+1}/{max_retries})",
                )
                if os.path.getsize(file_path) == 0:
                    # If file is empty, return empty list
                    return []
                # Wait before retrying
                time.sleep(retry_delay)
            except FileNotFoundError:
                log_event("storage", "error", f"File not found: {file_path}")
                # Create the file with an empty array
                self._write_json_file(file_path, [])
                return []
            except Exception as e:
                log_event(
                    "storage", "error", f"Error reading file {file_path}: {e}"
                )
                # Wait before retrying
                time.sleep(retry_delay)

        # If all retries failed, return empty list
        log_event(
            "storage",
            "error",
            f"All attempts to read {file_path} failed, returning empty list",
        )
        return []

    def _write_json_file(
        self, file_path: str, data: list[dict[str, Any]]
    ) -> bool:
        """Write data to a JSON file with retry logic"""
        max_retries = 3
        retry_delay = 1  # seconds

        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        for attempt in range(max_retries):
            try:
                # Write to a temporary file first
                temp_file = f"{file_path}.tmp"
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                # Rename the temporary file to the target file (atomic operation)
                if os.path.exists(file_path):
                    os.replace(temp_file, file_path)
                else:
                    os.rename(temp_file, file_path)

                return True
            except Exception as e:
                log_event(
                    "storage",
                    "error",
                    f"Error writing to {file_path} (attempt {attempt+1}/{max_retries}): {e}",
                )
                # Wait before retrying
                time.sleep(retry_delay)

        # If all retries failed
        log_event(
            "storage", "error", f"All attempts to write to {file_path} failed"
        )
        return False

    def save_entities(
        self, entity_type: str, entities: list[dict[str, Any]]
    ) -> bool:
        """Save a list of entities to the appropriate file"""
        file_path = self._get_file_path(entity_type)
        if not file_path:
            log_event(
                "storage", "error", f"Unknown entity type: {entity_type}"
            )
            return False

        log_event(
            "storage",
            "info",
            f"Saving {len(entities)} {entity_type} to {file_path}",
        )
        return self._write_json_file(file_path, entities)

    def append_entities(
        self, entity_type: str, entities: list[dict[str, Any]]
    ) -> bool:
        """Append entities to the existing file"""
        if not entities:
            return True  # Nothing to append

        file_path = self._get_file_path(entity_type)
        if not file_path:
            log_event(
                "storage", "error", f"Unknown entity type: {entity_type}"
            )
            return False

        # Read existing entities
        existing_entities = self.get_entities(entity_type)

        # Append new entities
        existing_entities.extend(entities)

        log_event(
            "storage",
            "info",
            f"Appending {len(entities)} {entity_type} to existing {len(existing_entities) - len(entities)}",
        )
        return self._write_json_file(file_path, existing_entities)

    def get_entities(self, entity_type: str) -> list[dict[str, Any]]:
        """Get all entities of a specific type"""
        file_path = self._get_file_path(entity_type)
        if not file_path:
            log_event(
                "storage", "error", f"Unknown entity type: {entity_type}"
            )
            return []

        return self._read_json_file(file_path)

    def update_entity(
        self, entity_type: str, entity_id: int, entity_data: dict[str, Any]
    ) -> bool:
        """Update a specific entity or add it if it doesn't exist"""
        entities = self.get_entities(entity_type)

        # Find the entity by ID
        entity_index = next(
            (i for i, e in enumerate(entities) if e.get("id") == entity_id),
            None,
        )

        if entity_index is not None:
            # Update existing entity
            entities[entity_index] = entity_data
            log_event(
                "storage", "info", f"Updated {entity_type} with ID {entity_id}"
            )
        else:
            # Add new entity
            entities.append(entity_data)
            log_event(
                "storage",
                "info",
                f"Added new {entity_type} with ID {entity_id}",
            )

        # Save updated list
        file_path = self._get_file_path(entity_type)
        return self._write_json_file(file_path, entities)

    def delete_entity(self, entity_type: str, entity_id: int) -> bool:
        """Delete a specific entity"""
        entities = self.get_entities(entity_type)

        # Filter out the entity with the specified ID
        updated_entities = [e for e in entities if e.get("id") != entity_id]

        # If no entity was removed, return False
        if len(entities) == len(updated_entities):
            log_event(
                "storage",
                "warning",
                f"No {entity_type} with ID {entity_id} found to delete",
            )
            return False

        log_event(
            "storage", "info", f"Deleted {entity_type} with ID {entity_id}"
        )

        # Save updated list
        file_path = self._get_file_path(entity_type)
        return self._write_json_file(file_path, updated_entities)

    def get_entity_count(self, entity_type: str) -> int:
        """Get the count of entities of a specific type"""
        entities = self.get_entities(entity_type)
        return len(entities)

    def _get_file_path(self, entity_type: str) -> str | None:
        """Get the file path for a specific entity type"""
        if entity_type == "leads" or entity_type == "deals":
            return config.settings.deals_file
        elif entity_type == "contacts":
            return config.settings.contacts_file
        elif entity_type == "companies":
            return config.settings.companies_file
        elif entity_type == "events":
            return config.settings.events_file
        else:
            return None

    def add_log_entry(self, entry: dict[str, Any]) -> bool:
        """Add an entry to the log file"""
        logs = self._read_json_file(config.settings.log_file)

        # Add timestamp if not present
        if "timestamp" not in entry:
            entry["timestamp"] = datetime.now().isoformat()

        logs.append(entry)

        # Clean up logs older than retention period
        logs = self._clean_old_logs(logs)

        return self._write_json_file(config.settings.log_file, logs)

    def _clean_old_logs(
        self, logs: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Remove log entries older than the retention period"""
        retention_date = datetime.now().timestamp() - (
            config.settings.log_retention_days * 24 * 60 * 60
        )

        # Filter out old logs
        current_logs = []
        for log in logs:
            try:
                log_date = datetime.fromisoformat(log["timestamp"]).timestamp()
                if log_date >= retention_date:
                    current_logs.append(log)
            except (ValueError, KeyError):
                # Keep logs with invalid dates or missing timestamp (shouldn't happen)
                current_logs.append(log)

        return current_logs

    def get_statistics(self) -> dict[str, Any]:
        """Get statistics about stored data"""
        return {
            "deals": self.get_entity_count("leads"),
            "contacts": self.get_entity_count("contacts"),
            "companies": self.get_entity_count("companies"),
            "events": self.get_entity_count("events"),
            "logs": len(self._read_json_file(config.settings.log_file)),
        }
