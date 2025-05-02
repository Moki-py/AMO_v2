"""
State manager for tracking export progress
"""

import os
import json
from datetime import datetime
from typing import Any

import config
from logger import log_event


class StateManager:
    """Manages the state of exports to enable resume functionality"""

    def __init__(self):
        """Initialize the state manager"""
        self.state_file = config.settings.state_file
        self.state = self._load_state()

    def _load_state(self) -> dict[str, Any]:
        """Load the export state from file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                log_event("state", "error", f"Error loading state: {e}")

        # Default state if file doesn't exist or loading fails
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
        """Save the current state to file"""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)

            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            log_event("state", "error", f"Error saving state: {e}")

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

    def get_running_exports(self) -> list:
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
