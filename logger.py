"""
Logging module for AmoCRM exporter
"""

import json
import os
from datetime import datetime
from typing import Any

import config

# This is a circular import if we import Storage directly, so we'll initialize storage later
storage = None


def init_storage(storage_instance):
    """Initialize the storage instance"""
    global storage
    storage = storage_instance


def log_event(
    component: str,
    level: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> bool:
    """
    Log an event to the log file and console

    Args:
        component: The component generating the log (e.g., 'api', 'webhook', 'storage')
        level: The log level ('info', 'warning', 'error')
        message: The log message
        details: Additional details to include in the log

    Returns:
        bool: True if the log was successfully written, False otherwise
    """
    timestamp = datetime.now().isoformat()

    log_entry = {
        "timestamp": timestamp,
        "component": component,
        "level": level,
        "message": message,
    }

    if details:
        log_entry["details"] = details

    # Print to console
    print(f"[{timestamp}] [{component}] [{level.upper()}] {message}")

    # If storage is not initialized, store log in temporary buffer
    if storage is None:
        return _store_log_in_buffer(log_entry)

    # Write to log file using storage module
    return storage.add_log_entry(log_entry)


# Buffer for logs before storage is initialized
_log_buffer = []


def _store_log_in_buffer(log_entry: dict[str, Any]) -> bool:
    """Store log in a temporary buffer until storage is initialized"""
    global _log_buffer
    _log_buffer.append(log_entry)
    return True


def flush_log_buffer():
    """Flush the log buffer to storage once it's initialized"""
    global _log_buffer, storage

    if storage is None or not _log_buffer:
        return

    for log_entry in _log_buffer:
        storage.add_log_entry(log_entry)

    _log_buffer = []


def get_recent_logs(count=100):
    """Get the most recent logs from the log file"""
    log_file = config.settings.log_file
    logs = []

    if os.path.exists(log_file):
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
                # Get the last N logs
                logs = logs[-count:] if len(logs) > count else logs
        except Exception as e:
            logs = [
                {
                    "timestamp": datetime.now().isoformat(),
                    "level": "error",
                    "message": f"Error loading logs: {e}",
                }
            ]

    return logs
