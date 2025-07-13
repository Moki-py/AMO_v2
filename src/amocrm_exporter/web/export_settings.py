"""
Export Settings Manager for AmoCRM Data Exporter

This module handles:
- Dynamic field loading from database
- Export settings storage and retrieval
- Field preview data generation
- Custom field parsing and handling
- Caching for performance optimization
"""

import asyncio
import json
import re
import concurrent.futures
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum

from pymongo import MongoClient
from bson import ObjectId

from ..core.logger import log_event
from ..storage.storage import Storage
from ..enrichment.data_enrichment import DataEnricher
from ..core import config


class EntityType(str, Enum):
    """Supported entity types for export"""
    DEALS = "deals"
    CONTACTS = "contacts"
    COMPANIES = "companies"
    EVENTS = "events"
    USERS = "users"
    PIPELINES = "pipelines"


@dataclass
class FieldInfo:
    """Information about a field available for export"""
    field_id: str
    field_name: str
    field_type: str
    is_custom: bool = False
    custom_id: Optional[str] = None
    preview_data: Optional[List[str]] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExportSettings:
    """Export settings for a specific entity type"""
    entity_type: str
    selected_fields: List[str]
    field_order: List[str]
    filters: Dict[str, Any]
    name: str
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
        if self.updated_at:
            data['updated_at'] = self.updated_at.isoformat()
        return data


class ExportSettingsManager:
    """Manages export settings with MongoDB storage and caching"""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.db = storage.db
        self.settings_collection = self.db.export_settings
        self.fields_cache: Dict[str, List[FieldInfo]] = {}
        self.preview_cache: Dict[str, List[str]] = {}
        self.cache_ttl = timedelta(minutes=config.settings.cache_ttl_minutes)
        self.last_cache_update: Dict[str, datetime] = {}
        self.data_enricher = DataEnricher(storage)

        # Entity type to collection mapping
        self.entity_collections = {
            EntityType.DEALS: "deals",
            EntityType.CONTACTS: "contacts",
            EntityType.COMPANIES: "companies",
            EntityType.EVENTS: "events",
            EntityType.USERS: "users",
            EntityType.PIPELINES: "pipelines"
        }

        log_event("export_settings", "info", "Export Settings Manager initialized")

    async def get_available_fields(self, entity_type: EntityType, force_refresh: bool = False) -> List[FieldInfo]:
        """Get all available fields for an entity type with caching"""
        cache_key = f"{entity_type}_fields"

        # Check cache validity
        if not force_refresh and self._is_cache_valid(cache_key):
            cached_fields = self.fields_cache.get(cache_key, [])
            if cached_fields:
                log_event("export_settings", "debug", f"Returning cached fields for {entity_type}")
                return cached_fields

        log_event("export_settings", "info", f"Loading available fields for {entity_type}")

        try:
            fields = await self._load_fields_from_db(entity_type)

            # Cache the results
            self.fields_cache[cache_key] = fields
            self.last_cache_update[cache_key] = datetime.now()

            log_event("export_settings", "info", f"Loaded {len(fields)} fields for {entity_type}")
            return fields

        except Exception as e:
            log_event("export_settings", "error", f"Error loading fields for {entity_type}: {e}")
            return []

    async def _load_fields_from_db(self, entity_type: EntityType) -> List[FieldInfo]:
        """Load field information from database"""
        collection_name = self.entity_collections.get(entity_type)
        if not collection_name:
            raise ValueError(f"Unknown entity type: {entity_type}")

        collection = self.db[collection_name]

        # Get sample documents to analyze field structure
        # Увеличиваем размер выборки для лучшего обнаружения полей
        sample_limit = min(1000, config.settings.max_sample_size // 5)  # Увеличенный размер выборки

        # Используем более интеллектуальную выборку для анализа полей
        # Берем случайную выборку, чтобы покрыть больше вариантов полей
        pipeline = [
            {"$sample": {"size": sample_limit}},
            {"$sort": {"updated_at": -1}}  # Сортируем по дате обновления
        ]

        try:
            sample_docs = list(collection.aggregate(pipeline))
        except Exception as e:
            log_event("export_settings", "warning", f"Failed to use aggregation pipeline, falling back to simple find: {e}")
            sample_docs = list(collection.find().limit(sample_limit))

        if not sample_docs:
            log_event("export_settings", "warning", f"No data found for {entity_type}")
            return []

        fields = []
        all_field_names: set[str] = set()

        # Initialize tracking for custom field names and their original names
        self._custom_field_names = set()
        self._original_field_names = {}  # Maps field_name -> original human-readable name

        # Analyze field structure from sample documents
        for doc in sample_docs:
            self._extract_fields_from_doc(doc, all_field_names, entity_type)

        # Convert to FieldInfo objects
        for field_name in sorted(all_field_names):
            field_info = await self._create_field_info(field_name, entity_type, sample_docs)
            fields.append(field_info)

        return fields

    def _extract_fields_from_doc(self, doc: Dict[str, Any], field_names: set, entity_type: EntityType, prefix: str = "") -> None:
        """Recursively extract field names from document"""
        for key, value in doc.items():
            if key.startswith('_'):
                continue

            # Special handling for custom_fields_values
            if key == 'custom_fields_values' and isinstance(value, list):
                # Process custom fields to extract field names
                for custom_field in value:
                    if isinstance(custom_field, dict):
                        field_name = custom_field.get('field_name', '')
                        field_id = custom_field.get('field_id', '')

                        if field_name:
                            # Use field_name as the column name (same as in exporters)
                            column_name = field_name
                            # Sanitize column name
                            column_name = column_name.replace('/', '_').replace('\\', '_').replace('[', '').replace(']', '')
                            field_names.add(column_name)
                            # Track this as a custom field name and save original name
                            if hasattr(self, '_custom_field_names'):
                                self._custom_field_names.add(column_name)
                            if hasattr(self, '_original_field_names'):
                                self._original_field_names[column_name] = field_name
                        elif field_id:
                            # Fallback to field_id based name
                            fallback_name = f"custom_field_{field_id}"
                            field_names.add(fallback_name)
                            # Track this as a custom field name
                            if hasattr(self, '_custom_field_names'):
                                self._custom_field_names.add(fallback_name)
                continue

            # Special handling for events: value_after and value_before contain custom_field_value
            if key in ['value_after', 'value_before'] and isinstance(value, list) and entity_type == EntityType.EVENTS:
                for event_value in value:
                    if isinstance(event_value, dict) and 'custom_field_value' in event_value:
                        custom_field_value = event_value['custom_field_value']
                        if isinstance(custom_field_value, dict):
                            field_id = custom_field_value.get('field_id', '')
                            if field_id:
                                # For events, we use field_id as the column name
                                column_name = f"custom_field_{field_id}"
                                field_names.add(column_name)
                                # Track this as a custom field name
                                if hasattr(self, '_custom_field_names'):
                                    self._custom_field_names.add(column_name)
                continue

            full_key = f"{prefix}.{key}" if prefix else key
            field_names.add(full_key)

            # Handle nested objects
            if isinstance(value, dict) and not self._is_custom_field_data(value):
                self._extract_fields_from_doc(value, field_names, entity_type, full_key)

            # Handle arrays of objects
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                self._extract_fields_from_doc(value[0], field_names, entity_type, full_key)

    def _is_custom_field_data(self, value: Dict[str, Any]) -> bool:
        """Check if this looks like custom field data"""
        return (
            isinstance(value, dict) and
            any(key in value for key in ['field_id', 'field_name', 'values'])
        )

    async def _create_field_info(self, field_name: str, entity_type: EntityType, sample_docs: List[Dict]) -> FieldInfo:
        """Create FieldInfo object for a field"""
        # Check if it's a custom field
        is_custom = self._is_custom_field(field_name)
        custom_id = None

        if is_custom:
            custom_id = self._extract_custom_field_id(field_name)

        # Determine field type
        field_type = self._determine_field_type(field_name, sample_docs)

        # Generate preview data
        preview_data = await self._generate_preview_data(field_name, entity_type, sample_docs)

        # Generate description with special handling for events
        if entity_type == EntityType.EVENTS and field_name.startswith('custom_field_'):
            description = f"Кастомное поле для событий (ID: {custom_id})"
        else:
            description = self._generate_field_description(field_name, field_type, is_custom)

        # Use original field name for custom fields if available
        display_name = field_name
        if is_custom and hasattr(self, '_original_field_names') and field_name in self._original_field_names:
            display_name = self._original_field_names[field_name]
        else:
            display_name = self._format_field_name(field_name)

        return FieldInfo(
            field_id=field_name,
            field_name=display_name,
            field_type=field_type,
            is_custom=is_custom,
            custom_id=custom_id,
            preview_data=preview_data,
            description=description
        )

    def _is_custom_field(self, field_name: str) -> bool:
        """Determine if field is a custom field"""
        # Check if it's a custom field based on:
        # 1. Contains 'custom_fields' in path (old format)
        # 2. Starts with 'custom_field_' (fallback format)
        # 3. Matches a known custom field pattern
        if ('custom_fields' in field_name or
            field_name.startswith('custom_field_') or
            bool(re.match(r'.*_\d+_.*', field_name)) or
            'cf_' in field_name.lower()):
            return True

        # Check if this field name exists in the sample data's custom_fields_values
        # This is a more robust check for the new format
        return self._is_field_from_custom_fields_values(field_name)

    def _is_field_from_custom_fields_values(self, field_name: str) -> bool:
        """Check if field name came from custom_fields_values processing"""
        # This will be set during field extraction if the field came from custom_fields_values
        return hasattr(self, '_custom_field_names') and field_name in getattr(self, '_custom_field_names', set())

    def _extract_custom_field_id(self, field_name: str) -> Optional[str]:
        """Extract custom field ID from field name"""
        # Pattern: field_123_name -> 123
        match = re.search(r'_(\d+)_', field_name)
        if match:
            return match.group(1)
        return None

    def _determine_field_type(self, field_name: str, sample_docs: List[Dict]) -> str:
        """Determine field type from sample data"""
        values = []

        for doc in sample_docs[:10]:  # Check first 10 docs
            value = self._get_nested_value(doc, field_name)
            if value is not None:
                values.append(value)

        if not values:
            return "unknown"

        # Analyze value types
        first_value = values[0]

        if isinstance(first_value, bool):
            return "boolean"
        elif isinstance(first_value, int):
            return "integer"
        elif isinstance(first_value, float):
            return "number"
        elif isinstance(first_value, str):
            # Check for special string patterns
            if re.match(r'^\d{4}-\d{2}-\d{2}', first_value):
                return "date"
            elif re.match(r'^https?://', first_value):
                return "url"
            elif '@' in first_value:
                return "email"
            else:
                return "text"
        elif isinstance(first_value, list):
            return "array"
        elif isinstance(first_value, dict):
            return "object"
        else:
            return "unknown"

    def _get_nested_value(self, doc: Dict, field_path: str) -> Any:
        """Get value from nested field path"""
        # Check if this is a custom field that needs special handling
        if hasattr(self, '_custom_field_names') and field_path in getattr(self, '_custom_field_names', set()):
            # This is a custom field - look for it in custom_fields_values
            custom_fields_values = doc.get('custom_fields_values', [])
            if isinstance(custom_fields_values, list):
                for custom_field in custom_fields_values:
                    if isinstance(custom_field, dict):
                        field_name = custom_field.get('field_name', '')
                        field_id = custom_field.get('field_id', '')

                        # Check if this custom field matches our field_path
                        if field_name and field_name == field_path:
                            # Extract the value from the custom field
                            values = custom_field.get('values', [])
                            if values:
                                if len(values) == 1:
                                    return values[0].get('value', '')
                                else:
                                    # Multiple values - join them
                                    value_list = [str(val.get('value', '')) for val in values if val.get('value')]
                                    return ', '.join(value_list)
                        elif field_id and field_path == f"custom_field_{field_id}":
                            # Fallback matching by field_id
                            values = custom_field.get('values', [])
                            if values:
                                if len(values) == 1:
                                    return values[0].get('value', '')
                                else:
                                    # Multiple values - join them
                                    value_list = [str(val.get('value', '')) for val in values if val.get('value')]
                                    return ', '.join(value_list)
            return None

        # Regular field handling
        keys = field_path.split('.')
        value = doc

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None

        return value

    def _extract_event_custom_field_value(self, doc: Dict[str, Any], field_id: str) -> Any:
        """Extract custom field value from event document"""
        # Check value_after and value_before for custom_field_value
        for key in ['value_after', 'value_before']:
            if key in doc and isinstance(doc[key], list):
                for event_value in doc[key]:
                    if isinstance(event_value, dict) and 'custom_field_value' in event_value:
                        custom_field_value = event_value['custom_field_value']
                        if isinstance(custom_field_value, dict) and str(custom_field_value.get('field_id')) == field_id:
                            # Return the text value if available
                            return custom_field_value.get('text', custom_field_value.get('value', 'N/A'))
        return None

    async def _generate_preview_data(self, field_name: str, entity_type: EntityType, sample_docs: List[Dict]) -> List[str]:
        """Generate preview data for a field using optimized methods when possible"""
        # Use optimized storage method if available
        if hasattr(self.storage, 'get_field_sample_values'):
            collection_name = self.entity_collections.get(entity_type)
            if collection_name:
                try:
                    sample_values = self.storage.get_field_sample_values(collection_name, field_name, limit=3)
                    if sample_values:
                        return sample_values
                except Exception as e:
                    log_event("export_settings", "warning", f"Optimized preview failed for {field_name}, using fallback: {e}")

        # Fallback to original method
        preview_values = []

        for doc in sample_docs[:5]:  # Get first 5 examples
            # Special handling for events custom fields
            if entity_type == EntityType.EVENTS and field_name.startswith('custom_field_'):
                field_id = field_name.replace('custom_field_', '')
                value = self._extract_event_custom_field_value(doc, field_id)
            else:
                value = self._get_nested_value(doc, field_name)

            if value is not None:
                # Convert to string representation
                str_value = self._format_value_for_preview(value)
                if str_value and str_value not in preview_values:
                    preview_values.append(str_value)

            if len(preview_values) >= 3:  # Limit to 3 examples
                break

        return preview_values

    def _format_value_for_preview(self, value: Any) -> str:
        """Format value for preview display"""
        if value is None:
            return ""
        elif isinstance(value, (str, int, float, bool)):
            return str(value)[:100]  # Limit length
        elif isinstance(value, list):
            if len(value) == 0:
                return "[]"
            elif len(value) == 1:
                return f"[{self._format_value_for_preview(value[0])}]"
            else:
                return f"[{self._format_value_for_preview(value[0])}, ...]"
        elif isinstance(value, dict):
            if 'name' in value:
                return value['name']
            elif 'title' in value:
                return value['title']
            elif 'value' in value:
                return str(value['value'])
            else:
                return "{...}"
        else:
            return str(type(value).__name__)

    def _format_field_name(self, field_name: str) -> str:
        """Format field name for display"""
        # Convert snake_case to Title Case
        formatted = field_name.replace('_', ' ').replace('.', ' › ')
        return ' '.join(word.capitalize() for word in formatted.split())

    def _generate_field_description(self, field_name: str, field_type: str, is_custom: bool) -> str:
        """Generate field description"""
        if is_custom:
            return f"Custom {field_type} field"

        # Generate descriptions for common fields
        descriptions = {
            'id': 'Unique identifier',
            'name': 'Name or title',
            'created_at': 'Creation date',
            'updated_at': 'Last modification date',
            'status_id': 'Status identifier',
            'responsible_user_id': 'Responsible user ID',
            'price': 'Price or value',
            'pipeline_id': 'Pipeline identifier',
            'stage_id': 'Stage identifier',
            'tags': 'Associated tags',
            'contact': 'Contact information',
            'company': 'Company information',
        }

        for key, desc in descriptions.items():
            if key in field_name.lower():
                return desc

        return f"{field_type.capitalize()} field"

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache is still valid"""
        last_update = self.last_cache_update.get(cache_key)
        if not last_update:
            return False

        return datetime.now() - last_update < self.cache_ttl

    async def save_export_settings(self, settings: ExportSettings) -> str:
        """Save export settings to database"""
        try:
            settings.updated_at = datetime.now()
            if not settings.created_at:
                settings.created_at = settings.updated_at

            # Convert to dict for MongoDB
            settings_dict = settings.to_dict()

            # Insert or update
            result = self.settings_collection.insert_one(settings_dict)
            settings_id = str(result.inserted_id)

            log_event("export_settings", "info", f"Saved export settings: {settings_id}")
            return settings_id

        except Exception as e:
            log_event("export_settings", "error", f"Error saving export settings: {e}")
            raise

    async def load_export_settings(self, settings_id: str) -> Optional[ExportSettings]:
        """Load export settings from database"""
        try:
            doc = self.settings_collection.find_one({"_id": ObjectId(settings_id)})
            if not doc:
                return None

            # Convert from MongoDB document
            doc.pop('_id', None)
            if 'created_at' in doc and isinstance(doc['created_at'], str):
                doc['created_at'] = datetime.fromisoformat(doc['created_at'])
            if 'updated_at' in doc and isinstance(doc['updated_at'], str):
                doc['updated_at'] = datetime.fromisoformat(doc['updated_at'])

            return ExportSettings(**doc)

        except Exception as e:
            log_event("export_settings", "error", f"Error loading export settings {settings_id}: {e}")
            return None

    async def list_export_settings(self, entity_type: Optional[EntityType] = None) -> List[Dict[str, Any]]:
        """List all saved export settings"""
        try:
            query = {}
            if entity_type:
                query['entity_type'] = entity_type.value

            cursor = self.settings_collection.find(query).sort('updated_at', -1)
            settings_list = []

            for doc in cursor:
                doc['_id'] = str(doc['_id'])
                settings_list.append(doc)

            return settings_list

        except Exception as e:
            log_event("export_settings", "error", f"Error listing export settings: {e}")
            return []

    async def delete_export_settings(self, settings_id: str) -> bool:
        """Delete export settings"""
        try:
            result = self.settings_collection.delete_one({"_id": ObjectId(settings_id)})
            success = result.deleted_count > 0

            if success:
                log_event("export_settings", "info", f"Deleted export settings: {settings_id}")
            else:
                log_event("export_settings", "warning", f"Export settings not found: {settings_id}")

            return success

        except Exception as e:
            log_event("export_settings", "error", f"Error deleting export settings {settings_id}: {e}")
            return False

    async def get_field_preview_data(self, entity_type: EntityType, field_name: str, limit: int = 10) -> List[str]:
        """Get preview data for a specific field"""
        cache_key = f"{entity_type}_{field_name}_preview"

        # Check cache
        if self._is_cache_valid(cache_key):
            cached_preview = self.preview_cache.get(cache_key, [])
            if cached_preview:
                return cached_preview

        try:
            collection_name = self.entity_collections.get(entity_type)
            if not collection_name:
                return []

            collection = self.db[collection_name]

            # Get sample documents
            sample_docs = list(collection.find().limit(limit * 2))
            preview_data = []

            for doc in sample_docs:
                value = self._get_nested_value(doc, field_name)
                if value is not None:
                    formatted_value = self._format_value_for_preview(value)
                    if formatted_value and formatted_value not in preview_data:
                        preview_data.append(formatted_value)

                if len(preview_data) >= limit:
                    break

            # Cache the results
            self.preview_cache[cache_key] = preview_data
            self.last_cache_update[cache_key] = datetime.now()

            return preview_data

        except Exception as e:
            log_event("export_settings", "error", f"Error getting preview data for {field_name}: {e}")
            return []

    async def clear_cache(self):
        """Clear all cached data"""
        self.fields_cache.clear()
        self.preview_cache.clear()
        self.last_cache_update.clear()
        log_event("export_settings", "info", "Cache cleared")

    async def get_field_statistics(self, entity_type: EntityType, field_name: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """Get statistics for a specific field with configurable limits"""
        try:
            collection_name = self.entity_collections.get(entity_type)
            if not collection_name:
                raise ValueError(f"Unknown entity type: {entity_type}")

            # Use configured limit if not specified
            if limit is None:
                limit = config.settings.max_sampling_documents

            return self.data_enricher.get_field_statistics(collection_name, field_name, limit)
        except Exception as e:
            log_event("export_settings", "error", f"Error getting field statistics for {entity_type}.{field_name}: {e}")
            return {"total": 0, "filled": 0, "fill_percentage": 0, "unique_values": 0}

    async def get_smart_sample(self, entity_type: EntityType, sample_size: int = 10) -> List[Dict[str, Any]]:
        """Get a smart sample of entities with good field coverage with configurable limits"""
        try:
            collection_name = self.entity_collections.get(entity_type)
            if not collection_name:
                raise ValueError(f"Unknown entity type: {entity_type}")

            # Ensure sample size doesn't exceed configured limits
            max_sample_size = config.settings.max_sample_size
            if sample_size > max_sample_size:
                log_event("export_settings", "warning", f"Sample size {sample_size} exceeds limit {max_sample_size}, using limit")
                sample_size = max_sample_size

            return self.data_enricher.get_smart_sample(collection_name, sample_size)
        except Exception as e:
            log_event("export_settings", "error", f"Error getting smart sample for {entity_type}: {e}")
            return []

    async def get_enriched_sample(self, entity_type: EntityType, sample_size: int = 10,
                                flatten_fields: bool = False, enrich_users: bool = True,
                                enrich_pipelines: bool = True) -> List[Dict[str, Any]]:
        """Get a smart sample of entities with enrichment options"""
        try:
            collection_name = self.entity_collections.get(entity_type)
            if not collection_name:
                raise ValueError(f"Unknown entity type: {entity_type}")

            sample_entities = self.data_enricher.get_smart_sample(collection_name, sample_size)

            if sample_entities:
                enriched_entities = self.data_enricher.process_entities_batch(
                    sample_entities,
                    collection_name,
                    flatten_fields=flatten_fields,
                    enrich_users=enrich_users,
                    enrich_pipelines=enrich_pipelines
                )
                return enriched_entities

            return []
        except Exception as e:
            log_event("export_settings", "error", f"Error getting enriched sample for {entity_type}: {e}")
            return []

    async def get_all_field_statistics(self, entity_type: EntityType) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all fields in an entity type using optimized parallel aggregation"""
        try:
            # Use optimized storage method if available
            if hasattr(self.storage, 'get_all_field_statistics_optimized'):
                collection_name = self.entity_collections.get(entity_type)
                if collection_name:
                    statistics = self.storage.get_all_field_statistics_optimized(collection_name)
                    if statistics:
                        log_event("export_settings", "info", f"Got optimized field statistics for {entity_type}")
                        return statistics

            # Fallback to parallel method
            return await self.get_all_field_statistics_parallel(entity_type)

        except Exception as e:
            log_event("export_settings", "error", f"Error getting all field statistics for {entity_type}: {e}")
            return {}

    async def get_all_field_statistics_parallel(self, entity_type: EntityType) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all fields using parallel processing"""
        try:
            fields = await self.get_available_fields(entity_type)
            if not fields:
                return {}

            statistics = {}
            batch_size = config.settings.sampling_batch_size

            # Process fields in parallel batches
            for i in range(0, len(fields), batch_size):
                batch_fields = fields[i:i+batch_size]

                # Create tasks for this batch
                tasks = []
                for field in batch_fields:
                    task = asyncio.create_task(self.get_field_statistics(entity_type, field.field_id))
                    tasks.append((field.field_id, task))

                # Wait for all tasks in this batch to complete
                for field_id, task in tasks:
                    try:
                        field_stats = await task
                        statistics[field_id] = field_stats
                    except Exception as e:
                        log_event("export_settings", "error", f"Error getting statistics for field {field_id}: {e}")
                        statistics[field_id] = {"total": 0, "filled": 0, "fill_percentage": 0, "unique_values": 0}

                # Small delay between batches to avoid overwhelming the database
                if i + batch_size < len(fields):
                    await asyncio.sleep(0.1)

            log_event("export_settings", "info", f"Completed parallel field statistics for {entity_type} ({len(statistics)} fields)")
            return statistics

        except Exception as e:
            log_event("export_settings", "error", f"Error in parallel field statistics for {entity_type}: {e}")
            return {}

    async def get_top_filled_fields(self, entity_type: EntityType, limit: int = 20) -> List[Dict[str, Any]]:
        """Get fields with highest fill percentage"""
        try:
            all_stats = await self.get_all_field_statistics(entity_type)

            # Sort by fill percentage
            sorted_fields = sorted(
                all_stats.items(),
                key=lambda x: x[1].get('fill_percentage', 0),
                reverse=True
            )

            return [
                {
                    'field_name': field_name,
                    'fill_percentage': stats.get('fill_percentage', 0),
                    'unique_values': stats.get('unique_values', 0),
                    'total': stats.get('total', 0),
                    'filled': stats.get('filled', 0)
                }
                for field_name, stats in sorted_fields[:limit]
            ]
        except Exception as e:
            log_event("export_settings", "error", f"Error getting top filled fields for {entity_type}: {e}")
            return []

    async def create_smart_export_settings(self, entity_type: EntityType, name: str,
                                         min_fill_percentage: float = 50.0,
                                         max_fields: int = 50) -> Optional[ExportSettings]:
        """Create export settings based on field statistics"""
        try:
            top_fields = await self.get_top_filled_fields(entity_type, max_fields)

            # Filter by minimum fill percentage
            selected_fields = [
                field['field_name'] for field in top_fields
                if field['fill_percentage'] >= min_fill_percentage
            ]

            if not selected_fields:
                log_event("export_settings", "warning", f"No fields found with {min_fill_percentage}% fill rate for {entity_type}")
                return None

            # Create export settings
            settings = ExportSettings(
                entity_type=entity_type.value,
                selected_fields=selected_fields,
                field_order=selected_fields,
                filters={},
                name=name,
                description=f"Auto-generated settings with {min_fill_percentage}% minimum fill rate",
                created_at=datetime.now(),
                updated_at=datetime.now()
            )

            return settings
        except Exception as e:
            log_event("export_settings", "error", f"Error creating smart export settings for {entity_type}: {e}")
            return None