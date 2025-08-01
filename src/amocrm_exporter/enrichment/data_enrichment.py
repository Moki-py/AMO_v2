"""
Data enrichment module for AmoCRM data
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import asyncio

from ..core import config
from ..storage.storage import Storage
from ..core.logger import log_event
from ..storage.cache_manager import get_cache_manager


class DataEnricher:
    """Class for enriching entities with user and pipeline information"""

    def __init__(self, storage: Storage):
        """Initialize the data enricher with storage backend"""
        self.storage = storage
        self.cache_manager = get_cache_manager()
        self._user_cache: Dict[int, Dict[str, Any]] = {}
        self._pipeline_cache: Dict[int, Dict[str, Any]] = {}
        self._custom_fields_cache: Dict[str, Dict[int, Dict[str, Any]]] = {}
        self._cache_timestamp = datetime.now()
        self._cache_ttl = timedelta(hours=1)  # Refresh cache every hour

    def _should_refresh_cache(self) -> bool:
        """Check if cache should be refreshed"""
        return datetime.now() - self._cache_timestamp > self._cache_ttl

    def _refresh_user_cache(self):
        """Refresh user cache from storage"""
        try:
            users = self.storage.get_entities("users")
            self._user_cache = {user["id"]: user for user in users if "id" in user}
            log_event("enricher", "info", f"Refreshed user cache with {len(self._user_cache)} users")
        except Exception as e:
            log_event("enricher", "error", f"Error refreshing user cache: {e}")

    def _refresh_pipeline_cache(self):
        """Refresh pipeline cache from storage"""
        try:
            pipelines = self.storage.get_entities("pipelines")
            self._pipeline_cache = {pipeline["id"]: pipeline for pipeline in pipelines if "id" in pipeline}
            log_event("enricher", "info", f"Refreshed pipeline cache with {len(self._pipeline_cache)} pipelines")
        except Exception as e:
            log_event("enricher", "error", f"Error refreshing pipeline cache: {e}")

    def _refresh_custom_fields_cache(self):
        """Refresh custom fields cache from Redis, MongoDB, or API (in that order)"""
        try:
            # First, try to get from Redis cache
            cached_all_fields = self.cache_manager.get_all_custom_fields()
            if cached_all_fields:
                log_event("enricher", "info", "Using custom fields from Redis cache")
                self._build_in_memory_cache_from_all_fields(cached_all_fields)
                return

            # Try to get from MongoDB storage if Redis cache miss
            custom_fields_collection = self.storage.db.get_collection("custom_fields")
            stored_fields = list(custom_fields_collection.find())

            # Check if we have fresh data (less than 24 hours old)
            should_fetch_from_api = True
            if stored_fields:
                latest_update = max(
                    (field.get("fetched_at", datetime.min) for field in stored_fields),
                    default=datetime.min
                )
                if isinstance(latest_update, str):
                    latest_update = datetime.fromisoformat(latest_update)

                if datetime.now() - latest_update < timedelta(hours=24):
                    should_fetch_from_api = False
                    log_event("enricher", "info", "Using custom fields from MongoDB storage")

            if should_fetch_from_api:
                # Fetch fresh data from API
                log_event("enricher", "info", "Fetching custom fields from API")
                try:
                    from ..core.api import AmoCRMAPI
                    api = AmoCRMAPI()
                    all_custom_fields = api.get_all_custom_fields()

                    # Store in MongoDB with timestamp
                    custom_fields_collection.delete_many({})  # Clear old data

                    for entity_type, fields in all_custom_fields.items():
                        for field in fields:
                            field["entity_type"] = entity_type
                            field["fetched_at"] = datetime.now().isoformat()
                        if fields:
                            custom_fields_collection.insert_many(fields)

                    # Store in Redis cache for fast future access
                    self.cache_manager.set_all_custom_fields(all_custom_fields)

                    # Also cache individual entity type mappings
                    for entity_type, fields in all_custom_fields.items():
                        # Cache raw fields for entity type
                        self.cache_manager.set_custom_fields(entity_type, fields)

                        # Cache mapping (field_id -> field_info) for easy lookup
                        mapping = {str(field.get("id")): field for field in fields if field.get("id")}
                        self.cache_manager.set_custom_fields_mapping(entity_type, mapping)

                    stored_fields = list(custom_fields_collection.find())
                    log_event("enricher", "info", f"Updated custom fields cache from API with {len(stored_fields)} fields")

                except Exception as e:
                    log_event("enricher", "warning", f"Failed to fetch from API, using stored data: {e}")

            # If we have stored fields but no API data, rebuild all_custom_fields and cache it
            if stored_fields and not cached_all_fields:
                all_custom_fields = {}
                for field in stored_fields:
                    entity_type = field.get("entity_type", "unknown")
                    if entity_type not in all_custom_fields:
                        all_custom_fields[entity_type] = []
                    all_custom_fields[entity_type].append(field)

                # Store in Redis cache
                self.cache_manager.set_all_custom_fields(all_custom_fields)

                # Cache individual entity mappings
                for entity_type, fields in all_custom_fields.items():
                    self.cache_manager.set_custom_fields(entity_type, fields)
                    mapping = {str(field.get("id")): field for field in fields if field.get("id")}
                    self.cache_manager.set_custom_fields_mapping(entity_type, mapping)

            # Build in-memory cache from stored data
            self._custom_fields_cache = {}
            for field in stored_fields:
                entity_type = field.get("entity_type", "unknown")
                field_id = field.get("id")

                if entity_type not in self._custom_fields_cache:
                    self._custom_fields_cache[entity_type] = {}

                if field_id:
                    self._custom_fields_cache[entity_type][field_id] = field

            log_event("enricher", "info", f"Built custom fields cache: {sum(len(fields) for fields in self._custom_fields_cache.values())} total fields")

        except Exception as e:
            log_event("enricher", "error", f"Error refreshing custom fields cache: {e}")
            self._custom_fields_cache = {}

    def _build_in_memory_cache_from_all_fields(self, all_custom_fields: Dict[str, List[Dict[str, Any]]]) -> None:
        """Build in-memory cache from all_custom_fields structure"""
        try:
            self._custom_fields_cache = {}
            for entity_type, fields in all_custom_fields.items():
                if entity_type not in self._custom_fields_cache:
                    self._custom_fields_cache[entity_type] = {}

                for field in fields:
                    field_id = field.get("id")
                    if field_id:
                        self._custom_fields_cache[entity_type][field_id] = field

            log_event("enricher", "info", f"Built in-memory cache from Redis: {sum(len(fields) for fields in self._custom_fields_cache.values())} total fields")
        except Exception as e:
            log_event("enricher", "error", f"Error building in-memory cache from Redis data: {e}")
            self._custom_fields_cache = {}

    def invalidate_custom_fields_cache(self, entity_type: Optional[str] = None) -> None:
        """Invalidate custom fields cache in Redis and local memory"""
        try:
            # Invalidate Redis cache
            self.cache_manager.invalidate_custom_fields_cache(entity_type)

            # Clear local cache
            if entity_type:
                if entity_type in self._custom_fields_cache:
                    del self._custom_fields_cache[entity_type]
                    log_event("enricher", "info", f"Cleared local custom fields cache for {entity_type}")
            else:
                self._custom_fields_cache.clear()
                log_event("enricher", "info", "Cleared all local custom fields cache")

            # Force cache refresh on next access
            self._cache_timestamp = datetime.min

        except Exception as e:
            log_event("enricher", "error", f"Error invalidating custom fields cache: {e}")

    def get_custom_field_from_cache(self, entity_type: str, field_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific custom field from Redis cache (bypassing local cache)"""
        try:
            # Try to get from Redis mapping first
            mapping = self.cache_manager.get_custom_fields_mapping(entity_type)
            if mapping and str(field_id) in mapping:
                return mapping[str(field_id)]

            # Fallback to checking if we have all fields cached
            all_fields = self.cache_manager.get_all_custom_fields()
            if all_fields and entity_type in all_fields:
                for field in all_fields[entity_type]:
                    if field.get("id") == field_id:
                        return field

            return None

        except Exception as e:
            log_event("enricher", "error", f"Error getting custom field {field_id} for {entity_type} from cache: {e}")
            return None

    def refresh_custom_fields_from_api(self, entity_type: Optional[str] = None) -> None:
        """Force refresh custom fields from API and update all caches"""
        try:
            from ..core.api import AmoCRMAPI
            api = AmoCRMAPI()

            if entity_type:
                # Refresh specific entity type
                log_event("enricher", "info", f"Force refreshing custom fields for {entity_type} from API")

                # Invalidate existing cache
                self.invalidate_custom_fields_cache(entity_type)

                # Fetch fresh data
                custom_fields = api.get_custom_fields(entity_type, use_cache=False)

                # Update cache
                self.cache_manager.set_custom_fields(entity_type, custom_fields)
                mapping = {str(field.get("id")): field for field in custom_fields if field.get("id")}
                self.cache_manager.set_custom_fields_mapping(entity_type, mapping)

                log_event("enricher", "info", f"Refreshed {len(custom_fields)} custom fields for {entity_type}")

            else:
                # Refresh all entity types
                log_event("enricher", "info", "Force refreshing all custom fields from API")

                # Invalidate existing cache
                self.invalidate_custom_fields_cache()

                # Fetch fresh data
                all_custom_fields = api.get_all_custom_fields(use_cache=False)

                # Update cache
                self.cache_manager.set_all_custom_fields(all_custom_fields)

                for entity_type, fields in all_custom_fields.items():
                    self.cache_manager.set_custom_fields(entity_type, fields)
                    mapping = {str(field.get("id")): field for field in fields if field.get("id")}
                    self.cache_manager.set_custom_fields_mapping(entity_type, mapping)

                total_fields = sum(len(fields) for fields in all_custom_fields.values())
                log_event("enricher", "info", f"Refreshed {total_fields} total custom fields")

            # Refresh local cache
            self._refresh_custom_fields_cache()

        except Exception as e:
            log_event("enricher", "error", f"Error force refreshing custom fields from API: {e}")

    def get_custom_fields_cache_status(self) -> Dict[str, Any]:
        """Get status information about custom fields caching"""
        try:
            cache_stats = {
                "redis_connected": self.cache_manager.is_connected,
                "local_cache_entities": list(self._custom_fields_cache.keys()),
                "local_cache_total_fields": sum(len(fields) for fields in self._custom_fields_cache.values()),
                "cache_timestamp": self._cache_timestamp.isoformat(),
                "cache_age_hours": (datetime.now() - self._cache_timestamp).total_seconds() / 3600,
                "should_refresh": self._should_refresh_cache()
            }

            # Check Redis cache status
            if self.cache_manager.is_connected:
                redis_all_fields = self.cache_manager.get_all_custom_fields()
                cache_stats["redis_has_all_fields"] = redis_all_fields is not None
                if redis_all_fields:
                    cache_stats["redis_total_fields"] = sum(len(fields) for fields in redis_all_fields.values())
                    cache_stats["redis_entity_types"] = list(redis_all_fields.keys())
                else:
                    cache_stats["redis_total_fields"] = 0
                    cache_stats["redis_entity_types"] = []

                # Check individual entity caches
                entity_types = ["leads", "contacts", "companies"]
                cache_stats["redis_entity_status"] = {}
                for entity_type in entity_types:
                    fields = self.cache_manager.get_custom_fields(entity_type)
                    mapping = self.cache_manager.get_custom_fields_mapping(entity_type)
                    cache_stats["redis_entity_status"][entity_type] = {
                        "has_fields": fields is not None,
                        "fields_count": len(fields) if fields else 0,
                        "has_mapping": mapping is not None,
                        "mapping_count": len(mapping) if mapping else 0
                    }
            else:
                cache_stats["redis_has_all_fields"] = False
                cache_stats["redis_total_fields"] = 0
                cache_stats["redis_entity_types"] = []
                cache_stats["redis_entity_status"] = {}

            return cache_stats

        except Exception as e:
            log_event("enricher", "error", f"Error getting custom fields cache status: {e}")
            return {"error": str(e)}

    def get_custom_field_info(self, entity_type: str, field_id: int) -> Optional[Dict[str, Any]]:
        """Get custom field information by entity type and field ID"""
        if self._should_refresh_cache():
            self._refresh_custom_fields_cache()

        return self._custom_fields_cache.get(entity_type, {}).get(field_id)

    def get_custom_field_name(self, entity_type: str, field_id: int) -> str:
        """Get human-readable name for a custom field"""
        field_info = self.get_custom_field_info(entity_type, field_id)
        if field_info:
            return field_info.get("name", f"Custom Field {field_id}")
        return f"Custom Field {field_id}"

    def get_custom_field_mapping(self, entity_type: str) -> Dict[int, str]:
        """Get mapping of field IDs to names for an entity type"""
        if self._should_refresh_cache():
            self._refresh_custom_fields_cache()

        fields = self._custom_fields_cache.get(entity_type, {})
        return {field_id: field_info.get("name", f"Custom Field {field_id}")
                for field_id, field_info in fields.items()}

    def get_user_info(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user information by ID"""
        # Сначала проверяем Redis кэш
        cached_user = self.cache_manager.get_user_data(user_id)
        if cached_user is not None:
            return cached_user

        # Если нет в Redis, проверяем локальный кэш
        if self._should_refresh_cache():
            self._refresh_user_cache()

        user_data = self._user_cache.get(user_id)

        # Сохраняем в Redis кэш для будущих запросов
        if user_data is not None:
            self.cache_manager.set_user_data(user_id, user_data)

        return user_data

    def get_pipeline_info(self, pipeline_id: int) -> Optional[Dict[str, Any]]:
        """Get pipeline information by ID"""
        # Сначала проверяем Redis кэш
        cached_pipeline = self.cache_manager.get_pipeline_data(pipeline_id)
        if cached_pipeline is not None:
            return cached_pipeline

        # Если нет в Redis, проверяем локальный кэш
        if self._should_refresh_cache():
            self._refresh_pipeline_cache()

        pipeline_data = self._pipeline_cache.get(pipeline_id)

        # Сохраняем в Redis кэш для будущих запросов
        if pipeline_data is not None:
            self.cache_manager.set_pipeline_data(pipeline_id, pipeline_data)

        return pipeline_data

    def enrich_entity_with_user(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single entity with user information"""
        enriched_entity = entity.copy()

        # Common user fields to enrich
        user_fields = [
            "responsible_user_id",
            "created_by",
            "updated_by",
            "closest_task_at",
            "account_id"
        ]

        for field in user_fields:
            if field in entity and entity[field]:
                user_info = self.get_user_info(entity[field])
                if user_info:
                    enriched_entity[f"{field}_name"] = user_info.get("name", "")
                    enriched_entity[f"{field}_email"] = user_info.get("email", "")
                    enriched_entity[f"{field}_lang"] = user_info.get("lang", "")
                    enriched_entity[f"{field}_group_id"] = user_info.get("group_id", "")

        return enriched_entity

    def enrich_entity_with_pipeline(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single entity with pipeline information"""
        enriched_entity = entity.copy()

        # Enrich pipeline information for leads
        if "pipeline_id" in entity and entity["pipeline_id"]:
            pipeline_info = self.get_pipeline_info(entity["pipeline_id"])
            if pipeline_info:
                enriched_entity["pipeline_name"] = pipeline_info.get("name", "")
                enriched_entity["pipeline_sort"] = pipeline_info.get("sort", 0)
                enriched_entity["pipeline_is_main"] = pipeline_info.get("is_main", False)
                enriched_entity["pipeline_is_unsorted_on"] = pipeline_info.get("is_unsorted_on", False)
                enriched_entity["pipeline_is_archive"] = pipeline_info.get("is_archive", False)
                enriched_entity["pipeline_account_id"] = pipeline_info.get("account_id", "")

                # Enrich status information
                if "status_id" in entity and entity["status_id"] and "statuses" in pipeline_info:
                    status_info = next(
                        (status for status in pipeline_info["statuses"] if status["id"] == entity["status_id"]),
                        None
                    )
                    if status_info:
                        enriched_entity["status_name"] = status_info.get("name", "")
                        enriched_entity["status_color"] = status_info.get("color", "")
                        enriched_entity["status_type"] = status_info.get("type", "")
                        enriched_entity["status_sort"] = status_info.get("sort", 0)

        return enriched_entity

    def enrich_entities_with_users(self, entities: List[Dict[str, Any]], entity_type: str) -> List[Dict[str, Any]]:
        """Enrich multiple entities with user information"""
        if not entities:
            return []

        try:
            enriched_entities = []
            for entity in entities:
                enriched_entity = self.enrich_entity_with_user(entity)
                enriched_entities.append(enriched_entity)

            log_event("enricher", "info", f"Enriched {len(enriched_entities)} {entity_type} with user data")
            return enriched_entities
        except Exception as e:
            log_event("enricher", "error", f"Error enriching {entity_type} with user data: {e}")
            return entities

    def enrich_entities_with_pipelines(self, entities: List[Dict[str, Any]], entity_type: str) -> List[Dict[str, Any]]:
        """Enrich multiple entities with pipeline information"""
        if not entities:
            return []

        try:
            enriched_entities = []
            for entity in entities:
                enriched_entity = self.enrich_entity_with_pipeline(entity)
                enriched_entities.append(enriched_entity)

            log_event("enricher", "info", f"Enriched {len(enriched_entities)} {entity_type} with pipeline data")
            return enriched_entities
        except Exception as e:
            log_event("enricher", "error", f"Error enriching {entity_type} with pipeline data: {e}")
            return entities

    def enrich_entities_full(self, entities: List[Dict[str, Any]], entity_type: str) -> List[Dict[str, Any]]:
        """Enrich entities with both user and pipeline information"""
        if not entities:
            return []

        try:
            enriched_entities = []
            for entity in entities:
                enriched_entity = self.enrich_entity_with_user(entity)
                enriched_entity = self.enrich_entity_with_pipeline(enriched_entity)
                enriched_entities.append(enriched_entity)

            log_event("enricher", "info", f"Fully enriched {len(enriched_entities)} {entity_type}")
            return enriched_entities
        except Exception as e:
            log_event("enricher", "error", f"Error fully enriching {entity_type}: {e}")
            return entities

    def flatten_custom_fields(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten custom fields structure for better data sampling with advanced multiselect and entity support"""
        flattened_entity = entity.copy()

        if "custom_fields_values" in entity and entity["custom_fields_values"]:
            # Determine entity type for field mapping
            entity_type = self._determine_entity_type(entity)
            field_mapping = self.get_custom_field_mapping(entity_type) if entity_type else {}

            for field in entity["custom_fields_values"]:
                if "field_id" in field and "values" in field:
                    field_id = field["field_id"]
                    values = field["values"]
                    field_name = field.get("field_name")

                    # Prioritize field_name over any mapping or field_id
                    if field_name:
                        # Use the actual field_name from the data (most reliable)
                        readable_name = field_name
                    elif field_id in field_mapping:
                        # Fallback to cached mapping
                        readable_name = field_mapping[field_id]
                    else:
                        # Last resort fallback
                        readable_name = f"Custom Field {field_id}"

                    # Handle different value types
                    if values:
                        flattened_values = self._flatten_field_values(values, field_id, readable_name)
                        flattened_entity.update(flattened_values)

        # Also handle embedded custom fields in other structures
        flattened_entity = self._flatten_embedded_structures(flattened_entity)

        return flattened_entity

    def _determine_entity_type(self, entity: Dict[str, Any]) -> Optional[str]:
        """Determine the entity type from the entity data"""
        # Check for entity type indicators
        if "price" in entity or "pipeline_id" in entity:
            return "leads"
        elif "company_name" in entity or "company" in entity:
            return "companies"
        elif "first_name" in entity or "last_name" in entity:
            return "contacts"

        # Fallback: check for common patterns
        if any(key in entity for key in ["responsible_user_id", "created_by", "updated_by"]):
            # All entities have these, try to guess from structure
            if "status_id" in entity:
                return "leads"  # Most likely leads have status_id
            elif "name" in entity and len(entity.get("name", "")) > 50:
                return "companies"  # Companies usually have longer names
            else:
                return "contacts"  # Default fallback

        return None

    def _flatten_field_values(self, values: List[Dict[str, Any]], field_id: str, field_name: str) -> Dict[str, Any]:
        """Flatten field values handling different AmoCRM field types"""
        flattened = {}

        # Create a sanitized column name from field_name
        if field_name and field_name != f"Custom Field {field_id}":
            # Sanitize field_name for use as column name
            sanitized_name = field_name.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').replace('\\', '_').replace('[', '').replace(']', '').replace('-', '_').lower()
            primary_key = sanitized_name
        else:
            # Fallback to field_id based naming
            primary_key = f"custom_field_{field_id}"

        if len(values) == 1:
            # Single value - handle different types
            value = values[0]
            flattened_value = self._extract_value_from_field(value)

            # Store with user-friendly name as primary
            flattened[primary_key] = flattened_value
            # Also store with field_id for compatibility (if different from primary)
            if primary_key != f"custom_field_{field_id}":
                flattened[f"custom_field_{field_id}"] = flattened_value

        else:
            # Multiple values - handle as multiselect/array
            all_values = []
            for i, value in enumerate(values):
                flattened_value = self._extract_value_from_field(value)
                all_values.append(flattened_value)

                # Store individual values for searchability
                flattened[f"{primary_key}_{i}"] = flattened_value
                # Also store with field_id for compatibility (if different from primary)
                if primary_key != f"custom_field_{field_id}":
                    flattened[f"custom_field_{field_id}_{i}"] = flattened_value

            # Store as concatenated string for easy searching
            flattened[f"{primary_key}_all"] = "; ".join(str(v) for v in all_values if v)
            if primary_key != f"custom_field_{field_id}":
                flattened[f"custom_field_{field_id}_all"] = "; ".join(str(v) for v in all_values if v)

            # Store count
            flattened[f"{primary_key}_count"] = len(all_values)

        return flattened

    def _extract_value_from_field(self, value: Dict[str, Any]) -> Any:
        """Extract the actual value from AmoCRM field value structure"""
        if not isinstance(value, dict):
            return value

        # Handle different AmoCRM field types
        if "value" in value:
            # Standard value field
            extracted = value["value"]

            # If value is a dict with additional info, try to get meaningful data
            if isinstance(extracted, dict):
                if "name" in extracted:
                    return extracted["name"]
                elif "title" in extracted:
                    return extracted["title"]
                elif "text" in extracted:
                    return extracted["text"]
                elif "id" in extracted:
                    # For entity references, include both ID and name if available
                    name_part = extracted.get("name", extracted.get("title", ""))
                    if name_part:
                        return f"{name_part} (ID: {extracted['id']})"
                    else:
                        return extracted["id"]
                else:
                    return str(extracted)
            else:
                return extracted

        elif "text" in value:
            # Text field
            return value["text"]

        elif "enum_id" in value:
            # Enum/select field
            enum_id = value["enum_id"]
            enum_code = value.get("enum_code", "")
            if enum_code:
                return f"{enum_code} (ID: {enum_id})"
            else:
                return enum_id

        elif "file" in value:
            # File field
            file_info = value["file"]
            if isinstance(file_info, dict):
                name = file_info.get("name", "")
                size = file_info.get("size", 0)
                return f"{name} ({size} bytes)" if name else f"File ({size} bytes)"
            else:
                return str(file_info)

        elif "url" in value:
            # URL field
            return value["url"]

        elif "phone" in value:
            # Phone field
            return value["phone"]

        elif "email" in value:
            # Email field
            return value["email"]

        else:
            # Generic fallback - try to find any meaningful text
            for key in ["name", "title", "text", "label", "description"]:
                if key in value and value[key]:
                    return value[key]

            # If nothing found, convert to string
            return str(value)

    def _flatten_embedded_structures(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten embedded structures like tags, contacts, companies links"""
        flattened = entity.copy()

        # Handle tags
        if "tags" in entity and isinstance(entity["tags"], list):
            tag_names = []
            tag_ids = []
            for tag in entity["tags"]:
                if isinstance(tag, dict):
                    if "name" in tag:
                        tag_names.append(tag["name"])
                    if "id" in tag:
                        tag_ids.append(str(tag["id"]))
                else:
                    tag_names.append(str(tag))

            if tag_names:
                flattened["tags_names"] = "; ".join(tag_names)
                flattened["tags_count"] = len(tag_names)
            if tag_ids:
                flattened["tags_ids"] = "; ".join(tag_ids)

        # Handle embedded contacts
        if "_embedded" in entity and "contacts" in entity["_embedded"]:
            contacts = entity["_embedded"]["contacts"]
            if isinstance(contacts, list):
                contact_names = []
                contact_ids = []
                for contact in contacts:
                    if isinstance(contact, dict):
                        if "name" in contact:
                            contact_names.append(contact["name"])
                        if "id" in contact:
                            contact_ids.append(str(contact["id"]))

                if contact_names:
                    flattened["embedded_contacts_names"] = "; ".join(contact_names)
                    flattened["embedded_contacts_count"] = len(contact_names)
                if contact_ids:
                    flattened["embedded_contacts_ids"] = "; ".join(contact_ids)

        # Handle embedded companies
        if "_embedded" in entity and "companies" in entity["_embedded"]:
            companies = entity["_embedded"]["companies"]
            if isinstance(companies, list):
                company_names = []
                company_ids = []
                for company in companies:
                    if isinstance(company, dict):
                        if "name" in company:
                            company_names.append(company["name"])
                        if "id" in company:
                            company_ids.append(str(company["id"]))

                if company_names:
                    flattened["embedded_companies_names"] = "; ".join(company_names)
                    flattened["embedded_companies_count"] = len(company_names)
                if company_ids:
                    flattened["embedded_companies_ids"] = "; ".join(company_ids)

        return flattened

    def process_entities_batch(self, entities: List[Dict[str, Any]], entity_type: str,
                             flatten_fields: bool = False, enrich_users: bool = True,
                             enrich_pipelines: bool = True) -> List[Dict[str, Any]]:
        """Process a batch of entities with all enrichment options"""
        if not entities:
            return []

        processed_entities = entities.copy()

        # Flatten custom fields if requested
        if flatten_fields:
            processed_entities = [self.flatten_custom_fields(entity) for entity in processed_entities]

        # Enrich with user data if requested
        if enrich_users:
            processed_entities = self.enrich_entities_with_users(processed_entities, entity_type)

        # Enrich with pipeline data if requested (only for leads)
        if enrich_pipelines and entity_type == "leads":
            processed_entities = self.enrich_entities_with_pipelines(processed_entities, entity_type)

        return processed_entities

    def get_field_statistics(self, entity_type: str, field_name: str, limit: int = 1000) -> Dict[str, Any]:
        """Get statistics for a specific field in an entity type using optimized aggregation"""
        try:
            # Use optimized storage method if available
            if hasattr(self.storage, 'get_field_statistics_optimized'):
                return self.storage.get_field_statistics_optimized(entity_type, field_name, limit)

            # Fallback to original method
            entities = self.storage.get_entities(entity_type, limit=limit)

            if not entities:
                return {"total": 0, "filled": 0, "fill_percentage": 0, "unique_values": 0}

            total_count = len(entities)
            filled_count = 0
            unique_values = set()

            for entity in entities:
                if field_name in entity and entity[field_name] is not None:
                    filled_count += 1
                    unique_values.add(str(entity[field_name]))

            fill_percentage = (filled_count / total_count) * 100 if total_count > 0 else 0

            return {
                "total": total_count,
                "filled": filled_count,
                "fill_percentage": round(fill_percentage, 2),
                "unique_values": len(unique_values)
            }
        except Exception as e:
            log_event("enricher", "error", f"Error getting field statistics: {e}")
            return {"total": 0, "filled": 0, "fill_percentage": 0, "unique_values": 0}

    def get_smart_sample(self, entity_type: str, sample_size: int = 10) -> List[Dict[str, Any]]:
        """Get a smart sample of entities with good field coverage using optimized aggregation"""
        try:
            # Use optimized storage method if available
            if hasattr(self.storage, 'get_smart_sample_optimized'):
                sample_entities = self.storage.get_smart_sample_optimized(entity_type, sample_size)
                if sample_entities:
                    log_event("enricher", "info", f"Generated optimized smart sample of {len(sample_entities)} {entity_type}")
                    return sample_entities

            # Fallback to original method
            # Get more entities than needed to select best samples
            entities = self.storage.get_entities(entity_type, limit=sample_size * 5)

            if not entities:
                return []

            # Score entities by field coverage
            scored_entities = []
            for entity in entities:
                field_count = len([k for k, v in entity.items() if v is not None and v != ""])
                scored_entities.append((field_count, entity))

            # Sort by field coverage (descending) and take top samples
            scored_entities.sort(key=lambda x: x[0], reverse=True)
            sample_entities = [entity for _, entity in scored_entities[:sample_size]]

            log_event("enricher", "info", f"Generated smart sample of {len(sample_entities)} {entity_type}")
            return sample_entities
        except Exception as e:
            log_event("enricher", "error", f"Error generating smart sample: {e}")
            return []