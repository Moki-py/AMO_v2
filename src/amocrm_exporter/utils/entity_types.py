"""
Unified entity types system for AmoCRM Data Exporter

This module provides:
- Centralized entity type definitions
- Type validation and normalization
- Pydantic models for type safety
- Mapping functions for different contexts
"""

from enum import Enum
from typing import Dict, List, Optional, Set, Union
from pydantic import BaseModel, Field, validator
from dataclasses import dataclass


class EntityType(str, Enum):
    """Unified entity types for AmoCRM Data Exporter"""

    # Core entity types
    DEALS = "deals"
    LEADS = "leads"  # Alias for deals (AmoCRM internal representation)
    CONTACTS = "contacts"
    COMPANIES = "companies"
    EVENTS = "events"
    USERS = "users"
    PIPELINES = "pipelines"

    # Special types
    ALL = "all"

    @classmethod
    def get_core_types(cls) -> List[str]:
        """Get all core entity types (excluding special types)"""
        return [
            cls.DEALS.value,
            cls.CONTACTS.value,
            cls.COMPANIES.value,
            cls.EVENTS.value,
            cls.USERS.value,
            cls.PIPELINES.value
        ]

    @classmethod
    def get_export_types(cls) -> List[str]:
        """Get entity types that can be exported"""
        return cls.get_core_types() + [cls.ALL.value]

    @classmethod
    def normalize(cls, entity_type: str) -> str:
        """Normalize entity type to internal representation"""
        normalized = entity_type.lower().strip()

        # Handle deals/leads normalization
        if normalized in ["deals", "leads"]:
            return "leads"  # Internal representation

        # Validate against known types
        valid_types = {t.value for t in cls}
        if normalized not in valid_types:
            raise ValueError(f"Invalid entity type: {entity_type}. Valid types: {valid_types}")

        return normalized

    @classmethod
    def to_display_name(cls, entity_type: str) -> str:
        """Convert entity type to display name"""
        normalized = cls.normalize(entity_type)

        display_names = {
            "leads": "deals",  # Display as "deals" for user interface
            "deals": "deals",
            "contacts": "contacts",
            "companies": "companies",
            "events": "events",
            "users": "users",
            "pipelines": "pipelines",
            "all": "all"
        }

        return display_names.get(normalized, normalized)

    @classmethod
    def to_collection_name(cls, entity_type: str) -> str:
        """Convert entity type to MongoDB collection name"""
        normalized = cls.normalize(entity_type)

        collection_mapping = {
            "leads": "deals",  # Store leads as deals collection
            "deals": "deals",
            "contacts": "contacts",
            "companies": "companies",
            "events": "events",
            "users": "users",
            "pipelines": "pipelines",
            "logs": "logs"
        }

        return collection_mapping.get(normalized, normalized)


@dataclass
class EntityTypeInfo:
    """Information about an entity type"""
    name: str
    display_name: str
    collection_name: str
    api_endpoint: str
    supports_export: bool
    supports_import: bool
    has_custom_fields: bool
    description: str


class EntityTypeRegistry:
    """Registry for entity type information"""

    _registry: Dict[str, EntityTypeInfo] = {
        "leads": EntityTypeInfo(
            name="leads",
            display_name="deals",
            collection_name="deals",
            api_endpoint="leads",
            supports_export=True,
            supports_import=True,
            has_custom_fields=True,
            description="Сделки (лиды) из AmoCRM"
        ),
        "contacts": EntityTypeInfo(
            name="contacts",
            display_name="contacts",
            collection_name="contacts",
            api_endpoint="contacts",
            supports_export=True,
            supports_import=True,
            has_custom_fields=True,
            description="Контакты из AmoCRM"
        ),
        "companies": EntityTypeInfo(
            name="companies",
            display_name="companies",
            collection_name="companies",
            api_endpoint="companies",
            supports_export=True,
            supports_import=True,
            has_custom_fields=True,
            description="Компании из AmoCRM"
        ),
        "events": EntityTypeInfo(
            name="events",
            display_name="events",
            collection_name="events",
            api_endpoint="events",
            supports_export=True,
            supports_import=False,
            has_custom_fields=False,
            description="События из AmoCRM"
        ),
        "users": EntityTypeInfo(
            name="users",
            display_name="users",
            collection_name="users",
            api_endpoint="users",
            supports_export=True,
            supports_import=False,
            has_custom_fields=False,
            description="Пользователи AmoCRM"
        ),
        "pipelines": EntityTypeInfo(
            name="pipelines",
            display_name="pipelines",
            collection_name="pipelines",
            api_endpoint="leads/pipelines",
            supports_export=True,
            supports_import=False,
            has_custom_fields=False,
            description="Воронки продаж AmoCRM"
        )
    }

    @classmethod
    def get(cls, entity_type: str) -> Optional[EntityTypeInfo]:
        """Get entity type info"""
        normalized = EntityType.normalize(entity_type)
        return cls._registry.get(normalized)

    @classmethod
    def get_all(cls) -> Dict[str, EntityTypeInfo]:
        """Get all entity type info"""
        return cls._registry.copy()

    @classmethod
    def get_exportable(cls) -> Dict[str, EntityTypeInfo]:
        """Get exportable entity types"""
        return {
            name: info for name, info in cls._registry.items()
            if info.supports_export
        }


class EntityTypeValidator(BaseModel):
    """Pydantic model for entity type validation"""

    entity_type: str = Field(..., description="Entity type to validate")

    @validator("entity_type")
    def validate_entity_type(cls, v):
        """Validate entity type"""
        if not v:
            raise ValueError("Entity type cannot be empty")

        try:
            return EntityType.normalize(v)
        except ValueError as e:
            raise ValueError(str(e))

    @property
    def normalized(self) -> str:
        """Get normalized entity type"""
        return self.entity_type

    @property
    def display_name(self) -> str:
        """Get display name"""
        return EntityType.to_display_name(self.entity_type)

    @property
    def collection_name(self) -> str:
        """Get collection name"""
        return EntityType.to_collection_name(self.entity_type)


class EntityTypeSet(BaseModel):
    """Set of entity types for batch operations"""

    types: List[str] = Field(default_factory=list, description="List of entity types")

    @validator("types")
    def validate_types(cls, v):
        """Validate all entity types"""
        if not v:
            return v

        validated = []
        for entity_type in v:
            try:
                normalized = EntityType.normalize(entity_type)
                validated.append(normalized)
            except ValueError as e:
                raise ValueError(f"Invalid entity type '{entity_type}': {str(e)}")

        return validated

    @property
    def normalized(self) -> List[str]:
        """Get normalized entity types"""
        return self.types

    @property
    def display_names(self) -> List[str]:
        """Get display names"""
        return [EntityType.to_display_name(t) for t in self.types]

    @property
    def collection_names(self) -> List[str]:
        """Get collection names"""
        return [EntityType.to_collection_name(t) for t in self.types]


def validate_entity_type(entity_type: str) -> str:
    """Validate and normalize entity type"""
    validator = EntityTypeValidator(entity_type=entity_type)
    return validator.normalized


def validate_entity_types(entity_types: List[str]) -> List[str]:
    """Validate and normalize multiple entity types"""
    if not entity_types:
        return []

    validator = EntityTypeSet(types=entity_types)
    return validator.normalized


def get_entity_type_info(entity_type: str) -> Optional[EntityTypeInfo]:
    """Get information about an entity type"""
    return EntityTypeRegistry.get(entity_type)


def get_all_entity_types() -> List[str]:
    """Get all valid entity types"""
    return EntityType.get_core_types()


def get_exportable_entity_types() -> List[str]:
    """Get exportable entity types"""
    return list(EntityTypeRegistry.get_exportable().keys())


def is_valid_entity_type(entity_type: str) -> bool:
    """Check if entity type is valid"""
    try:
        EntityType.normalize(entity_type)
        return True
    except ValueError:
        return False


def get_entity_type_mapping() -> Dict[str, str]:
    """Get mapping of entity types to collection names"""
    return {
        entity_type: EntityType.to_collection_name(entity_type)
        for entity_type in EntityType.get_core_types()
    }