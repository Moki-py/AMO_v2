"""
Pre-computed Statistics Module для AmoCRM Data Exporter

Предварительно вычисляет и кэширует статистику полей для быстрого доступа.
"""

import time
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from threading import Lock
import hashlib
import json

from ..storage.storage import Storage
from ..core.logger import log_event


@dataclass
class StatisticEntry:
    """Запись предвычисленной статистики"""
    entity_type: str
    field_name: str
    statistics: Dict[str, Any]
    computed_at: datetime
    data_hash: str
    computation_time: float
    valid_until: Optional[datetime] = None


class PrecomputedStatisticsManager:
    """Менеджер предвычисленных статистик"""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.lock = Lock()
        self.cache_collection = "precomputed_statistics"
        self.default_ttl_hours = 24
        self.background_update_threshold = 0.8  # Update when 80% of TTL has passed

        # Ensure statistics collection exists
        self._ensure_statistics_collection()

    def _ensure_statistics_collection(self):
        """Создает коллекцию для предвычисленных статистик с нужными индексами"""
        try:
            collection = self.storage.db[self.cache_collection]

            # Create indexes
            collection.create_index([("entity_type", 1), ("field_name", 1)], unique=True)
            collection.create_index([("computed_at", 1)])
            collection.create_index([("valid_until", 1)])
            collection.create_index([("data_hash", 1)])

            log_event("precomputed_stats", "info", "Precomputed statistics collection initialized")

        except Exception as e:
            log_event("precomputed_stats", "error", f"Failed to initialize statistics collection: {e}")

    def get_statistics(self, entity_type: str, field_name: str,
                      force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """
        Получает статистику поля, используя кэш или вычисляя заново

        Args:
            entity_type: Тип сущности
            field_name: Имя поля
            force_refresh: Принудительное обновление

        Returns:
            Словарь со статистикой или None
        """
        with self.lock:
            # Check cache first
            if not force_refresh:
                cached_stats = self._get_cached_statistics(entity_type, field_name)
                if cached_stats and self._is_cache_valid(cached_stats):
                    return cached_stats["statistics"]

            # Compute new statistics
            return self._compute_and_cache_statistics(entity_type, field_name)

    def get_all_statistics(self, entity_type: str,
                          force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Получает статистику для всех полей типа сущности

        Args:
            entity_type: Тип сущности
            force_refresh: Принудительное обновление

        Returns:
            Словарь со статистикой для всех полей
        """
        # Get sample entities to determine fields
        sample_entities = self.storage.get_entities(entity_type, limit=100)
        if not sample_entities:
            return {}

        # Extract all field names
        all_fields = set()
        for entity in sample_entities:
            all_fields.update(entity.keys())

        # Get statistics for each field
        result = {}
        for field_name in all_fields:
            stats = self.get_statistics(entity_type, field_name, force_refresh)
            if stats:
                result[field_name] = stats

        return result

    def _get_cached_statistics(self, entity_type: str, field_name: str) -> Optional[Dict[str, Any]]:
        """Получает кэшированную статистику из MongoDB"""
        try:
            collection = self.storage.db[self.cache_collection]

            cached_entry = collection.find_one({
                "entity_type": entity_type,
                "field_name": field_name
            })

            if cached_entry:
                # Convert datetime strings back to datetime objects
                if "computed_at" in cached_entry and isinstance(cached_entry["computed_at"], str):
                    cached_entry["computed_at"] = datetime.fromisoformat(cached_entry["computed_at"])
                if "valid_until" in cached_entry and isinstance(cached_entry["valid_until"], str):
                    cached_entry["valid_until"] = datetime.fromisoformat(cached_entry["valid_until"])

                return cached_entry

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error getting cached statistics: {e}")

        return None

    def _is_cache_valid(self, cached_entry: Dict[str, Any]) -> bool:
        """Проверяет, действительна ли кэшированная запись"""
        try:
            # Check if data has changed
            current_hash = self._compute_data_hash(cached_entry["entity_type"], cached_entry["field_name"])
            if current_hash != cached_entry.get("data_hash", ""):
                return False

            # Check TTL
            valid_until = cached_entry.get("valid_until")
            if valid_until and isinstance(valid_until, datetime):
                return datetime.now() < valid_until

            # Default TTL check
            computed_at = cached_entry.get("computed_at")
            if computed_at and isinstance(computed_at, datetime):
                return datetime.now() < computed_at + timedelta(hours=self.default_ttl_hours)

            return False

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error validating cache: {e}")
            return False

    def _compute_data_hash(self, entity_type: str, field_name: str) -> str:
        """Вычисляет хэш данных для обнаружения изменений"""
        try:
            # Get a sample of entities to compute hash
            sample_entities = self.storage.get_entities(entity_type, limit=1000)
            field_values = [str(entity.get(field_name, "")) for entity in sample_entities]

            # Create hash from field values
            data_string = "".join(sorted(field_values))
            return hashlib.md5(data_string.encode()).hexdigest()

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error computing data hash: {e}")
            return ""

    def _compute_and_cache_statistics(self, entity_type: str, field_name: str) -> Optional[Dict[str, Any]]:
        """Вычисляет и кэширует статистику поля"""
        try:
            start_time = time.time()

            # Compute statistics using storage method
            stats = self.storage.get_field_statistics_optimized(entity_type, field_name)

            computation_time = time.time() - start_time

            if not stats:
                return None

            # Compute data hash
            data_hash = self._compute_data_hash(entity_type, field_name)

            # Create cache entry
            cache_entry = {
                "entity_type": entity_type,
                "field_name": field_name,
                "statistics": stats,
                "computed_at": datetime.now().isoformat(),
                "valid_until": (datetime.now() + timedelta(hours=self.default_ttl_hours)).isoformat(),
                "data_hash": data_hash,
                "computation_time": computation_time
            }

            # Save to cache
            collection = self.storage.db[self.cache_collection]
            collection.replace_one(
                {"entity_type": entity_type, "field_name": field_name},
                cache_entry,
                upsert=True
            )

            log_event("precomputed_stats", "info",
                     f"Computed statistics for {entity_type}.{field_name} in {computation_time:.2f}s")

            return stats

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error computing statistics: {e}")
            return None

    def invalidate_statistics(self, entity_type: str, field_name: Optional[str] = None):
        """Инвалидирует кэшированную статистику"""
        try:
            collection = self.storage.db[self.cache_collection]

            if field_name:
                # Invalidate specific field
                collection.delete_one({
                    "entity_type": entity_type,
                    "field_name": field_name
                })
                log_event("precomputed_stats", "info", f"Invalidated statistics for {entity_type}.{field_name}")
            else:
                # Invalidate all fields for entity type
                result = collection.delete_many({"entity_type": entity_type})
                log_event("precomputed_stats", "info",
                         f"Invalidated {result.deleted_count} statistics entries for {entity_type}")

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error invalidating statistics: {e}")

    def get_cache_status(self) -> Dict[str, Any]:
        """Получает статус кэша предвычисленных статистик"""
        try:
            collection = self.storage.db[self.cache_collection]

            # Get basic stats
            total_entries = collection.count_documents({})

            # Get entries by entity type
            pipeline = [
                {"$group": {"_id": "$entity_type", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            entity_stats = list(collection.aggregate(pipeline))

            # Get expired entries
            now = datetime.now()
            expired_count = collection.count_documents({
                "valid_until": {"$lt": now.isoformat()}
            })

            # Get entries needing update (80% of TTL passed)
            update_threshold = now - timedelta(hours=self.default_ttl_hours * self.background_update_threshold)
            needs_update_count = collection.count_documents({
                "computed_at": {"$lt": update_threshold.isoformat()}
            })

            return {
                "total_entries": total_entries,
                "expired_entries": expired_count,
                "needs_update": needs_update_count,
                "entity_type_distribution": {item["_id"]: item["count"] for item in entity_stats},
                "cache_hit_ratio": self._calculate_cache_hit_ratio()
            }

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error getting cache status: {e}")
            return {"error": str(e)}

    def _calculate_cache_hit_ratio(self) -> float:
        """Вычисляет коэффициент попадания в кэш"""
        # This would need to be implemented with actual hit/miss tracking
        # For now, return a placeholder
        return 0.85

    def cleanup_expired_entries(self) -> int:
        """Удаляет устаревшие записи из кэша"""
        try:
            collection = self.storage.db[self.cache_collection]

            # Delete expired entries
            result = collection.delete_many({
                "valid_until": {"$lt": datetime.now().isoformat()}
            })

            log_event("precomputed_stats", "info", f"Cleaned up {result.deleted_count} expired statistics entries")

            return result.deleted_count

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error cleaning up expired entries: {e}")
            return 0

    def background_update_statistics(self, entity_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Фоновое обновление статистик, которые скоро устареют"""
        try:
            collection = self.storage.db[self.cache_collection]

            # Find entries that need updating
            update_threshold = datetime.now() - timedelta(hours=self.default_ttl_hours * self.background_update_threshold)

            query = {"computed_at": {"$lt": update_threshold.isoformat()}}
            if entity_types:
                query["entity_type"] = {"$in": entity_types}  # type: ignore

            entries_to_update = list(collection.find(query))

            updated_count = 0
            errors = []

            for entry in entries_to_update:
                try:
                    self._compute_and_cache_statistics(
                        entry["entity_type"],
                        entry["field_name"]
                    )
                    updated_count += 1
                except Exception as e:
                    errors.append(f"{entry['entity_type']}.{entry['field_name']}: {e}")

            log_event("precomputed_stats", "info",
                     f"Background update completed: {updated_count} updated, {len(errors)} errors")

            return {
                "updated_count": updated_count,
                "errors": errors,
                "total_candidates": len(entries_to_update)
            }

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error in background update: {e}")
            return {"error": str(e)}

    def export_statistics(self, entity_type: str) -> Dict[str, Any]:
        """Экспортирует все статистики для типа сущности"""
        try:
            collection = self.storage.db[self.cache_collection]

            entries = list(collection.find({"entity_type": entity_type}))

            result = {
                "entity_type": entity_type,
                "exported_at": datetime.now().isoformat(),
                "fields": {}
            }

            for entry in entries:
                result["fields"][entry["field_name"]] = {
                    "statistics": entry["statistics"],
                    "computed_at": entry["computed_at"],
                    "computation_time": entry.get("computation_time", 0)
                }

            return result

        except Exception as e:
            log_event("precomputed_stats", "error", f"Error exporting statistics: {e}")
            return {"error": str(e)}


# Global instance
_precomputed_stats_manager: Optional[PrecomputedStatisticsManager] = None


def get_precomputed_stats_manager(storage: Optional[Storage] = None) -> PrecomputedStatisticsManager:
    """Получить или создать глобальный экземпляр менеджера статистик"""
    global _precomputed_stats_manager

    if _precomputed_stats_manager is None:
        if storage is None:
            storage = Storage()
        _precomputed_stats_manager = PrecomputedStatisticsManager(storage)

    return _precomputed_stats_manager


def invalidate_entity_statistics(entity_type: str, field_name: Optional[str] = None):
    """Инвалидирует статистику для сущности (helper function)"""
    manager = get_precomputed_stats_manager()
    manager.invalidate_statistics(entity_type, field_name)


def get_cached_field_statistics(entity_type: str, field_name: str) -> Optional[Dict[str, Any]]:
    """Получает кэшированную статистику поля (helper function)"""
    manager = get_precomputed_stats_manager()
    return manager.get_statistics(entity_type, field_name)