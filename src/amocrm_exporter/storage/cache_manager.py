"""
Cache manager for Redis-based caching
"""

import json
import hashlib
import asyncio
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
import redis
from redis.exceptions import RedisError
import redis.asyncio as redis_asyncio

from ..core import config
from ..core.logger import log_event


class CacheManager:
    """Синхронный Redis кэш менеджер"""

    def __init__(self):
        """Инициализация кэш менеджера"""
        self.redis_client = None
        self.is_connected = False
        self.default_ttl = config.settings.redis_ttl_seconds
        self._connect()

    def _connect(self):
        """Подключение к Redis"""
        try:
            self.redis_client = redis.Redis(
                host=config.settings.redis_host,
                port=config.settings.redis_port,
                db=config.settings.redis_db,
                password=config.settings.redis_password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                health_check_interval=30
            )

            # Проверка подключения
            self.redis_client.ping()
            self.is_connected = True
            log_event("cache", "info", "✅ Redis подключен успешно")

        except Exception as e:
            log_event("cache", "warning", f"❌ Не удалось подключиться к Redis: {e}")
            self.is_connected = False

    def _generate_cache_key(self, prefix: str, **kwargs) -> str:
        """Генерация уникального ключа кэша"""
        key_parts = [prefix]
        for key, value in sorted(kwargs.items()):
            if isinstance(value, (dict, list)):
                value = json.dumps(value, sort_keys=True)
            key_parts.append(f"{key}:{value}")

        key_string = "|".join(key_parts)
        # Используем хэш для длинных ключей
        if len(key_string) > 200:
            key_hash = hashlib.md5(key_string.encode()).hexdigest()
            return f"{prefix}:{key_hash}"
        return key_string

    def get(self, key: str, default=None) -> Any:
        """Получение значения из кэша"""
        if not self.is_connected:
            return default

        try:
            if self.redis_client is None:
                return default
            value = self.redis_client.get(key)
            if value is None:
                return default
            return json.loads(str(value))

        except (RedisError, json.JSONDecodeError) as e:
            log_event("cache", "warning", f"Ошибка получения из кэша {key}: {e}")
            return default

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Сохранение значения в кэш"""
        if not self.is_connected:
            return False

        try:
            if self.redis_client is None:
                return False
            ttl = ttl or self.default_ttl
            serialized_value = json.dumps(value, ensure_ascii=False)
            self.redis_client.setex(key, ttl, serialized_value)
            return True

        except (RedisError, TypeError) as e:
            log_event("cache", "warning", f"Ошибка сохранения в кэш {key}: {e}")
            return False

    def delete(self, key: str) -> bool:
        """Удаление значения из кэша"""
        if not self.is_connected:
            return False

        try:
            if self.redis_client is None:
                return False
            self.redis_client.delete(key)
            return True

        except RedisError as e:
            log_event("cache", "warning", f"Ошибка удаления из кэша {key}: {e}")
            return False

    def clear_pattern(self, pattern: str) -> int:
        """Очистка кэша по паттерну"""
        if not self.is_connected:
            return 0

        try:
            if self.redis_client is None:
                return 0
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted = self.redis_client.delete(*keys)
                return int(deleted) if deleted is not None else 0
            return 0

        except RedisError as e:
            log_event("cache", "warning", f"Ошибка очистки кэша по паттерну {pattern}: {e}")
            return 0

    def get_field_statistics(self, entity_type: str, field_name: str, max_documents: int = 10000) -> Optional[Dict[str, Any]]:
        """Получение статистики поля из кэша"""
        cache_key = self._generate_cache_key(
            "field_stats",
            entity_type=entity_type,
            field_name=field_name,
            max_documents=max_documents
        )
        return self.get(cache_key)

    def set_field_statistics(self, entity_type: str, field_name: str, statistics: Dict[str, Any],
                           max_documents: int = 10000, ttl: Optional[int] = None) -> bool:
        """Сохранение статистики поля в кэш"""
        cache_key = self._generate_cache_key(
            "field_stats",
            entity_type=entity_type,
            field_name=field_name,
            max_documents=max_documents
        )
        return self.set(cache_key, statistics, ttl)

    def get_user_data(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получение данных пользователя из кэша"""
        cache_key = self._generate_cache_key("user_data", user_id=user_id)
        return self.get(cache_key)

    def set_user_data(self, user_id: int, user_data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Сохранение данных пользователя в кэш"""
        cache_key = self._generate_cache_key("user_data", user_id=user_id)
        return self.set(cache_key, user_data, ttl)

    def get_pipeline_data(self, pipeline_id: int) -> Optional[Dict[str, Any]]:
        """Получение данных пайплайна из кэша"""
        cache_key = self._generate_cache_key("pipeline_data", pipeline_id=pipeline_id)
        return self.get(cache_key)

    def set_pipeline_data(self, pipeline_id: int, pipeline_data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Сохранение данных пайплайна в кэш"""
        cache_key = self._generate_cache_key("pipeline_data", pipeline_id=pipeline_id)
        return self.set(cache_key, pipeline_data, ttl)

    def get_smart_sample(self, entity_type: str, sample_size: int = 10, max_analyze: int = 5000) -> Optional[List[Dict[str, Any]]]:
        """Получение умной выборки из кэша"""
        cache_key = self._generate_cache_key(
            "smart_sample",
            entity_type=entity_type,
            sample_size=sample_size,
            max_analyze=max_analyze
        )
        return self.get(cache_key)

    def set_smart_sample(self, entity_type: str, sample_data: List[Dict[str, Any]],
                        sample_size: int = 10, max_analyze: int = 5000, ttl: Optional[int] = None) -> bool:
        """Сохранение умной выборки в кэш"""
        cache_key = self._generate_cache_key(
            "smart_sample",
            entity_type=entity_type,
            sample_size=sample_size,
            max_analyze=max_analyze
        )
        return self.set(cache_key, sample_data, ttl)

    def invalidate_entity_cache(self, entity_type: str):
        """Инвалидация кэша для конкретного типа сущности"""
        patterns = [
            f"field_stats|entity_type:{entity_type}|*",
            f"smart_sample|entity_type:{entity_type}|*",
        ]

        total_cleared = 0
        for pattern in patterns:
            cleared = self.clear_pattern(pattern)
            total_cleared += cleared

        log_event("cache", "info", f"Очищено {total_cleared} записей кэша для {entity_type}")

    def invalidate_user_cache(self, user_id: Optional[int] = None):
        """Инвалидация кэша пользователей"""
        if user_id:
            cache_key = self._generate_cache_key("user_data", user_id=user_id)
            self.delete(cache_key)
        else:
            cleared = self.clear_pattern("user_data|*")
            log_event("cache", "info", f"Очищено {cleared} записей кэша пользователей")

    def invalidate_pipeline_cache(self, pipeline_id: Optional[int] = None):
        """Инвалидация кэша пайплайнов"""
        if pipeline_id:
            cache_key = self._generate_cache_key("pipeline_data", pipeline_id=pipeline_id)
            self.delete(cache_key)
        else:
            cleared = self.clear_pattern("pipeline_data|*")
            log_event("cache", "info", f"Очищено {cleared} записей кэша пайплайнов")

    def get_custom_fields(self, entity_type: str) -> Optional[List[Dict[str, Any]]]:
        """Получение кастомных полей для типа сущности из кэша"""
        cache_key = self._generate_cache_key("custom_fields", entity_type=entity_type)
        return self.get(cache_key)

    def set_custom_fields(self, entity_type: str, custom_fields: List[Dict[str, Any]], ttl: Optional[int] = None) -> bool:
        """Сохранение кастомных полей для типа сущности в кэш"""
        cache_key = self._generate_cache_key("custom_fields", entity_type=entity_type)
        # Используем более длинный TTL для кастомных полей, так как они редко изменяются
        custom_fields_ttl = ttl or (self.default_ttl * 10)  # 10x default TTL
        return self.set(cache_key, custom_fields, custom_fields_ttl)

    def get_all_custom_fields(self) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        """Получение всех кастомных полей из кэша"""
        cache_key = self._generate_cache_key("all_custom_fields")
        return self.get(cache_key)

    def set_all_custom_fields(self, all_custom_fields: Dict[str, List[Dict[str, Any]]], ttl: Optional[int] = None) -> bool:
        """Сохранение всех кастомных полей в кэш"""
        cache_key = self._generate_cache_key("all_custom_fields")
        # Используем более длинный TTL для кастомных полей
        custom_fields_ttl = ttl or (self.default_ttl * 10)  # 10x default TTL
        return self.set(cache_key, all_custom_fields, custom_fields_ttl)

    def get_custom_field_metadata(self, field_id: str, entity_type: str) -> Optional[Dict[str, Any]]:
        """Получение метаданных кастомного поля из кэша"""
        cache_key = self._generate_cache_key("custom_field_metadata", field_id=field_id, entity_type=entity_type)
        return self.get(cache_key)

    def set_custom_field_metadata(self, field_id: str, entity_type: str, metadata: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Сохранение метаданных кастомного поля в кэш"""
        cache_key = self._generate_cache_key("custom_field_metadata", field_id=field_id, entity_type=entity_type)
        return self.set(cache_key, metadata, ttl)

    def get_custom_fields_mapping(self, entity_type: str) -> Optional[Dict[str, Dict[str, Any]]]:
        """Получение маппинга кастомных полей (field_id -> field_info) из кэша"""
        cache_key = self._generate_cache_key("custom_fields_mapping", entity_type=entity_type)
        return self.get(cache_key)

    def set_custom_fields_mapping(self, entity_type: str, mapping: Dict[str, Dict[str, Any]], ttl: Optional[int] = None) -> bool:
        """Сохранение маппинга кастомных полей в кэш"""
        cache_key = self._generate_cache_key("custom_fields_mapping", entity_type=entity_type)
        # Используем более длинный TTL для маппинга кастомных полей
        mapping_ttl = ttl or (self.default_ttl * 10)  # 10x default TTL
        return self.set(cache_key, mapping, mapping_ttl)

    def invalidate_custom_fields_cache(self, entity_type: Optional[str] = None) -> None:
        """Инвалидация кэша кастомных полей"""
        if entity_type:
            # Очистка кэша для конкретного типа сущности
            patterns = [
                f"custom_fields|entity_type:{entity_type}|*",
                f"custom_fields_mapping|entity_type:{entity_type}|*",
                f"custom_field_metadata|*|entity_type:{entity_type}|*",
            ]
        else:
            # Очистка всего кэша кастомных полей
            patterns = [
                "custom_fields|*",
                "all_custom_fields|*",
                "custom_fields_mapping|*",
                "custom_field_metadata|*",
            ]

        total_cleared = 0
        for pattern in patterns:
            cleared = self.clear_pattern(pattern)
            total_cleared += cleared

        log_event("cache", "info", f"Очищено {total_cleared} записей кэша кастомных полей" + (f" для {entity_type}" if entity_type else ""))

    def get_cache_stats(self) -> Dict[str, Any]:
        """Получение статистики кэша"""
        if not self.is_connected:
            return {"connected": False, "error": "Redis не подключен"}

        try:
            info = self.redis_client.info()
            stats = {
                "connected": True,
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "keys_count": self.redis_client.dbsize(),
            }

            # Подсчет hit rate
            hits = stats["keyspace_hits"]
            misses = stats["keyspace_misses"]
            if hits + misses > 0:
                stats["hit_rate"] = round((hits / (hits + misses)) * 100, 2)
            else:
                stats["hit_rate"] = 0

            return stats

        except RedisError as e:
            return {"connected": False, "error": str(e)}


class AsyncCacheManager:
    """Асинхронный Redis кэш менеджер"""

    def __init__(self):
        """Инициализация асинхронного кэш менеджера"""
        self.redis_client = None
        self.is_connected = False
        self.default_ttl = config.settings.redis_ttl_seconds

    async def connect(self):
        """Асинхронное подключение к Redis"""
        try:
            self.redis_client = redis_asyncio.Redis(
                host=config.settings.redis_host,
                port=config.settings.redis_port,
                db=config.settings.redis_db,
                password=config.settings.redis_password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                health_check_interval=30
            )

            # Проверка подключения
            await self.redis_client.ping()
            self.is_connected = True
            log_event("cache", "info", "✅ Async Redis подключен успешно")

        except Exception as e:
            log_event("cache", "warning", f"❌ Не удалось подключиться к Async Redis: {e}")
            self.is_connected = False

    async def close(self):
        """Закрытие подключения к Redis"""
        if self.redis_client:
            await self.redis_client.close()
            self.is_connected = False

    def _generate_cache_key(self, prefix: str, **kwargs) -> str:
        """Генерация уникального ключа кэша"""
        key_parts = [prefix]
        for key, value in sorted(kwargs.items()):
            if isinstance(value, (dict, list)):
                value = json.dumps(value, sort_keys=True)
            key_parts.append(f"{key}:{value}")

        key_string = "|".join(key_parts)
        # Используем хэш для длинных ключей
        if len(key_string) > 200:
            key_hash = hashlib.md5(key_string.encode()).hexdigest()
            return f"{prefix}:{key_hash}"
        return key_string

    async def get(self, key: str, default=None) -> Any:
        """Асинхронное получение значения из кэша"""
        if not self.is_connected:
            return default

        try:
            value = await self.redis_client.get(key)
            if value is None:
                return default
            return json.loads(value)

        except (RedisError, json.JSONDecodeError) as e:
            log_event("cache", "warning", f"Ошибка получения из кэша {key}: {e}")
            return default

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Асинхронное сохранение значения в кэш"""
        if not self.is_connected:
            return False

        try:
            ttl = ttl or self.default_ttl
            serialized_value = json.dumps(value, ensure_ascii=False)
            await self.redis_client.setex(key, ttl, serialized_value)
            return True

        except (RedisError, TypeError) as e:
            log_event("cache", "warning", f"Ошибка сохранения в кэш {key}: {e}")
            return False


# Глобальный экземпляр кэш менеджера
cache_manager = CacheManager()


def get_cache_manager() -> CacheManager:
    """Получение экземпляра кэш менеджера"""
    return cache_manager


async def get_async_cache_manager() -> AsyncCacheManager:
    """Получение экземпляра асинхронного кэш менеджера"""
    async_cache = AsyncCacheManager()
    await async_cache.connect()
    return async_cache