"""
Storage module for handling MongoDB collections
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
import pymongo
import time
import functools
import threading
from collections import defaultdict

from ..core import config
from ..core.logger import log_event
from .cache_manager import get_cache_manager
from ..processors.batch_processor import get_batch_processor, StreamingProcessor


class PerformanceMonitor:
    """Monitor and track MongoDB operation performance"""

    def __init__(self):
        """Initialize performance monitor"""
        self.operation_stats = defaultdict(list)
        self.slow_query_threshold = 1.0  # Log queries slower than 1 second
        self.lock = threading.Lock()

    def track_operation(self, operation_name: str, duration: float, collection_name: Optional[str] = None, query_type: Optional[str] = None):
        """Track an operation's performance"""
        with self.lock:
            self.operation_stats[operation_name].append({
                'duration': duration,
                'timestamp': datetime.now().isoformat(),
                'collection': collection_name,
                'query_type': query_type
            })

            # Log slow operations
            if duration > self.slow_query_threshold:
                log_event("performance", "warning",
                         f"Slow operation: {operation_name} took {duration:.2f}s on {collection_name or 'unknown'}")

            # Keep only recent stats (last 1000 operations per type)
            if len(self.operation_stats[operation_name]) > 1000:
                self.operation_stats[operation_name] = self.operation_stats[operation_name][-500:]

    def get_operation_stats(self, operation_name: Optional[str] = None) -> Dict[str, Any]:
        """Get performance statistics for operations"""
        with self.lock:
            if operation_name:
                if operation_name not in self.operation_stats:
                    return {}

                durations = [op['duration'] for op in self.operation_stats[operation_name]]
                if not durations:
                    return {}

                return {
                    'operation': operation_name,
                    'count': len(durations),
                    'avg_duration': sum(durations) / len(durations),
                    'min_duration': min(durations),
                    'max_duration': max(durations),
                    'slow_queries': len([d for d in durations if d > self.slow_query_threshold])
                }
            else:
                # Return stats for all operations
                stats = {}
                for op_name in self.operation_stats.keys():
                    stats[op_name] = self.get_operation_stats(op_name)
                return stats

    def get_slow_operations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the slowest recent operations"""
        slow_ops = []

        with self.lock:
            for op_name, operations in self.operation_stats.items():
                for op in operations:
                    if op['duration'] > self.slow_query_threshold:
                        slow_ops.append({
                            'operation': op_name,
                            **op
                        })

        # Sort by duration descending and return top N
        slow_ops.sort(key=lambda x: x['duration'], reverse=True)
        return slow_ops[:limit]


def monitor_performance(operation_name: Optional[str] = None, collection_name: Optional[str] = None, query_type: Optional[str] = None):
    """Decorator to monitor MongoDB operation performance"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            op_name = operation_name or func.__name__

            try:
                result = func(self, *args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time

                # Get collection name from self if not provided
                coll_name = collection_name
                if not coll_name and hasattr(self, '_current_collection'):
                    coll_name = self._current_collection

                # Track the operation
                if hasattr(self, 'performance_monitor'):
                    self.performance_monitor.track_operation(
                        op_name, duration, coll_name, query_type
                    )

        return wrapper
    return decorator


class Storage:
    """Storage manager for MongoDB collections"""

    def __init__(self):
        """Initialize the storage manager and MongoDB client"""
        self.client = MongoClient(config.settings.mongodb_uri)
        self.db = self.client[config.settings.mongodb_db]
        self.performance_monitor = PerformanceMonitor()
        self._current_collection = None  # Track current collection for monitoring
        self.cache_manager = get_cache_manager()
        self.batch_processor = get_batch_processor("mongodb")
        self.streaming_processor = StreamingProcessor(chunk_size=1000, buffer_size=10000)
        self._ensure_indexes()

        # Create a test log entry to verify logging is working
        try:
            log_collection = self.db['logs']
            test_log = {
                "timestamp": datetime.now().isoformat(),
                "component": "storage",
                "level": "info",
                "message": "Test log entry created during initialization"
            }
            log_collection.insert_one(test_log)
            print("Created test log entry in MongoDB")
        except Exception as e:
            print(f"Error creating test log entry: {e}")

    @monitor_performance("ensure_indexes")
    def _ensure_indexes(self):
        """Ensure optimized indexes exist for all collections"""
        print("🔧 Создание индексов MongoDB...")

        try:
            # Define all entity types that need indexing
            entity_types = ["leads", "deals", "contacts", "companies", "events", "users", "pipelines"]

            # Create basic indexes for all main collections
            for entity_type in entity_types:
                collection_name = self._get_collection_name(entity_type)
                collection = self.db[collection_name]

                try:
                    # Primary index on id field (most important for performance)
                    collection.create_index([("id", ASCENDING)], unique=True, sparse=True, background=True)
                    print(f"✅ Создан основной индекс для {collection_name}")

                    # Create sampling indexes for this entity type
                    self._ensure_sampling_indexes(collection_name, entity_type)

                except Exception as e:
                    print(f"⚠️ Ошибка создания индексов для {collection_name}: {e}")

            # Create indexes for logs collection
            try:
                logs_collection = self.db['logs']
                logs_collection.create_index([("timestamp", DESCENDING)], background=True)
                logs_collection.create_index([("component", ASCENDING)], background=True)
                logs_collection.create_index([("level", ASCENDING)], background=True)
                print("✅ Созданы индексы для коллекции logs")
            except Exception as e:
                print(f"⚠️ Ошибка создания индексов для logs: {e}")

            # Create indexes for flattened data collections
            self._ensure_flattened_indexes()

            print("✅ Создание индексов завершено успешно")

        except Exception as e:
            log_event("storage", "error", f"Ошибка при создании индексов: {e}")
            print(f"❌ Критическая ошибка создания индексов: {e}")

    def _ensure_flattened_indexes(self):
        """Create indexes for flattened data collections"""
        for entity_type in ["leads", "deals", "contacts", "companies", "events", "users", "pipelines"]:
            flattened_collection_name = f"{entity_type}_flattened"
            print(f"Creating indexes for flattened collection: {flattened_collection_name}")

            collection = self.db[flattened_collection_name]

            # Primary index on original entity id
            collection.create_index([("original_id", ASCENDING)], unique=True, sparse=True)

            # Index on entity type for filtering
            collection.create_index([("entity_type", ASCENDING)], background=True)

            # Index on last_updated for synchronization
            collection.create_index([("last_updated", ASCENDING)], background=True)

            # Create text index for full-text search across all flattened fields
            try:
                collection.create_index([("$**", "text")], background=True)
            except Exception as e:
                print(f"Text index creation info for {flattened_collection_name}: {e}")

            # Create indexes for common custom field patterns
            field_patterns = [
                "custom_field_*",
                "tags_*",
                "embedded_*",
                "*_names",
                "*_ids",
                "*_count",
                "*_all"
            ]

            # Create specialized indexes for custom fields
            for field_pattern in field_patterns:
                try:
                    # Create compound index for custom field searches
                    collection.create_index([
                        ("entity_type", ASCENDING),
                        ("last_updated", ASCENDING)
                    ], sparse=True, background=True)
                except Exception as e:
                    print(f"Compound index creation info for {flattened_collection_name}: {e}")

    def _ensure_sampling_indexes(self, collection_name: str, entity_type: str):
        """Create specialized indexes for efficient sampling operations"""
        try:
            collection = self.db[collection_name]

            # Common fields that need indexing for sampling
            sampling_fields = {
                "deals": [
                    "created_at", "updated_at", "closed_at", "price", "status_id", "pipeline_id",
                    "responsible_user_id", "created_by", "updated_by", "account_id"
                ],
                "contacts": [
                    "created_at", "updated_at", "responsible_user_id", "created_by",
                    "updated_by", "account_id", "name", "first_name", "last_name"
                ],
                "companies": [
                    "created_at", "updated_at", "responsible_user_id", "created_by",
                    "updated_by", "account_id", "name"
                ],
                "events": [
                    "created_at", "updated_at", "entity_id", "entity_type", "type"
                ],
                "users": [
                    "created_at", "updated_at", "name", "email", "lang", "group_id"
                ],
                "pipelines": [
                    "created_at", "updated_at", "name", "sort", "is_main", "account_id"
                ]
            }

            fields_to_index = sampling_fields.get(entity_type, ["created_at", "updated_at"])

            # Create individual indexes for common sampling fields
            for field in fields_to_index:
                try:
                    collection.create_index([(field, ASCENDING)], sparse=True, background=True)
                except Exception as e:
                    # Log but don't fail if index already exists
                    print(f"Index creation info for {field}: {e}")

            # Create compound indexes for efficient random sampling
            try:
                # Compound index for time-based sampling with sorting
                collection.create_index([
                    ("created_at", ASCENDING),
                    ("updated_at", ASCENDING)
                ], sparse=True, background=True)

                # For deals, create specialized indexes
                if entity_type == "deals":
                    collection.create_index([
                        ("pipeline_id", ASCENDING),
                        ("status_id", ASCENDING),
                        ("created_at", ASCENDING)
                    ], sparse=True, background=True)

                    collection.create_index([
                        ("responsible_user_id", ASCENDING),
                        ("price", DESCENDING)
                    ], sparse=True, background=True)

            except Exception as e:
                print(f"Compound index creation info for {entity_type}: {e}")

        except Exception as e:
            log_event("storage", "error", f"Error creating sampling indexes for {entity_type}: {e}")

    @monitor_performance("save_entities")
    def save_entities(self, entity_type: str, entities: list[dict[str, Any]]) -> bool:
        """Replace all entities of a type in the collection using adaptive batch processing"""
        self._current_collection = self._get_collection_name(entity_type)
        try:
            collection = self.db[self._current_collection]

            if not entities:
                return True

            # First, delete all existing documents
            collection.delete_many({})

            # Определяем функцию обработки batch'а
            def process_batch(batch: list[dict[str, Any]]) -> list[bool]:
                """Обработка одного batch'а сущностей"""
                operations = []
                for entity in batch:
                    if "id" in entity:
                        operations.append(
                            pymongo.UpdateOne(
                                {"id": entity["id"]},
                                {"$set": entity},
                                upsert=True
                            )
                        )
                    else:
                        operations.append(pymongo.InsertOne(entity))

                if operations:
                    try:
                        collection.bulk_write(operations, ordered=False)
                        return [True] * len(batch)
                    except PyMongoError as e:
                        log_event("storage", "error", f"Ошибка bulk_write: {e}")
                        return [False] * len(batch)
                return []

            # Используем adaptive batch processor
            result = self.batch_processor.process_items(entities, process_batch)

            # Инвалидируем кэш для данного типа сущности
            self.cache_manager.invalidate_entity_cache(entity_type)

            log_event(
                "storage", "info",
                f"Saved {result['processed']} {entity_type} to MongoDB "
                f"(batch size: {result['final_batch_size']}, "
                f"success rate: {result['success_rate']:.1f}%)"
            )
            return result["success"]

        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in save_entities: {e}"
            )
            return False
        finally:
            self._current_collection = None

    @monitor_performance("append_entities")
    def append_entities(self, entity_type: str, entities: list[dict[str, Any]]) -> bool:
        """Append entities to the collection (insert or update by id)"""
        if not entities:
            return True

        self._current_collection = self._get_collection_name(entity_type)
        try:
            collection = self.db[self._current_collection]
            for entity in entities:
                if "id" in entity:
                    collection.replace_one({"id": entity["id"]}, entity, upsert=True)
                else:
                    collection.insert_one(entity)
            log_event(
                "storage", "info",
                f"Appended {len(entities)} {entity_type} to MongoDB"
            )
            return True
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in append_entities: {e}"
            )
            return False
        finally:
            self._current_collection = None

    @monitor_performance("get_entities")
    def get_entities(self, entity_type: str, query: dict[str, Any] | None = None, limit: int | None = None,
                     skip: int = 0, sort_field: str | None = None, sort_order: int = 1) -> list[dict[str, Any]]:
        """Get entities of a specific type from MongoDB with optional filtering, pagination and sorting"""
        self._current_collection = self._get_collection_name(entity_type)
        try:
            collection = self.db[self._current_collection]

            # Build query with optimization hints
            cursor = collection.find(query or {}, {"_id": 0})

            # Apply sorting
            if sort_field:
                cursor = cursor.sort(sort_field, sort_order)
            elif entity_type == "logs":
                cursor = cursor.sort("timestamp", -1)
            else:
                # Default sort by id for consistent pagination
                cursor = cursor.sort("id", 1)

            # Apply pagination
            if skip > 0:
                cursor = cursor.skip(skip)

            # Apply limit with default max to prevent memory issues
            if limit:
                cursor = cursor.limit(min(limit, 10000))  # Cap at 10k records
            else:
                cursor = cursor.limit(1000)  # Default limit to prevent large responses

            return list(cursor)
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in get_entities: {e}"
            )
            return []
        finally:
            self._current_collection = None

    @monitor_performance("get_entities_paginated")
    def get_entities_paginated(self, entity_type: str, page: int = 1, page_size: int = 100,
                              query: dict[str, Any] | None = None, sort_field: str | None = None,
                              sort_order: int = 1) -> dict[str, Any]:
        """Get entities with pagination information"""
        self._current_collection = self._get_collection_name(entity_type)
        try:
            collection = self.db[self._current_collection]

            # Calculate skip
            skip = (page - 1) * page_size

            # Get total count for pagination info
            total_count = collection.count_documents(query or {})

            # Get entities
            entities = self.get_entities(
                entity_type,
                query=query,
                limit=page_size,
                skip=skip,
                sort_field=sort_field,
                sort_order=sort_order
            )

            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size
            has_next = page < total_pages
            has_prev = page > 1

            return {
                "entities": entities,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": total_count,
                    "total_pages": total_pages,
                    "has_next": has_next,
                    "has_prev": has_prev
                }
            }
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in get_entities_paginated: {e}"
            )
            return {
                "entities": [],
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": 0,
                    "total_pages": 0,
                    "has_next": False,
                    "has_prev": False
                }
            }
        finally:
            self._current_collection = None

    def get_entities_stream(self, entity_type: str, query: dict[str, Any] | None = None,
                          sort_field: str | None = None, sort_order: int = 1,
                          batch_size: int = 1000):
        """
        Получение сущностей в виде потока для больших результатов
        Возвращает генератор, который yielded batch'и данных
        """
        self._current_collection = self._get_collection_name(entity_type)
        try:
            collection = self.db[self._current_collection]

            # Создаем cursor с сортировкой
            cursor = collection.find(query or {}, {"_id": 0})

            # Применяем сортировку
            if sort_field:
                cursor = cursor.sort(sort_field, sort_order)
            elif entity_type == "logs":
                cursor = cursor.sort("timestamp", -1)
            else:
                cursor = cursor.sort("id", 1)

            # Обрабатываем данные batch'ами
            batch = []
            for document in cursor:
                batch.append(document)

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            # Возвращаем последний batch если он не пустой
            if batch:
                yield batch

        except PyMongoError as e:
            log_event("storage", "error", f"MongoDB error in get_entities_stream: {e}")
            yield []
        finally:
            self._current_collection = None

    def get_entities_with_streaming(self, entity_type: str, query: dict[str, Any] | None = None,
                                  processor_func: Optional[Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]] = None,
                                  batch_size: int = 1000) -> Dict[str, Any]:
        """
        Получение и обработка сущностей с использованием streaming processor

        Args:
            entity_type: Тип сущности
            query: Фильтр для поиска
            processor_func: Функция обработки batch'а (опционально)
            batch_size: Размер batch'а для streaming

        Returns:
            Результаты обработки
        """
        log_event("storage", "info", f"Начинаем streaming обработку {entity_type}")

        # Создаем генератор данных
        data_generator = self.get_entities_stream(entity_type, query, batch_size=batch_size)

        # Если нет функции обработки, используем identity function
        if processor_func is None:
            processor_func = lambda x: x

        # Используем streaming processor
        result = self.streaming_processor.stream_process(data_generator, processor_func)

        log_event("storage", "info",
                 f"Streaming обработка {entity_type} завершена: "
                 f"обработано {result['processed']}, ошибок {result['errors']}")

        return result

    def get_large_dataset_efficiently(self, entity_type: str, query: dict[str, Any] | None = None,
                                    limit: Optional[int] = None, use_streaming: bool = True) -> List[Dict[str, Any]]:
        """
        Эффективное получение больших наборов данных с автоматическим выбором метода

        Args:
            entity_type: Тип сущности
            query: Фильтр для поиска
            limit: Лимит количества записей
            use_streaming: Использовать streaming для больших результатов

        Returns:
            Список сущностей
        """
        # Сначала получаем примерное количество документов
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            estimated_count = collection.count_documents(query or {})

            # Если данных мало, используем обычный метод
            if estimated_count <= 5000 and not use_streaming:
                return self.get_entities(entity_type, query, limit)

            # Для больших данных используем streaming
            log_event("storage", "info",
                     f"Используем streaming для {estimated_count} записей {entity_type}")

            all_entities = []
            processed_count = 0

            for batch in self.get_entities_stream(entity_type, query, batch_size=2000):
                all_entities.extend(batch)
                processed_count += len(batch)

                # Применяем лимит если указан
                if limit and processed_count >= limit:
                    all_entities = all_entities[:limit]
                    break

                # Логируем прогресс
                if processed_count % 10000 == 0:
                    log_event("storage", "info", f"Обработано {processed_count} записей {entity_type}")

            return all_entities

        except PyMongoError as e:
            log_event("storage", "error", f"Ошибка в get_large_dataset_efficiently: {e}")
            return []

    def stream_export_data(self, entity_type: str, export_func: Callable[[List[Dict[str, Any]]], None],
                          query: dict[str, Any] | None = None, batch_size: int = 1000):
        """
        Streaming экспорт данных для больших объемов

        Args:
            entity_type: Тип сущности
            export_func: Функция экспорта batch'а данных
            query: Фильтр для поиска
            batch_size: Размер batch'а для экспорта
        """
        log_event("storage", "info", f"Начинаем streaming экспорт {entity_type}")

        total_exported = 0
        batch_count = 0

        try:
            for batch in self.get_entities_stream(entity_type, query, batch_size=batch_size):
                if not batch:
                    continue

                try:
                    export_func(batch)
                    total_exported += len(batch)
                    batch_count += 1

                    # Логируем прогресс каждые 10 batch'ей
                    if batch_count % 10 == 0:
                        log_event("storage", "info",
                                 f"Экспортировано {total_exported} записей {entity_type} "
                                 f"({batch_count} batch'ей)")

                except Exception as e:
                    log_event("storage", "error", f"Ошибка экспорта batch'а: {e}")

        except Exception as e:
            log_event("storage", "error", f"Ошибка в stream_export_data: {e}")

        log_event("storage", "info",
                 f"Streaming экспорт {entity_type} завершен: "
                 f"экспортировано {total_exported} записей в {batch_count} batch'ах")

    def update_entity(self, entity_type: str, entity_id: int, entity_data: dict[str, Any]) -> bool:
        """Update a specific entity or add it if it doesn't exist"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            result = collection.replace_one({"id": entity_id}, entity_data, upsert=True)
            if result.matched_count:
                log_event(
                    "storage", "info",
                    f"Updated {entity_type} with ID {entity_id}"
                )
            else:
                log_event(
                    "storage", "info",
                    f"Added new {entity_type} with ID {entity_id}"
                )
            return True
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in update_entity: {e}"
            )
            return False

    def delete_entity(self, entity_type: str, entity_id: int) -> bool:
        """Delete a specific entity by id"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            result = collection.delete_one({"id": entity_id})
            if result.deleted_count:
                log_event(
                    "storage", "info",
                    f"Deleted {entity_type} with ID {entity_id}"
                )
                return True
            else:
                log_event(
                    "storage", "warning",
                    f"No {entity_type} with ID {entity_id} found to delete"
                )
                return False
        except PyMongoError as e:
            log_event(
                "storage", "error",
                f"MongoDB error in delete_entity: {e}"
            )
            return False

    def get_entity_count(self, entity_type: str) -> int:
        """Get the count of entities of a specific type"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]
            return collection.count_documents({})
        except PyMongoError as e:
            log_event("storage", "error", f"MongoDB error in get_entity_count: {e}")
            return 0

    def _get_collection_name(self, entity_type: str) -> str:
        """Map entity type to MongoDB collection name"""
        # Normalize deals/leads to use the same collection
        if entity_type in ["leads", "deals"]:
            return "deals"
        elif entity_type == "contacts":
            return "contacts"
        elif entity_type == "companies":
            return "companies"
        elif entity_type == "events":
            return "events"
        elif entity_type == "logs":
            return "logs"
        else:
            return entity_type

    def add_log_entry(self, entry: dict[str, Any]) -> bool:
        """Add an entry to the logs collection"""
        try:
            collection = self.db[self._get_collection_name("logs")]
            if "timestamp" not in entry:
                entry["timestamp"] = datetime.now().isoformat()
            collection.insert_one(entry)
            self._clean_old_logs()
            return True
        except PyMongoError as e:
            # Don't use log_event to avoid circular reference
            print(f"ERROR: MongoDB error in add_log_entry: {e}")
            return False

    def _clean_old_logs(self):
        """Remove log entries older than the retention period"""
        try:
            retention_date = datetime.now().timestamp() - (
                config.settings.log_retention_days * 24 * 60 * 60
            )
            cutoff = datetime.fromtimestamp(retention_date).isoformat()
            collection = self.db[self._get_collection_name("logs")]
            collection.delete_many({"timestamp": {"$lt": cutoff}})
        except Exception as e:
            log_event("storage", "error", f"MongoDB error in _clean_old_logs: {e}")

    def get_statistics(self) -> dict[str, Any]:
        """Get statistics about stored data"""
        return {
            "deals": self.get_entity_count("leads"),
            "contacts": self.get_entity_count("contacts"),
            "companies": self.get_entity_count("companies"),
            "events": self.get_entity_count("events"),
            "logs": self.get_entity_count("logs"),
        }

    @monitor_performance("get_field_statistics_optimized")
    def get_field_statistics_optimized(self, entity_type: str, field_name: str, max_documents: int = 10000) -> dict[str, Any]:
        """Get field statistics using MongoDB aggregation pipeline for better performance"""
        # Проверяем кэш сначала
        cached_stats = self.cache_manager.get_field_statistics(entity_type, field_name, max_documents)
        if cached_stats is not None:
            return cached_stats

        self._current_collection = self._get_collection_name(entity_type)
        try:
            collection = self.db[self._current_collection]

            # Use aggregation pipeline for efficient field statistics
            pipeline = [
                # Limit the number of documents to analyze for performance
                {"$limit": max_documents},

                # Project only the field we're analyzing
                {"$project": {
                    "field_value": f"${field_name}",
                    "has_field": {"$ne": [f"${field_name}", None]},
                    "is_empty_string": {"$eq": [f"${field_name}", ""]},
                    "is_empty_array": {"$and": [
                        {"$isArray": f"${field_name}"},
                        {"$eq": [{"$size": f"${field_name}"}, 0]}
                    ]}
                }},

                # Group to calculate statistics
                {"$group": {
                    "_id": None,
                    "total_documents": {"$sum": 1},
                    "has_field_count": {
                        "$sum": {"$cond": ["$has_field", 1, 0]}
                    },
                    "non_empty_count": {
                        "$sum": {"$cond": [
                            {"$and": [
                                "$has_field",
                                {"$not": "$is_empty_string"},
                                {"$not": "$is_empty_array"}
                            ]}, 1, 0
                        ]}
                    },
                    "unique_values": {"$addToSet": "$field_value"}
                }},

                # Project final results
                {"$project": {
                    "_id": 0,
                    "total": "$total_documents",
                    "has_field": "$has_field_count",
                    "filled": "$non_empty_count",
                    "fill_percentage": {
                        "$cond": [
                            {"$gt": ["$total_documents", 0]},
                            {"$multiply": [
                                {"$divide": ["$non_empty_count", "$total_documents"]},
                                100
                            ]},
                            0
                        ]
                    },
                    "unique_values_count": {"$size": "$unique_values"}
                }}
            ]

            result = list(collection.aggregate(pipeline))

            if result:
                stats = result[0]
                final_stats = {
                    "total": stats.get("total", 0),
                    "has_field": stats.get("has_field", 0),
                    "filled": stats.get("filled", 0),
                    "fill_percentage": round(stats.get("fill_percentage", 0), 2),
                    "unique_values": stats.get("unique_values_count", 0)
                }
            else:
                final_stats = {"total": 0, "has_field": 0, "filled": 0, "fill_percentage": 0, "unique_values": 0}

            # Сохраняем в кэш
            self.cache_manager.set_field_statistics(entity_type, field_name, final_stats, max_documents)
            return final_stats

        except Exception as e:
            log_event("storage", "error", f"Error getting field statistics for {field_name}: {e}")
            return {"total": 0, "has_field": 0, "filled": 0, "fill_percentage": 0, "unique_values": 0}
        finally:
            self._current_collection = None

    @monitor_performance("get_smart_sample_optimized")
    def get_smart_sample_optimized(self, entity_type: str, sample_size: int = 10, max_analyze: int = 5000) -> list[dict[str, Any]]:
        """Get smart sample using MongoDB aggregation pipeline for better performance"""
        # Проверяем кэш сначала
        cached_sample = self.cache_manager.get_smart_sample(entity_type, sample_size, max_analyze)
        if cached_sample is not None:
            return cached_sample

        self._current_collection = self._get_collection_name(entity_type)
        try:
            collection = self.db[self._current_collection]

            # Use aggregation pipeline for smart sampling
            pipeline = [
                # Limit analysis to recent documents for performance
                {"$sort": {"updated_at": -1}},
                {"$limit": max_analyze},

                # Add field count for each document
                {"$addFields": {
                    "field_count": {
                        "$size": {
                            "$filter": {
                                "input": {"$objectToArray": "$$ROOT"},
                                "as": "field",
                                "cond": {
                                    "$and": [
                                        {"$ne": ["$$field.v", None]},
                                        {"$ne": ["$$field.v", ""]},
                                        {"$not": {"$eq": [
                                            {"$type": "$$field.v"},
                                            "missing"
                                        ]}}
                                    ]
                                }
                            }
                        }
                    }
                }},

                # Sort by field count descending (documents with most filled fields first)
                {"$sort": {"field_count": -1}},

                # Take top samples
                {"$limit": sample_size},

                # Remove our temporary field_count
                {"$project": {"field_count": 0, "_id": 0}}
            ]

            result = list(collection.aggregate(pipeline))

            # Сохраняем в кэш если получили результат
            if result and len(result) > 0:
                self.cache_manager.set_smart_sample(entity_type, result, sample_size, max_analyze)

            return result

        except Exception as e:
            log_event("storage", "error", f"Error getting smart sample for {entity_type}: {e}")
            return []
        finally:
            self._current_collection = None

    def get_all_field_statistics_optimized(self, entity_type: str, max_documents: int = 5000) -> dict[str, dict[str, Any]]:
        """Get statistics for all fields using optimized aggregation pipeline"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]

            # First, get a sample of documents to determine all field names
            sample_pipeline = [
                {"$limit": 100},
                {"$project": {"field_names": {"$objectToArray": "$$ROOT"}}},
                {"$unwind": "$field_names"},
                {"$group": {"_id": "$field_names.k"}},
                {"$project": {"_id": 0, "field_name": "$_id"}}
            ]

            field_results = list(collection.aggregate(sample_pipeline))
            field_names = [f["field_name"] for f in field_results if not f["field_name"].startswith("_")]

            # Now get statistics for each field using parallel aggregation
            all_stats = {}

            # Process fields in batches to avoid memory issues
            batch_size = 10
            for i in range(0, len(field_names), batch_size):
                batch_fields = field_names[i:i+batch_size]

                # Create aggregation pipeline for this batch
                project_stage = {}
                for j, field_name in enumerate(batch_fields):
                    project_stage[f"field_{j}"] = f"${field_name}"

                group_stage = {
                    "_id": None,
                    "total": {"$sum": 1}
                }

                for j in range(len(batch_fields)):
                    group_stage[f"filled_{j}"] = {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": [f"$field_{j}", None]},
                                        {"$ne": [f"$field_{j}", ""]},
                                        {"$not": {
                                            "$and": [
                                                {"$isArray": f"$field_{j}"},
                                                {"$eq": [{"$size": f"$field_{j}"}, 0]}
                                            ]
                                        }}
                                    ]
                                }, 1, 0
                            ]
                        }
                    }
                    group_stage[f"unique_{j}"] = {"$addToSet": f"$field_{j}"}

                pipeline = [
                    {"$limit": max_documents},
                    {"$project": project_stage},
                    {"$group": group_stage}
                ]

                batch_result = list(collection.aggregate(pipeline))

                if batch_result:
                    result = batch_result[0]
                    total = result.get("total", 0)

                    for j, field_name in enumerate(batch_fields):
                        filled = result.get(f"filled_{j}", 0)
                        unique_values = result.get(f"unique_{j}", [])
                        fill_percentage = (filled / total * 100) if total > 0 else 0

                        all_stats[field_name] = {
                            "total": total,
                            "filled": filled,
                            "fill_percentage": round(fill_percentage, 2),
                            "unique_values": len([v for v in unique_values if v is not None])
                        }

            return all_stats

        except Exception as e:
            log_event("storage", "error", f"Error getting all field statistics for {entity_type}: {e}")
            return {}

    def get_field_sample_values(self, entity_type: str, field_name: str, limit: int = 20,
                               max_analyze: int = 1000) -> list[str]:
        """Get sample values for a field using aggregation pipeline"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]

            pipeline = [
                # Limit documents to analyze
                {"$limit": max_analyze},

                # Match documents that have the field and it's not empty
                {"$match": {
                    field_name: {
                        "$exists": True,
                        "$ne": None,
                        "$ne": ""
                    }
                }},

                # Project only the field we want
                {"$project": {"field_value": f"${field_name}"}},

                # Group by value to get unique values
                {"$group": {"_id": "$field_value"}},

                # Limit results
                {"$limit": limit},

                # Project to clean format
                {"$project": {"_id": 0, "value": "$_id"}}
            ]

            results = list(collection.aggregate(pipeline))
            return [str(r["value"]) for r in results if r["value"] is not None]

        except Exception as e:
            log_event("storage", "error", f"Error getting field sample values for {field_name}: {e}")
            return []

    # Methods for flattened data management
    def save_flattened_entities(self, entity_type: str, flattened_entities: List[Dict[str, Any]]) -> bool:
        """Save flattened entities to separate collection"""
        try:
            if not flattened_entities:
                return True

            collection_name = f"{entity_type}_flattened"
            collection = self.db[collection_name]

            # Prepare flattened documents with metadata
            documents = []
            for entity in flattened_entities:
                if "id" not in entity:
                    continue

                flattened_doc = {
                    "original_id": entity["id"],
                    "entity_type": entity_type,
                    "last_updated": datetime.now().isoformat(),
                    "flattened_data": entity
                }
                documents.append(flattened_doc)

            if not documents:
                return True

            # Use bulk upsert operations
            operations = []
            for doc in documents:
                operations.append(
                    pymongo.UpdateOne(
                        {"original_id": doc["original_id"]},
                        {"$set": doc},
                        upsert=True
                    )
                )

            collection.bulk_write(operations, ordered=False)

            log_event("storage", "info", f"Saved {len(documents)} flattened {entity_type} entities")
            return True

        except PyMongoError as e:
            log_event("storage", "error", f"Error saving flattened entities: {e}")
            return False

    def get_flattened_entities(self, entity_type: str, query: Optional[Dict[str, Any]] = None,
                             limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get flattened entities with optional query and limit"""
        try:
            collection_name = f"{entity_type}_flattened"
            collection = self.db[collection_name]

            # Build query
            mongo_query = {"entity_type": entity_type}
            if query:
                # Add query conditions to flattened_data fields
                for key, value in query.items():
                    mongo_query[f"flattened_data.{key}"] = value

            cursor = collection.find(mongo_query, {"_id": 0})

            if limit:
                cursor = cursor.limit(limit)

            results = []
            for doc in cursor:
                flattened_data = doc.get("flattened_data", {})
                flattened_data["_flattened_metadata"] = {
                    "original_id": doc.get("original_id"),
                    "entity_type": doc.get("entity_type"),
                    "last_updated": doc.get("last_updated")
                }
                results.append(flattened_data)

            return results

        except PyMongoError as e:
            log_event("storage", "error", f"Error getting flattened entities: {e}")
            return []

    def search_flattened_entities(self, entity_type: str, search_text: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Full-text search in flattened entities"""
        try:
            collection_name = f"{entity_type}_flattened"
            collection = self.db[collection_name]

            # Use text search with entity type filter
            query = {
                "$text": {"$search": search_text},
                "entity_type": entity_type
            }

            cursor = collection.find(
                query,
                {"_id": 0, "score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})]).limit(limit)

            results = []
            for doc in cursor:
                flattened_data = doc.get("flattened_data", {})
                flattened_data["_search_score"] = doc.get("score", 0)
                flattened_data["_flattened_metadata"] = {
                    "original_id": doc.get("original_id"),
                    "entity_type": doc.get("entity_type"),
                    "last_updated": doc.get("last_updated")
                }
                results.append(flattened_data)

            return results

        except PyMongoError as e:
            log_event("storage", "error", f"Error searching flattened entities: {e}")
            return []

    def sync_flattened_data(self, entity_type: str, entity_ids: Optional[List[int]] = None) -> bool:
        """Synchronize flattened data with original entities"""
        try:
            # Get original entities
            query = {}
            if entity_ids:
                query["id"] = {"$in": entity_ids}

            original_entities = self.get_entities(entity_type, query)

            if not original_entities:
                return True

            # Import and use data enrichment for flattening
            from data_enrichment import DataEnricher
            enricher = DataEnricher(self)

            # Flatten the entities
            flattened_entities = [
                enricher.flatten_custom_fields(entity) for entity in original_entities
            ]

            # Save flattened data
            return self.save_flattened_entities(entity_type, flattened_entities)

        except Exception as e:
            log_event("storage", "error", f"Error synchronizing flattened data: {e}")
            return False

    def get_flattened_field_statistics(self, entity_type: str, max_documents: int = 5000) -> Dict[str, Dict[str, Any]]:
        """Get field statistics for flattened entities"""
        try:
            collection_name = f"{entity_type}_flattened"
            collection = self.db[collection_name]

            # Get all flattened field names
            sample_pipeline = [
                {"$match": {"entity_type": entity_type}},
                {"$limit": 100},
                {"$project": {"field_names": {"$objectToArray": "$flattened_data"}}},
                {"$unwind": "$field_names"},
                {"$group": {"_id": "$field_names.k"}},
                {"$project": {"_id": 0, "field_name": "$_id"}}
            ]

            field_results = list(collection.aggregate(sample_pipeline))
            field_names = [f["field_name"] for f in field_results if not f["field_name"].startswith("_")]

            # Get statistics for each field
            all_stats = {}

            for field_name in field_names:
                pipeline = [
                    {"$match": {"entity_type": entity_type}},
                    {"$limit": max_documents},
                    {"$group": {
                        "_id": None,
                        "total": {"$sum": 1},
                        "filled": {
                            "$sum": {
                                "$cond": [
                                    {
                                        "$and": [
                                            {"$ne": [f"$flattened_data.{field_name}", None]},
                                            {"$ne": [f"$flattened_data.{field_name}", ""]}
                                        ]
                                    }, 1, 0
                                ]
                            }
                        },
                        "unique_values": {"$addToSet": f"$flattened_data.{field_name}"}
                    }}
                ]

                result = list(collection.aggregate(pipeline))
                if result:
                    stats = result[0]
                    total = stats.get("total", 0)
                    filled = stats.get("filled", 0)
                    unique_values = stats.get("unique_values", [])

                    all_stats[field_name] = {
                        "total": total,
                        "filled": filled,
                        "fill_percentage": round((filled / total * 100) if total > 0 else 0, 2),
                        "unique_values": len([v for v in unique_values if v is not None])
                    }

            return all_stats

        except Exception as e:
            log_event("storage", "error", f"Error getting flattened field statistics: {e}")
            return {}

    def get_entities_needing_flattening(self, entity_type: str, batch_size: int = 100,
                                        max_age_hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get entities that need flattening (not in flattened collection or outdated)

        Args:
            entity_type: Type of entities to check
            batch_size: Maximum number of entities to return
            max_age_hours: Maximum age in hours for flattened data before re-flattening

        Returns:
            List of entities that need flattening
        """
        try:
            # Use aggregation pipeline for efficient comparison
            original_collection = self.db[self._get_collection_name(entity_type)]
            flattened_collection = self.db[f"{entity_type}_flattened"]

            # Calculate cutoff time for forced re-flattening
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)

            # Build aggregation pipeline for efficient comparison
            pipeline = [
                # Match entities with recent updates or no flattened version
                {
                    "$lookup": {
                        "from": f"{entity_type}_flattened",
                        "localField": "id",
                        "foreignField": "original_id",
                        "as": "flattened_info"
                    }
                },
                # Add computed fields for comparison
                {
                    "$addFields": {
                        # Normalize updated_at to datetime for comparison
                        "normalized_updated_at": {
                            "$switch": {
                                "branches": [
                                    # If updated_at is a string, convert to date
                                    {
                                        "case": {"$eq": [{"$type": "$updated_at"}, "string"]},
                                        "then": {
                                            "$convert": {
                                                "input": "$updated_at",
                                                "to": "date",
                                                "onError": None
                                            }
                                        }
                                    },
                                    # If updated_at is a number (Unix timestamp), convert to date
                                    {
                                        "case": {"$eq": [{"$type": "$updated_at"}, "number"]},
                                        "then": {
                                            "$convert": {
                                                "input": {"$multiply": ["$updated_at", 1000]},
                                                "to": "date",
                                                "onError": None
                                            }
                                        }
                                    }
                                ],
                                "default": None
                            }
                        },
                        # Get flattened last_updated as date
                        "flattened_last_updated": {
                            "$convert": {
                                "input": {"$arrayElemAt": ["$flattened_info.last_updated", 0]},
                                "to": "date",
                                "onError": None
                            }
                        },
                        "needs_flattening": {
                            "$or": [
                                # No flattened version exists
                                {"$eq": [{"$size": "$flattened_info"}, 0]},
                                # Flattened version is outdated
                                {
                                    "$and": [
                                        {"$gt": [{"$size": "$flattened_info"}, 0]},
                                        {
                                            "$or": [
                                                # Entity was updated after flattening (only compare if both dates are valid)
                                                {
                                                    "$and": [
                                                        {"$ne": ["$normalized_updated_at", None]},
                                                        {"$ne": ["$flattened_last_updated", None]},
                                                        {"$gt": ["$normalized_updated_at", "$flattened_last_updated"]}
                                                    ]
                                                },
                                                # Flattened data is too old
                                                {
                                                    "$and": [
                                                        {"$ne": ["$flattened_last_updated", None]},
                                                        {"$lt": ["$flattened_last_updated", cutoff_time]}
                                                    ]
                                                },
                                                # If we can't parse dates, assume needs flattening
                                                {
                                                    "$or": [
                                                        {"$eq": ["$normalized_updated_at", None]},
                                                        {"$eq": ["$flattened_last_updated", None]}
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
                # Filter only entities that need flattening
                {"$match": {"needs_flattening": True}},
                # Remove the computed fields from results
                {"$unset": ["flattened_info", "needs_flattening", "normalized_updated_at", "flattened_last_updated"]},
                # Sort by priority (recently updated first)
                {"$sort": {"updated_at": -1}},
                # Limit results
                {"$limit": batch_size}
            ]

            # Execute aggregation
            result = list(original_collection.aggregate(pipeline))

            # Fallback to simpler method if aggregation fails
            if not result:
                return self._get_entities_needing_flattening_simple(entity_type, batch_size)

            log_event("storage", "info",
                     f"Found {len(result)} {entity_type} entities needing flattening using optimized query")

            return result

        except Exception as e:
            log_event("storage", "warning", f"Optimized flattening query failed: {e}, falling back to simple method")
            return self._get_entities_needing_flattening_simple(entity_type, batch_size)

    def _get_entities_needing_flattening_simple(self, entity_type: str, batch_size: int = 100) -> List[Dict[str, Any]]:
        """Fallback method for getting entities needing flattening (original implementation)"""
        try:
            # Get all original entities with pagination to avoid memory issues
            original_entities = self.get_entities(entity_type, limit=batch_size * 5)
            original_ids = {entity["id"]: entity.get("updated_at", 0) for entity in original_entities if "id" in entity}

            # Get flattened entities metadata
            flattened_collection = self.db[f"{entity_type}_flattened"]
            flattened_docs = list(flattened_collection.find(
                {"entity_type": entity_type},
                {"original_id": 1, "last_updated": 1}
            ))

            flattened_ids = {
                doc["original_id"]: doc.get("last_updated", "")
                for doc in flattened_docs
            }

            # Helper function to normalize timestamps for comparison
            def normalize_timestamp(timestamp):
                """Normalize timestamps to comparable format"""
                if isinstance(timestamp, str):
                    try:
                        # Try to parse ISO format string to datetime
                        from datetime import datetime
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        return dt.timestamp()
                    except (ValueError, AttributeError):
                        # If parsing fails, return 0 to force re-flattening
                        return 0
                elif isinstance(timestamp, (int, float)):
                    # Already a Unix timestamp
                    return timestamp
                else:
                    # Unknown type, return 0 to force re-flattening
                    return 0

            # Find entities that need flattening
            entities_to_flatten = []
            for entity in original_entities:
                if "id" not in entity:
                    continue

                entity_id = entity["id"]
                entity_updated = entity.get("updated_at", 0)

                # Normalize timestamps for comparison
                normalized_entity_updated = normalize_timestamp(entity_updated)
                normalized_flattened_updated = normalize_timestamp(
                    flattened_ids.get(entity_id, 0)
                )

                # Check if entity needs flattening
                if (entity_id not in flattened_ids or
                    not flattened_ids[entity_id] or
                    normalized_entity_updated > normalized_flattened_updated):
                    entities_to_flatten.append(entity)

                    if len(entities_to_flatten) >= batch_size:
                        break

            return entities_to_flatten

        except Exception as e:
            log_event("storage", "error", f"Error getting entities needing flattening: {e}")
            return []

    def optimize_query_performance(self, entity_type: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze and optimize query performance"""
        try:
            collection = self.db[self._get_collection_name(entity_type)]

            # Use explain to get query performance info
            explain_result = collection.find(query).explain()

            # Analyze execution stats
            execution_stats = explain_result.get("executionStats", {})

            return {
                "query": query,
                "execution_time_ms": execution_stats.get("executionTimeMillis", 0),
                "documents_examined": execution_stats.get("totalDocsExamined", 0),
                "documents_returned": execution_stats.get("totalDocsReturned", 0),
                "index_used": execution_stats.get("winningPlan", {}).get("inputStage", {}).get("indexName"),
                "stage": execution_stats.get("winningPlan", {}).get("stage"),
                "optimization_suggestions": self._get_optimization_suggestions(execution_stats)
            }
        except Exception as e:
            log_event("storage", "error", f"Error analyzing query performance: {e}")
            return {}

    def _get_optimization_suggestions(self, execution_stats: Dict[str, Any]) -> List[str]:
        """Generate optimization suggestions based on execution stats"""
        suggestions = []

        docs_examined = execution_stats.get("totalDocsExamined", 0)
        docs_returned = execution_stats.get("totalDocsReturned", 0)

        # Check if too many documents are being examined
        if docs_examined > docs_returned * 10:
            suggestions.append("Consider adding an index to reduce document examination")

        # Check if collection scan is being used
        winning_plan = execution_stats.get("winningPlan", {})
        if winning_plan.get("stage") == "COLLSCAN":
            suggestions.append("Query is performing a collection scan - add appropriate indexes")

        # Check execution time
        exec_time = execution_stats.get("executionTimeMillis", 0)
        if exec_time > 1000:
            suggestions.append("Query execution time is high - consider query optimization")

        return suggestions

    def get_collection_stats(self, entity_type: str) -> Dict[str, Any]:
        """Get collection statistics for performance monitoring"""
        try:
            collection_name = self._get_collection_name(entity_type)
            collection = self.db[collection_name]

            # Get collection stats
            stats = self.db.command("collStats", collection_name)

            # Get index information
            indexes = list(collection.list_indexes())

            return {
                "collection_name": collection_name,
                "document_count": stats.get("count", 0),
                "storage_size": stats.get("storageSize", 0),
                "index_count": len(indexes),
                "indexes": [{"name": idx.get("name"), "keys": idx.get("key")} for idx in indexes],
                "avg_doc_size": stats.get("avgObjSize", 0),
                "total_index_size": stats.get("totalIndexSize", 0)
            }
        except Exception as e:
            log_event("storage", "error", f"Error getting collection stats: {e}")
            return {}

    def get_slow_queries_analysis(self, threshold_ms: int = 1000) -> List[Dict[str, Any]]:
        """Get analysis of slow queries from performance monitor"""
        try:
            slow_operations = self.performance_monitor.get_slow_operations(50)

            analysis = []
            for op in slow_operations:
                if op["duration"] * 1000 > threshold_ms:  # Convert to ms
                    analysis.append({
                        "operation": op["operation"],
                        "duration_ms": op["duration"] * 1000,
                        "collection": op.get("collection", "unknown"),
                        "timestamp": op["timestamp"],
                        "optimization_needed": True
                    })

            return analysis
        except Exception as e:
            log_event("storage", "error", f"Error analyzing slow queries: {e}")
            return []

    def delete_flattened_entities(self, entity_type: str, entity_ids: List[int]) -> bool:
        """Delete flattened entities by original entity IDs"""
        try:
            collection_name = f"{entity_type}_flattened"
            collection = self.db[collection_name]

            result = collection.delete_many({
                "original_id": {"$in": entity_ids}
            })

            log_event("storage", "info", f"Deleted {result.deleted_count} flattened {entity_type} entities")
            return True

        except PyMongoError as e:
            log_event("storage", "error", f"Error deleting flattened entities: {e}")
            return False

    def monitor_indexes(self) -> Dict[str, Any]:
        """Мониторинг индексов MongoDB для всех коллекций"""
        index_stats = {}

        for entity_type in ["leads", "deals", "contacts", "companies", "events", "users", "pipelines", "logs"]:
            collection_name = self._get_collection_name(entity_type)
            try:
                collection = self.db[collection_name]

                # Получаем статистику индексов
                index_stats[entity_type] = {
                    "collection": collection_name,
                    "indexes": [],
                    "total_size": 0,
                    "unused_indexes": []
                }

                # Получаем список всех индексов
                indexes = list(collection.list_indexes())

                for index in indexes:
                    index_name = index.get("name", "unknown")
                    index_keys = index.get("key", {})

                    # Получаем статистику использования индекса
                    try:
                        index_stat = self.db.command("collStats", collection_name, indexDetails=True)
                        index_sizes = index_stat.get("indexSizes", {})
                        index_size = index_sizes.get(index_name, 0)

                        index_info = {
                            "name": index_name,
                            "keys": index_keys,
                            "size": index_size,
                            "sparse": index.get("sparse", False),
                            "partial": "partialFilterExpression" in index,
                            "unique": index.get("unique", False),
                            "background": index.get("background", False)
                        }

                        index_stats[entity_type]["indexes"].append(index_info)
                        index_stats[entity_type]["total_size"] += index_size

                    except Exception as e:
                        log_event("storage", "warning", f"Ошибка получения статистики индекса {index_name}: {e}")

                # Проверяем неиспользуемые индексы
                try:
                    unused_indexes = self._find_unused_indexes(collection_name)
                    index_stats[entity_type]["unused_indexes"] = unused_indexes
                except Exception as e:
                    log_event("storage", "warning", f"Ошибка поиска неиспользуемых индексов: {e}")

            except Exception as e:
                log_event("storage", "error", f"Ошибка мониторинга индексов для {entity_type}: {e}")

        return index_stats

    def _find_unused_indexes(self, collection_name: str) -> List[str]:
        """Поиск неиспользуемых индексов на основе статистики"""
        unused_indexes = []

        try:
            # Получаем статистику использования индексов
            stats = self.db.command("collStats", collection_name, indexDetails=True)

            # Проверяем статистику использования каждого индекса
            index_access_stats = stats.get("indexStats", [])

            for index_stat in index_access_stats:
                index_name = index_stat.get("name", "")
                accesses = index_stat.get("accesses", {})
                ops = accesses.get("ops", 0)

                # Если индекс не использовался (кроме _id индекса)
                if ops == 0 and index_name != "_id_":
                    unused_indexes.append(index_name)

        except Exception as e:
            log_event("storage", "warning", f"Ошибка получения статистики использования индексов: {e}")

        return unused_indexes

    def analyze_query_performance(self, entity_type: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """Анализ производительности запроса и рекомендации по индексам"""
        collection_name = self._get_collection_name(entity_type)

        try:
            collection = self.db[collection_name]

            # Выполняем explain для запроса
            explain_result = collection.find(query).explain("executionStats")

            execution_stats = explain_result.get("executionStats", {})
            winning_plan = explain_result.get("queryPlanner", {}).get("winningPlan", {})

            analysis = {
                "query": query,
                "execution_time_ms": execution_stats.get("executionTimeMillis", 0),
                "total_docs_examined": execution_stats.get("totalDocsExamined", 0),
                "total_docs_returned": execution_stats.get("totalDocsReturned", 0),
                "execution_success": execution_stats.get("executionSuccess", False),
                "index_used": self._extract_index_info(winning_plan),
                "stage": winning_plan.get("stage", "unknown"),
                "needs_optimization": False,
                "recommendations": []
            }

            # Анализируем производительность
            docs_examined = analysis["total_docs_examined"]
            docs_returned = analysis["total_docs_returned"]
            execution_time = analysis["execution_time_ms"]

            # Проверяем, нужна ли оптимизация
            if docs_examined > docs_returned * 10:  # Слишком много документов просмотрено
                analysis["needs_optimization"] = True
                analysis["recommendations"].append("Рассмотрите создание индекса для уменьшения количества просматриваемых документов")

            if execution_time > 1000:  # Запрос выполняется больше секунды
                analysis["needs_optimization"] = True
                analysis["recommendations"].append("Запрос выполняется слишком долго, требуется оптимизация")

            if analysis["stage"] == "COLLSCAN":  # Полное сканирование коллекции
                analysis["needs_optimization"] = True
                analysis["recommendations"].append("Запрос выполняет полное сканирование коллекции, создайте индекс")

            # Предлагаем создание индекса
            if analysis["needs_optimization"]:
                suggested_index = self._suggest_index(query)
                if suggested_index:
                    analysis["recommendations"].append(f"Рекомендуется создать индекс: {suggested_index}")

            return analysis

        except Exception as e:
            log_event("storage", "error", f"Ошибка анализа производительности запроса: {e}")
            return {"error": str(e)}

    def _extract_index_info(self, winning_plan: Dict[str, Any]) -> Optional[str]:
        """Извлечение информации об использованном индексе"""
        if "inputStage" in winning_plan:
            input_stage = winning_plan["inputStage"]
            if "indexName" in input_stage:
                return input_stage["indexName"]
        elif "indexName" in winning_plan:
            return winning_plan["indexName"]
        return None

    def _suggest_index(self, query: Dict[str, Any]) -> Optional[str]:
        """Предложение индекса на основе запроса"""
        if not query:
            return None

        # Простое предложение индекса на основе полей запроса
        index_fields = []
        for field, value in query.items():
            if field not in ["$or", "$and", "$nor"]:  # Исключаем логические операторы
                index_fields.append(field)

        if index_fields:
            field_pairs = [f'"{field}": 1' for field in index_fields]
            return f"db.{self._get_collection_name('unknown')}.createIndex({{{', '.join(field_pairs)}}})"
        return None

    def create_recommended_indexes(self, entity_type: str, force: bool = False) -> Dict[str, Any]:
        """Создание рекомендованных индексов на основе анализа запросов"""
        collection_name = self._get_collection_name(entity_type)

        try:
            collection = self.db[collection_name]
            results = {
                "entity_type": entity_type,
                "collection": collection_name,
                "indexes_created": [],
                "errors": []
            }

            # Рекомендованные индексы на основе распространенных запросов
            recommended_indexes = self._get_recommended_indexes(entity_type)

            for index_spec in recommended_indexes:
                index_name = ""
                try:
                    index_name = index_spec.get("name", "")
                    keys = index_spec.get("keys", {})
                    options = index_spec.get("options", {})

                    # Проверяем, существует ли уже такой индекс
                    existing_indexes = [idx["name"] for idx in collection.list_indexes()]

                    if index_name not in existing_indexes or force:
                        collection.create_index(
                            list(keys.items()),
                            name=index_name,
                            background=True,
                            **options
                        )
                        results["indexes_created"].append(index_name)
                        log_event("storage", "info", f"Создан рекомендованный индекс: {index_name}")
                    else:
                        log_event("storage", "info", f"Индекс уже существует: {index_name}")

                except Exception as e:
                    error_msg = f"Ошибка создания индекса {index_name}: {e}"
                    results["errors"].append(error_msg)
                    log_event("storage", "error", error_msg)

            return results

        except Exception as e:
            log_event("storage", "error", f"Ошибка создания рекомендованных индексов: {e}")
            return {"error": str(e)}

    def _get_recommended_indexes(self, entity_type: str) -> List[Dict[str, Any]]:
        """Получение списка рекомендованных индексов для типа сущности"""
        recommendations = []

        if entity_type in ["leads", "deals"]:
            recommendations.extend([
                {
                    "name": "compound_status_updated",
                    "keys": {"status_id": 1, "updated_at": -1},
                    "options": {"sparse": True}
                },
                {
                    "name": "compound_user_price",
                    "keys": {"responsible_user_id": 1, "price": -1},
                    "options": {"sparse": True}
                },
                {
                    "name": "compound_pipeline_created",
                    "keys": {"pipeline_id": 1, "created_at": -1},
                    "options": {"sparse": True}
                }
            ])

        elif entity_type == "contacts":
            recommendations.extend([
                {
                    "name": "compound_company_updated",
                    "keys": {"company_id": 1, "updated_at": -1},
                    "options": {"sparse": True}
                },
                {
                    "name": "text_search_names",
                    "keys": {"name": "text", "first_name": "text", "last_name": "text"},
                    "options": {}
                }
            ])

        elif entity_type == "companies":
            recommendations.extend([
                {
                    "name": "compound_user_updated",
                    "keys": {"responsible_user_id": 1, "updated_at": -1},
                    "options": {"sparse": True}
                }
            ])

        return recommendations
