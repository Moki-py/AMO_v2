# Исправление ошибок системы

## Обзор проблем

В системе были обнаружены следующие критические ошибки:

1. **Конфликт индексов heartbeat** - MongoDB пытается создать индекс без TTL, но уже существует индекс с TTL
2. **Дублирующиеся ключи в MongoDB** - множественные записи с одинаковым ID в коллекциях
3. **Ошибки Google Sheets конфигурации** - проблемы с обработкой строковых значений настроек

## Исправления

### 1. Исправление конфликта индексов heartbeat

**Проблема:** `IndexOptionsConflict` при создании TTL индекса для heartbeat коллекции

**Решение:** Улучшена логика в `src/amocrm_exporter/storage/state_manager.py`:

```python
def _ensure_heartbeat_indexes(self):
    """Ensure indexes exist for heartbeat collection"""
    try:
        # Create index on entity_type for fast lookups
        self.heartbeat_collection.create_index([("entity_type", 1)])
        # Create index on last_heartbeat for timeout detection
        self.heartbeat_collection.create_index([("last_heartbeat", 1)])
        # Create compound index for efficient queries
        self.heartbeat_collection.create_index([("entity_type", 1), ("last_heartbeat", -1)])

        # Handle TTL index carefully - check if it exists first
        try:
            # Try to create TTL index to automatically cleanup old heartbeats (after 1 hour)
            self.heartbeat_collection.create_index([("last_heartbeat", 1)], expireAfterSeconds=3600)
        except pymongo.errors.OperationFailure as ttl_error:
            if "IndexOptionsConflict" in str(ttl_error):
                # Index exists without TTL, drop and recreate
                log_event("state", "warning", "TTL index conflict detected, dropping and recreating last_heartbeat index")
                try:
                    self.heartbeat_collection.drop_index("last_heartbeat_1")
                    self.heartbeat_collection.create_index([("last_heartbeat", 1)], expireAfterSeconds=3600)
                    log_event("state", "info", "Successfully recreated TTL index for heartbeat collection")
                except Exception as drop_error:
                    log_event("state", "warning", f"Could not recreate TTL index: {drop_error}")
            elif "already exists" in str(ttl_error):
                # Index already exists with TTL, this is fine
                log_event("state", "info", "TTL index already exists for heartbeat collection")
            else:
                raise ttl_error

        log_event("state", "info", "Created indexes for heartbeat collection")
    except pymongo.errors.PyMongoError as e:
        log_event("state", "error", f"Error creating heartbeat indexes: {e}")
```

### 2. Исправление дублирующихся ключей

**Проблема:** `E11000 duplicate key error` в коллекциях MongoDB

**Решение:** Создан скрипт `scripts/fix_duplicate_keys.py` для очистки дублирующихся записей:

```python
def fix_duplicate_keys():
    """Исправляет дублирующиеся ключи в MongoDB коллекциях"""

    # Находим дублирующиеся записи по id
    pipeline = [
        {"$group": {
            "_id": "$id",
            "count": {"$sum": 1},
            "docs": {"$push": "$_id"}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]

    # Удаляем дублирующиеся записи, оставляя самую новую
    for duplicate in duplicates:
        doc_ids = duplicate["docs"]
        docs_to_remove = doc_ids[1:]  # Удаляем все кроме первой
        collection.delete_many({"_id": {"$in": docs_to_remove}})
```

### 3. Исправление Google Sheets конфигурации

**Проблема:** `'str' object has no attribute 'get'` при валидации настроек

**Решение:** Улучшена обработка строковых значений в `src/amocrm_exporter/core/google_sheets_config.py`:

```python
def _get_spreadsheet_ids(self) -> Dict[str, Optional[str]]:
    """Get configured spreadsheet IDs from settings"""
    spreadsheet_ids = {
        'leads': settings.google_sheets_leads_id,
        'contacts': settings.google_sheets_contacts_id,
        'companies': settings.google_sheets_companies_id,
        'events': settings.google_sheets_events_id
    }

    # Фильтруем пустые значения и приводим к строковому типу
    filtered_ids = {}
    for entity_type, spreadsheet_id in spreadsheet_ids.items():
        if spreadsheet_id and isinstance(spreadsheet_id, str) and spreadsheet_id.strip():
            filtered_ids[entity_type] = spreadsheet_id.strip()
        else:
            filtered_ids[entity_type] = None

    return filtered_ids
```

## Запуск исправлений

### Автоматическое исправление всех ошибок

```bash
python scripts/fix_all_errors.py
```

### Ручное исправление отдельных проблем

1. **Исправление дублирующихся ключей:**
```bash
python scripts/fix_duplicate_keys.py
```

2. **Перезапуск сервисов:**
```bash
docker-compose down
docker-compose up -d
```

3. **Проверка состояния:**
```bash
docker-compose ps
docker-compose logs --tail=50 app
```

## Проверка исправлений

После применения исправлений проверьте:

1. **Логи приложения** - отсутствие ошибок `IndexOptionsConflict` и `DuplicateKey`
2. **Google Sheets конфигурацию** - корректная валидация настроек
3. **Состояние MongoDB** - отсутствие дублирующихся записей

## Предотвращение проблем в будущем

1. **Регулярная очистка дублирующихся записей** - запускайте скрипт периодически
2. **Мониторинг индексов** - следите за созданием индексов в логах
3. **Валидация конфигурации** - проверяйте настройки Google Sheets перед использованием

## Дополнительные рекомендации

1. **Резервное копирование** - создавайте резервные копии перед массовыми операциями
2. **Тестирование** - проверяйте исправления в тестовой среде
3. **Документирование** - ведите журнал изменений и исправлений