# Руководство по Data Enrichment (Обогащению данных)

## Обзор

Система Data Enrichment автоматически расширяет экспортированные данные AmoCRM дополнительной информацией, делая их более полными и удобными для анализа. Основная цель - заменить численные ID пользователей, воронок и статусов человекочитаемыми названиями и дополнительными метаданными.

## Архитектура системы

### Компоненты

1. **DataEnricher** - основной класс для обогащения данных
2. **Кэширование** - автоматическое кэширование справочных данных с TTL
3. **Custom Fields Flattening** - сплющивание сложных структур AmoCRM
4. **Background Process** - автоматическое обновление кэша

### Схема работы

```
AmoCRM API → Raw Data → DataEnricher → Enriched Data → MongoDB/Files
                ↓
            Cache Layer
        (Users, Pipelines, Custom Fields)
```

## Типы обогащения

### 1. Обогащение пользовательской информацией

#### Входные данные
```json
{
  "id": 12345,
  "name": "Важная сделка",
  "responsible_user_id": 567,
  "created_by": 890,
  "updated_by": 567
}
```

#### Результат обогащения
```json
{
  "id": 12345,
  "name": "Важная сделка",
  "responsible_user_id": 567,
  "responsible_user_id_name": "Иван Иванов",
  "responsible_user_id_email": "ivan@company.com",
  "responsible_user_id_lang": "ru",
  "responsible_user_id_group_id": 2,
  "created_by": 890,
  "created_by_name": "Петр Петров",
  "created_by_email": "petr@company.com",
  "updated_by": 567,
  "updated_by_name": "Иван Иванов",
  "updated_by_email": "ivan@company.com"
}
```

#### Обогащаемые поля
- `responsible_user_id` - ответственный пользователь
- `created_by` - создатель записи
- `updated_by` - кто последний раз обновлял
- `account_id` - аккаунт (для некоторых сущностей)

### 2. Обогащение данными воронок (для сделок)

#### Входные данные
```json
{
  "id": 12345,
  "name": "Важная сделка",
  "pipeline_id": 123,
  "status_id": 456,
  "price": 100000
}
```

#### Результат обогащения
```json
{
  "id": 12345,
  "name": "Важная сделка",
  "pipeline_id": 123,
  "pipeline_name": "Продажи B2B",
  "pipeline_sort": 1,
  "pipeline_is_main": true,
  "pipeline_is_unsorted_on": false,
  "pipeline_is_archive": false,
  "pipeline_account_id": "account_123",
  "status_id": 456,
  "status_name": "Переговоры",
  "status_color": "#c1c1c1",
  "status_type": "ongoing",
  "status_sort": 2,
  "price": 100000
}
```

### 3. Сплющивание кастомных полей

#### Входные данные
```json
{
  "id": 12345,
  "custom_fields_values": [
    {
      "field_id": 789,
      "field_name": "Бюджет клиента",
      "values": [{"value": "1000000"}]
    },
    {
      "field_id": 790,
      "field_name": "Источники трафика",
      "values": [
        {"value": "Google Ads"},
        {"value": "Социальные сети"},
        {"value": "Рекомендации"}
      ]
    }
  ]
}
```

#### Результат сплющивания
```json
{
  "id": 12345,
  "custom_field_789": "1000000",
  "custom_field_budget_klienta": "1000000",
  "custom_field_790_0": "Google Ads",
  "custom_field_790_1": "Социальные сети",
  "custom_field_790_2": "Рекомендации",
  "custom_field_790_all": "Google Ads; Социальные сети; Рекомендации",
  "custom_field_790_count": 3,
  "custom_field_istochniki_trafika_all": "Google Ads; Социальные сети; Рекомендации"
}
```

## Настройка и использование

### Программная настройка

#### Инициализация DataEnricher
```python
from storage import Storage
from data_enrichment import DataEnricher

storage = Storage()
enricher = DataEnricher(storage)
```

#### Обогащение одной сущности
```python
# Обогащение пользователями
enriched_deal = enricher.enrich_entity_with_user(deal)

# Обогащение воронками
enriched_deal = enricher.enrich_entity_with_pipeline(deal)

# Полное обогащение
enriched_deal = enricher.enrich_entity_with_user(deal)
enriched_deal = enricher.enrich_entity_with_pipeline(enriched_deal)

# Сплющивание кастомных полей
flattened_deal = enricher.flatten_custom_fields(deal)
```

#### Batch обработка
```python
# Обогащение списка сущностей
enriched_deals = enricher.enrich_entities_with_users(deals, "deals")
enriched_deals = enricher.enrich_entities_with_pipelines(enriched_deals, "deals")

# Комплексная обработка батча
processed_deals = enricher.process_entities_batch(
    deals,
    "deals",
    flatten_fields=True,
    enrich_users=True,
    enrich_pipelines=True
)
```

### Настройка через веб-интерфейс

1. Перейдите в раздел "Export Settings"
2. Выберите тип сущности (Deals, Contacts, Companies)
3. В разделе "Enrichment Options":
   - ☑️ Enrich with Users - обогащение пользователями
   - ☑️ Enrich with Pipelines - обогащение воронками (только для deals)
   - ☑️ Flatten Custom Fields - сплющивание кастомных полей
4. Нажмите "Preview Sample" для просмотра результата
5. Сохраните настройки

### REST API

#### Получение настроек обогащения
```bash
GET /api/enrichment/settings

Response:
{
  "status": "success",
  "settings": {
    "deals": {
      "enrich_users": true,
      "enrich_pipelines": true,
      "flatten_fields": true
    },
    "contacts": {
      "enrich_users": true,
      "flatten_fields": true
    }
  }
}
```

#### Обновление настроек
```bash
POST /api/enrichment/settings
Content-Type: application/json

{
  "entity_type": "deals",
  "enrich_users": true,
  "enrich_pipelines": true,
  "flatten_fields": true
}

Response:
{
  "status": "success",
  "message": "Enrichment settings updated"
}
```

#### Обновление кэша
```bash
POST /api/enrichment/refresh-cache

Response:
{
  "status": "success",
  "message": "Cache refreshed",
  "users_count": 25,
  "pipelines_count": 3,
  "custom_fields_count": 47
}
```

## Система кэширования

### Типы кэшируемых данных

1. **Пользователи** - TTL: 1 час
   - ID → {name, email, lang, group_id}

2. **Воронки** - TTL: 1 час
   - ID → {name, sort, is_main, statuses[]}

3. **Кастомные поля** - TTL: 24 часа
   - entity_type + field_id → {name, type, options}

### Автоматическое обновление

Кэш автоматически обновляется когда:
- Истек TTL (время жизни)
- Явно вызван метод `refresh_*_cache()`
- Обнаружено отсутствие данных в кэше

### Принудительное обновление

```python
# Обновление кэша пользователей
enricher._refresh_user_cache()

# Обновление кэша воронок
enricher._refresh_pipeline_cache()

# Обновление кэша кастомных полей
enricher._refresh_custom_fields_cache()
```

## Custom Fields Flattening

### Поддерживаемые типы полей

#### Простые поля
- **TEXT** - текстовые поля
- **NUMERIC** - числовые поля
- **DATE** - поля дат
- **CHECKBOX** - поля-флажки

#### Сложные поля
- **SELECT** - выпадающие списки (enum_id + enum_code)
- **MULTISELECT** - множественный выбор
- **PHONE** - телефонные поля
- **EMAIL** - email поля
- **URL** - URL поля
- **FILE** - файловые поля (name + size)

#### Связанные сущности
- **CONTACT** - связи с контактами
- **COMPANY** - связи с компаниями
- **LEAD** - связи со сделками

### Алгоритм сплющивания

1. **Определение типа сущности** по ключевым полям
2. **Получение маппинга полей** из кэша custom_fields
3. **Обработка каждого поля** в custom_fields_values:
   - Извлечение значения по типу поля
   - Создание readable названия поля
   - Обработка множественных значений
4. **Сплющивание embedded структур** (теги, связанные объекты)
5. **Возврат обогащенного объекта**

### Naming Convention

#### Одиночные значения
```
custom_field_{field_id} = "value"
custom_field_{readable_name} = "value"
```

#### Множественные значения
```
custom_field_{field_id}_0 = "value1"
custom_field_{field_id}_1 = "value2"
custom_field_{field_id}_all = "value1; value2"
custom_field_{field_id}_count = 2
```

#### Embedded структуры
```
tags_names = "VIP; Важный клиент"
tags_ids = "1; 5"
tags_count = 2
embedded_contacts_names = "Иван Иванов; Петр Петров"
```

## Оптимизация производительности

### Batch обработка

Для больших объемов данных используйте batch методы:

```python
# Вместо цикла по одной записи
for entity in entities:
    enriched = enricher.enrich_entity_with_user(entity)

# Используйте batch метод
enriched_entities = enricher.enrich_entities_with_users(entities, "deals")
```

### Селективное обогащение

Отключайте ненужные типы обогащения:

```python
# Только пользователи, без воронок
processed = enricher.process_entities_batch(
    entities, "contacts",
    enrich_users=True,
    enrich_pipelines=False,  # не нужно для контактов
    flatten_fields=True
)
```

### Мониторинг кэша

Отслеживайте эффективность кэша:

```python
# Проверка актуальности кэша
if enricher._should_refresh_cache():
    print("Cache needs refresh")

# Размер кэша
print(f"Users in cache: {len(enricher._user_cache)}")
print(f"Pipelines in cache: {len(enricher._pipeline_cache)}")
```

## Устранение неполадок

### Проблема: Нет обогащения пользователями

**Возможные причины:**
1. Пользователи не экспортированы в коллекцию `users`
2. Кэш пользователей пуст или устарел
3. Неверные ID пользователей в данных

**Решение:**
```python
# Проверить наличие пользователей
users_count = storage.get_entity_count("users")
print(f"Users in storage: {users_count}")

# Принудительно обновить кэш
enricher._refresh_user_cache()
print(f"Users in cache: {len(enricher._user_cache)}")

# Проверить конкретного пользователя
user_info = enricher.get_user_info(567)
print(f"User 567: {user_info}")
```

### Проблема: Не сплющиваются кастомные поля

**Возможные причины:**
1. Нет custom_fields_values в данных
2. Неверная структура custom_fields_values
3. Кэш кастомных полей пуст

**Решение:**
```python
# Проверить структуру данных
entity = {"custom_fields_values": [...]}
if "custom_fields_values" in entity:
    print("Custom fields present")

# Обновить кэш полей
enricher._refresh_custom_fields_cache()

# Проверить маппинг полей
mapping = enricher.get_custom_field_mapping("leads")
print(f"Fields mapping: {mapping}")
```

### Проблема: Медленная обработка

**Решения:**
1. Используйте batch методы вместо циклов
2. Отключите неиспользуемые типы обогащения
3. Настройте TTL кэша под ваши нужды
4. Используйте flattened коллекции для поиска

```python
# Оптимизированная обработка
def process_large_dataset(entities):
    # Batch размер 1000 записей
    batch_size = 1000
    for i in range(0, len(entities), batch_size):
        batch = entities[i:i+batch_size]
        processed_batch = enricher.process_entities_batch(
            batch, "deals",
            flatten_fields=True,
            enrich_users=True,
            enrich_pipelines=True
        )
        # Сохранить batch в storage
        storage.append_entities("deals", processed_batch)
```

## Примеры использования

### Пример 1: Обогащение экспортированных сделок

```python
from storage import Storage
from data_enrichment import DataEnricher

# Инициализация
storage = Storage()
enricher = DataEnricher(storage)

# Получение сделок
deals = storage.get_entities("deals", limit=100)

# Обогащение
enriched_deals = enricher.process_entities_batch(
    deals, "deals",
    flatten_fields=True,
    enrich_users=True,
    enrich_pipelines=True
)

# Сохранение
storage.save_entities("deals_enriched", enriched_deals)
```

### Пример 2: Создание отчета по продажам

```python
# Получение обогащенных сделок
deals = storage.get_entities("deals_enriched")

# Анализ по ответственным
sales_by_user = {}
for deal in deals:
    user_name = deal.get("responsible_user_id_name", "Unknown")
    price = deal.get("price", 0)

    if user_name not in sales_by_user:
        sales_by_user[user_name] = 0
    sales_by_user[user_name] += price

print("Sales by user:")
for user, total in sorted(sales_by_user.items(), key=lambda x: x[1], reverse=True):
    print(f"{user}: {total:,} руб.")
```

### Пример 3: Поиск по сплющенным данным

```python
# Поиск сделок с определенным значением кастомного поля
deals_with_budget = storage.get_entities(
    "deals_enriched",
    query={"custom_field_budget": {"$gte": "1000000"}}
)

# Поиск по тегам
vip_deals = storage.get_entities(
    "deals_enriched",
    query={"tags_names": {"$regex": "VIP"}}
)
```

## Заключение

Система Data Enrichment значительно улучшает качество экспортированных данных, делая их более удобными для анализа и отчетности. Правильная настройка кэширования и использование batch методов обеспечивают высокую производительность даже при обработке больших объемов данных.