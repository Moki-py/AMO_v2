# AmoCRM Data Exporter

Приложение для экспорта данных из AmoCRM в JSON-файлы с возможностью просмотра через веб-интерфейс и параллельной выгрузкой. Поддерживает сохранение в MongoDB.

## Оглавление
- [Основные возможности](#основные-возможности)
- [Новые возможности](#новые-возможности)
- [Структура проекта](#структура-проекта)
- [Требования](#требования)
- [Быстрый старт](#быстрый-старт)
- [Docker](#docker)
- [Работа с веб-интерфейсом](#работа-с-веб-интерфейсом)
- [Data Enrichment (Обогащение данных)](#data-enrichment-обогащение-данных)
- [Flattening System (Система сплющивания)](#flattening-system-система-сплющивания)
- [Performance Monitoring (Мониторинг производительности)](#performance-monitoring-мониторинг-производительности)
- [Benchmark Tests (Тесты производительности)](#benchmark-tests-тесты-производительности)
- [Использование CLI](#использование-cli)
- [Описание ключевых файлов](#описание-ключевых-файлов)
- [Экспортируемые данные](#экспортируемые-данные)
- [Решение типичных проблем](#решение-типичных-проблем)
- [Расширение функциональности](#расширение-функциональности)
- [Scalable Worker System](#scalable-worker-system)
- [Message Queue-Based Worker System](#message-queue-based-worker-system)

## Основные возможности

- **Параллельная выгрузка** сделок, контактов, компаний, событий, пользователей и воронок
- **Веб-интерфейс** для просмотра данных и управления выгрузкой
- **Сохранение прогресса** и возможность продолжить прерванную выгрузку
- **Пакетное сохранение** для защиты от потери данных
- **Работа с долгосрочным токеном** AmoCRM
- **Автоматическое создание JSON файлов** в начале работы
- **Система предотвращения повторного экспорта** с отслеживанием экспортированных ID
- **Atomic операции и backup механизм** для надежности данных
- **Heartbeat мониторинг** активных экспортов с автоматической очисткой зависших процессов

## Новые возможности

### 🔥 Data Enrichment (Обогащение данных)
- **Автоматическое обогащение** сделок, контактов и компаний информацией о пользователях
- **Интеграция данных воронок** с автоматическим маппингом статусов и названий
- **Кэширование данных пользователей и воронок** для ускорения процесса
- **Настраиваемые правила обогащения** через веб-интерфейс

### 🔄 Custom Fields Flattening (Сплющивание кастомных полей)
- **Интеллектуальное сплющивание** сложных структур AmoCRM
- **Поддержка мультиселектов** и связанных сущностей
- **Человекочитаемые названия полей** вместо ID
- **Background синхронизация** сплющенных данных
- **Полнотекстовый поиск** по сплющенным данным

### 📊 Advanced Sampling (Улучшенная выборка данных)
- **Умная выборка** на основе статистики заполненности полей
- **Оптимизированные MongoDB запросы** с использованием aggregation pipeline
- **Параллельная обработка** статистики полей
- **Кэширование результатов** с настраиваемым TTL

### ⚡ Performance Monitoring (Мониторинг производительности)
- **Автоматическое отслеживание** времени выполнения MongoDB операций
- **Детекция медленных запросов** с настраиваемым threshold
- **Статистика производительности** по всем операциям
- **REST API** для мониторинга производительности

### 🧪 Benchmark Testing (Тесты производительности)
- **Комплексное тестирование** всех компонентов системы
- **Статистический анализ** производительности с множественными итерациями
- **Отчеты производительности** в человекочитаемом формате
- **Сохранение результатов** с временными метками

## Структура проекта

Проект имеет современную модульную структуру Python пакета:

```
AMO_v2/
├── src/                                # Исходный код приложения
│   └── amocrm_exporter/               # Основной пакет
│       ├── __init__.py                # Инициализация пакета
│       ├── cli.py                     # CLI интерфейс
│       │
│       ├── core/                      # Основные модули системы
│       │   ├── config.py              # Конфигурация приложения
│       │   ├── api.py                 # Работа с API AmoCRM
│       │   ├── auth.py                # Аутентификация
│       │   ├── logger.py              # Система логирования
│       │   └── api_responses.py       # Обработка ответов API
│       │
│       ├── exporters/                 # Модули экспорта данных
│       │   ├── parallel_exporter.py   # Параллельный экспорт
│       │   ├── excel_exporter.py      # Экспорт в Excel
│       │   └── sheets_exporter.py     # Экспорт в Google Sheets
│       │
│       ├── processors/                # Обработка данных
│       │   ├── batch_processor.py     # Пакетная обработка
│       │   ├── flattening_processor.py # Сплющивание данных
│       │   └── precomputed_statistics.py # Предвычисленная статистика
│       │
│       ├── enrichment/                # Обогащение данных
│       │   ├── data_enrichment.py     # Основное обогащение
│       │   └── smart_field_detector.py # Умное детектирование полей
│       │
│       ├── storage/                   # Хранение данных
│       │   ├── storage.py             # Основные операции хранения
│       │   ├── cache_manager.py       # Управление кэшем
│       │   └── state_manager.py       # Управление состоянием
│       │
│       ├── workers/                   # Фоновые задачи
│       │   ├── worker.py              # Основной worker
│       │   ├── run_worker.py          # Запуск worker'а
│       │   ├── background_statistics_updater.py # Обновление статистики
│       │   ├── monitoring.py          # Мониторинг
│       │   └── message_broker.py      # Message broker
│       │
│       ├── utils/                     # Утилиты
│       │   ├── exceptions.py          # Исключения
│       │   ├── entity_types.py        # Типы сущностей
│       │   ├── rate_limiter.py        # Ограничитель скорости
│       │   ├── backup_manager.py      # Управление резервными копиями
│       │   └── resilience.py          # Устойчивость к сбоям
│       │
│       └── web/                       # Веб-интерфейс
│           ├── modern_ui_server.py    # FastAPI сервер
│           ├── export_settings.py     # Настройки экспорта
│           └── templates/             # HTML шаблоны
│
├── tests/                             # Тесты
├── docs/                              # Документация
├── scripts/                           # Вспомогательные скрипты
├── config/                            # Конфигурационные файлы
├── data/                              # Данные (создается автоматически)
│
├── main.py                            # Основная точка входа для экспорта
├── web_server.py                      # Точка входа для веб-сервера
├── setup.py                           # Установочный скрипт
├── pyproject.toml                     # Современная конфигурация проекта
├── requirements.txt                   # Зависимости Python
├── PROJECT_STRUCTURE.md               # Подробная документация структуры
│
├── run_modern_ui.sh                   # Скрипт запуска для Linux/Mac
├── run_modern_ui.bat                  # Скрипт запуска для Windows
│
├── docker-compose.yml                 # Docker Compose конфигурация
├── Dockerfile                         # Docker образ
└── .env                               # Переменные окружения
```

📖 **Подробная документация**: См. [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)

## Требования

- Python 3.7 или выше
- Долгосрочный токен доступа AmoCRM
- Установленные зависимости из `requirements.txt`:
  - requests
  - python-dotenv
  - APScheduler
  - fastapi
  - uvicorn
  - jinja2
  - pydantic-settings
  - pymongo (для работы с MongoDB)
- (опционально) Docker и docker-compose для запуска в контейнере
- (опционально) MongoDB для хранения данных в базе

## Быстрый старт

1. **Клонирование репозитория**:
   ```bash
   git clone https://github.com/your-repo/amocrm-data-exporter.git
   cd amocrm-data-exporter
   ```

2. **Установка зависимостей**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Настройка подключения**:
   - Создайте файл `.env` с вашими настройками:
   ```
   # AmoCRM long-term token - получите в настройках интеграции AmoCRM
   LONGTERM_TOKEN=ваш_долгосрочный_токен_здесь

   # AmoCRM domain - ваш субдомен в AmoCRM
   AMOCRM_DOMAIN=ваш_субдомен.amocrm.ru

   # API domain (из токена, обычно api-a.amocrm.ru)
   API_DOMAIN=api-a.amocrm.ru

   # MongoDB settings (опционально)
   MONGO_URL=mongodb://localhost:27017/amo_db
   ```

4. **Запуск приложения**:

   **Способ 1: Через скрипты (рекомендуется)**
   - В Linux/Mac:
     ```bash
     bash run_modern_ui.sh
     ```
   - В Windows:
     ```
     run_modern_ui.bat
     ```

   **Способ 2: Прямой запуск**
   ```bash
   # Веб-интерфейс
   python web_server.py

   # Основной экспорт
   python main.py
   ```

   **Способ 3: После установки пакета**
   ```bash
   # Установка в dev режиме
   pip install -e .

   # Запуск через CLI команды
   amocrm-web          # Веб-интерфейс
   amocrm-exporter     # Основной экспорт
   amocrm-worker       # Worker для фоновых задач
   ```

## Docker

Проект поддерживает запуск через Docker с интеграцией MongoDB.

### Запуск полного стека

```bash
# Запуск приложения с MongoDB и Mongo Express
docker-compose up -d
```

Это запустит:
- Приложение AMO Data Exporter на порту 8000
- MongoDB на порту 27017
- Mongo Express (веб-интерфейс для MongoDB) на порту 8081

### Запуск только MongoDB для локальной разработки

```bash
# Запуск только MongoDB и Mongo Express
docker-compose -f local.docker-compose.yml up -d
```

### Доступ к Mongo Express

Mongo Express будет доступен по адресу: http://localhost:8081
- Логин: admin
- Пароль: admin

## Работа с веб-интерфейсом

### Современный интерфейс

Для запуска современного интерфейса на FastAPI:
```bash
# Linux/Mac
bash run_modern_ui.sh

# Windows
run_modern_ui.bat
```

Современный интерфейс включает:
- **Дашборд** с количеством выгруженных сущностей
- **Кнопки управления** для запуска/остановки экспорта
- **Журнал операций** с последними событиями
- **Автоматическое обновление** каждые 10 секунд

### Функции веб-интерфейса:

1. **Просмотр статистики**: количество выгруженных сделок, контактов, компаний и событий
2. **Управление экспортом**:
   - Запуск экспорта всех данных
   - Запуск экспорта отдельных типов данных
   - Перезапуск экспорта с начала
   - Остановка всех текущих операций экспорта
3. **Мониторинг процесса**: отображение текущего статуса и прогресса экспорта
4. **Просмотр журнала**: последние операции и ошибки
5. **Управление хранилищем**: переключение между JSON и MongoDB

## Data Enrichment (Обогащение данных)

Система автоматического обогащения данных позволяет расширить экспортируемые данные дополнительной информацией для более качественного анализа.

### Возможности обогащения:

#### Обогащение пользовательской информацией
```python
# Автоматически добавляются поля для всех сущностей:
# responsible_user_id_name - имя ответственного
# responsible_user_id_email - email ответственного
# created_by_name - имя создателя
# updated_by_name - имя кто обновил
```

#### Обогащение данными воронок (для сделок)
```python
# Добавляются поля:
# pipeline_name - название воронки
# status_name - название статуса
# status_color - цвет статуса
# pipeline_is_main - главная ли воронка
```

### Настройка обогащения

В веб-интерфейсе во вкладке "Export Settings" можно:
- Включить/выключить обогащение пользователями
- Включить/выключить обогащение воронками
- Выбрать конкретные поля для обогащения
- Просмотреть preview обогащенных данных

### API для управления обогащением

```bash
# Получить настройки обогащения
GET /api/enrichment/settings

# Обновить настройки обогащения
POST /api/enrichment/settings
{
  "enrich_users": true,
  "enrich_pipelines": true,
  "custom_fields": ["responsible_user_id", "created_by"]
}

# Принудительно обновить кэш пользователей/воронок
POST /api/enrichment/refresh-cache
```

## Flattening System (Система сплющивания)

Система сплющивания custom_fields преобразует сложные структуры AmoCRM в плоские поля для улучшения sampling и анализа данных.

### Что сплющивается:

#### Custom Fields (Кастомные поля)
```javascript
// До сплющивания:
"custom_fields_values": [
  {
    "field_id": 123,
    "field_name": "Бюджет",
    "values": [{"value": "100000"}]
  }
]

// После сплющивания:
"custom_field_123": "100000",
"custom_field_budget": "100000"
```

#### Мультиселекты и связанные сущности
```javascript
// Мультиселект с несколькими значениями:
"custom_field_456_0": "Значение 1",
"custom_field_456_1": "Значение 2",
"custom_field_456_all": "Значение 1; Значение 2",
"custom_field_456_count": 2
```

#### Теги и связанные объекты
```javascript
"tags_names": "VIP; Постоянный клиент",
"tags_ids": "1; 5",
"tags_count": 2,
"embedded_contacts_names": "Иван Иванов; Петр Петров"
```

### Background синхронизация

Flattening Processor автоматически:
- Синхронизирует данные каждые 30 минут
- Обрабатывает только изменившиеся записи
- Создает отдельные коллекции `{entity_type}_flattened`
- Поддерживает полнотекстовый поиск

### API для управления flattening

```bash
# Статус процесса
GET /api/flattening/status

# Запуск/остановка процесса
POST /api/flattening/start
POST /api/flattening/stop

# Принудительная синхронизация
POST /api/flattening/sync/deals

# Поиск по сплющенным данным
GET /api/flattening/search/deals?q=VIP

# Статистика полей
GET /api/flattening/statistics/deals
```

## Performance Monitoring (Мониторинг производительности)

Система автоматического мониторинга производительности MongoDB операций.

### Возможности мониторинга:

#### Автоматическое отслеживание
- Время выполнения всех MongoDB операций
- Использование памяти (при наличии psutil)
- Детекция медленных запросов (>1 сек по умолчанию)
- Статистика по типам операций

#### Метрики производительности
```javascript
{
  "operation": "get_entities",
  "count": 150,
  "avg_duration": 0.25,
  "min_duration": 0.12,
  "max_duration": 1.8,
  "slow_queries": 5
}
```

### API мониторинга производительности

```bash
# Статистика всех операций
GET /api/performance/stats

# Статистика конкретной операции
GET /api/performance/stats/save_entities

# Медленные операции
GET /api/performance/slow-operations?limit=20

# Настройка threshold медленных запросов
GET /api/performance/threshold
POST /api/performance/threshold
{
  "threshold": 2.0  // секунды
}

# Сброс статистики
POST /api/performance/reset-stats
```

### Логирование медленных операций

Операции превышающие threshold автоматически логируются:
```
WARNING: Slow operation: get_entities took 2.15s on deals
```

## Benchmark Tests (Тесты производительности)

Комплексная система тестирования производительности всех компонентов системы.

### Категории тестов:

#### Storage Operations (Операции хранилища)
- save_entities, append_entities
- get_entities, get_entity_count
- Bulk операции и индексы

#### Sampling Operations (Операции выборки)
- Статистика полей
- Умная выборка
- Aggregation pipeline

#### Data Enrichment (Обогащение данных)
- Обогащение пользователями
- Сплющивание custom_fields
- Batch обработка

#### Export Settings (Настройки экспорта)
- Генерация sample данных
- Анализ структуры полей

#### Flattened Data (Сплющенные данные)
- Синхронизация данных
- Полнотекстовый поиск
- Статистика flattened полей

### Запуск benchmark тестов

#### Через API
```bash
# Запуск всех тестов
POST /api/benchmark/run
{
  "categories": ["all"]
}

# Запуск конкретных категорий
POST /api/benchmark/run
{
  "categories": ["storage", "sampling"]
}

# Доступные категории
GET /api/benchmark/available-categories
```

#### Через командную строку
```bash
# Запуск всех тестов
python benchmark_tests.py

# Результаты сохраняются в benchmark_results_YYYYMMDD_HHMMSS.json
```

### Пример отчета
```
================================================================================
AMOCRM EXPORT SYSTEM BENCHMARK REPORT
================================================================================
Timestamp: 2025-01-28T15:30:00
Total Duration: 45.67s

System Information:
  Platform: Windows-10-10.0.19045
  Python: 3.11.0
  CPU Cores: 8
  Memory: 16.0 GB

STORAGE BENCHMARKS:
----------------------------------------
  save_entities:
    Mean: 125.3ms
    Median: 118.5ms
    Min: 95.2ms
    Max: 180.7ms
    Success Rate: 3/3

  get_entities:
    Mean: 45.8ms
    Median: 44.2ms
    Min: 38.1ms
    Max: 56.3ms
    Success Rate: 5/5
```

## Использование CLI

Вы можете управлять экспортом через командную строку:

```bash
# Выгрузить все данные
python modern_ui_server.py --fetch-all

# Выгрузить только сделки
python modern_ui_server.py --fetch-deals

# Выгрузить контакты и компании
python modern_ui_server.py --fetch-contacts --fetch-companies

# Запустить только веб-сервер
python modern_ui_server.py

# Перезапустить выгрузку с начала (очистить состояние)
python modern_ui_server.py --fetch-all --force-restart

# Изменить размер пакета сохранения (по умолчанию 10 страниц)
python modern_ui_server.py --fetch-all --batch-size 20
```

### Параметры командной строки:

| Параметр | Описание |
|----------|----------|
| `--fetch-all` | Выгрузить все типы данных |
| `--fetch-deals` | Выгрузить только сделки |
| `--fetch-contacts` | Выгрузить только контакты |
| `--fetch-companies` | Выгрузить только компании |
| `--fetch-events` | Выгрузить только события |
| `--force-restart` | Начать выгрузку с начала (очистить прогресс) |
| `--batch-size N` | Количество страниц перед сохранением |
| `--port N` | Порт для веб-сервера (по умолчанию 8000) |
| `--use-mongo` | Использовать MongoDB вместо JSON-файлов |

## Описание ключевых файлов

### Основные модули системы

#### `modern_ui_server.py`
Основной скрипт запуска современного веб-интерфейса на FastAPI. Обрабатывает аргументы командной строки и инициализирует все компоненты системы.

#### `api.py`
Модуль для взаимодействия с API AmoCRM. Реализует:
- Управление скоростью запросов (rate limiting)
- Получение данных с пагинацией
- Обработку ошибок API
- Методы для получения сделок, контактов, компаний и событий

#### `auth.py`
Модуль аутентификации, отвечающий за:
- Загрузку и хранение долгосрочного токена AmoCRM
- Валидацию токена
- Обновление токена при необходимости

#### `config.py`
Конфигурация приложения, загружает настройки из .env файла:
- URL и параметры подключения к API
- Пути к файлам данных
- Лимиты запросов к API
- Параметры пагинации
- Настройки подключения к MongoDB

#### `storage.py`
Модуль для работы с хранилищем данных:
- Чтение/запись JSON файлов
- Атомарные операции записи через временные файлы
- Обновление отдельных сущностей
- Получение статистики по выгруженным данным
- Поддержка MongoDB как альтернативного хранилища

#### `state_manager.py`
Управление состоянием экспорта:
- Сохранение прогресса выгрузки
- Отслеживание запущенных процессов экспорта
- Возможность продолжить выгрузку с места остановки

#### `parallel_exporter.py`
Параллельная выгрузка данных:
- Многопоточный экспорт разных типов данных
- Контроль за выполнением процессов
- Обработка ошибок и повторные попытки
- Пакетное сохранение для оптимизации производительности

#### `logger.py`
Система логирования:
- Запись логов в JSON-файл или MongoDB
- Буферизация логов до инициализации хранилища
- Ротация логов по сроку давности
- Вывод логов в консоль

## Экспортируемые данные

Система поддерживает экспорт следующих типов данных из AmoCRM:

### 📊 Сделки (Leads/Deals)
- **Основные поля**: ID, название, сумма, статус, воронка, дата создания/обновления
- **Ответственные**: ответственный пользователь, создатель, кто обновил
- **Даты**: создания, обновления, закрытия, ближайшей задачи
- **Custom fields**: все кастомные поля с автоматическим сплющиванием
- **Связи**: прикрепленные контакты и компании
- **Обогащение**: данные пользователей и воронок (названия, статусы, цвета)

### 👥 Контакты (Contacts)
- **Основные поля**: ID, имя, фамилия, должность, телефоны, email
- **Ответственные**: ответственный пользователь, создатель
- **Custom fields**: все кастомные поля контактов
- **Связи**: прикрепленные компании и сделки
- **Обогащение**: информация о пользователях

### 🏢 Компании (Companies)
- **Основные поля**: ID, название, тип, адрес, телефоны, email, сайт
- **Ответственные**: ответственный пользователь, создатель
- **Custom fields**: все кастомные поля компаний
- **Связи**: прикрепленные контакты и сделки
- **Обогащение**: информация о пользователях

### 📅 События (Events)
- **Основные поля**: ID, тип события, дата создания
- **Связи**: связанная сущность (сделка, контакт, компания)
- **Данные**: параметры и метаданные события
- **Пользователи**: кто создал событие

### 👤 Пользователи (Users) 🆕
- **Основные поля**: ID, имя, email, язык, роль
- **Группы**: ID группы, права доступа
- **Статус**: активен ли пользователь
- **Настройки**: часовой пояс, настройки уведомлений
- **Использование**: для обогащения других сущностей

### 🔄 Воронки (Pipelines) 🆕
- **Основные поля**: ID, название, сортировка
- **Статусы**: все статусы воронки с названиями и цветами
- **Настройки**: главная ли воронка, архивная ли
- **Использование**: для обогащения сделок статусной информацией

### 🔧 Техническая информация
- **Формат**: JSON документы в MongoDB или JSON файлы
- **Кодировка**: UTF-8
- **Структура**: максимально приближена к AmoCRM API
- **Дополнения**: обогащенные поля с префиксами `_name`, `_email` и т.д.

### 📈 Статистика экспорта
- **Счетчики**: количество экспортированных записей по типам
- **Прогресс**: текущая страница и общий прогресс экспорта
- **Ошибки**: логирование ошибок и пропущенных записей
- **Производительность**: метрики времени выполнения операций

### 🔍 Возможности поиска
- **MongoDB**: полнотекстовый поиск, индексы, aggregation
- **Flattened данные**: упрощенный поиск по сплющенным custom fields
- **Фильтрация**: по датам, статусам, пользователям
- **Сортировка**: по любым полям с индексами

## Решение типичных проблем

### Если не работает веб-интерфейс:

1. **Порт занят другим приложением**
   - Убедитесь, что порт 8000 (или указанный вами) не занят
   - Попробуйте указать другой порт: `python modern_ui_server.py --port 8080`

2. **Отсутствуют зависимости**
   - Убедитесь, что установлены все пакеты: `pip install -r requirements.txt`

3. **Проблемы с правами доступа**
   - Убедитесь, что у приложения есть права на создание и запись файлов в директорию `data`

### Если выгрузка останавливается или завершается с ошибкой:

1. **Проблемы с токеном**
   - Проверьте валидность токена и его права доступа
   - Убедитесь, что токен не истек и имеет необходимые права

2. **Ошибки API**
   - Посмотрите логи на наличие ошибок API
   - Проверьте лимиты запросов в AmoCRM

3. **Прерывание выгрузки**
   - Запустите выгрузку повторно - она продолжится с места остановки
   - Если нужно начать заново, используйте `--force-restart`

### Проблемы с MongoDB:

1. **Нет подключения к MongoDB**
   - Убедитесь, что MongoDB запущена и доступна
   - Проверьте настройки подключения в файле .env
   - При использовании Docker убедитесь, что контейнеры запущены и сеть настроена

2. **Ошибки при сохранении данных**
   - Проверьте права доступа MongoDB
   - Убедитесь, что у MongoDB достаточно места на диске

## Расширение функциональности

### Добавление новых типов данных

Для добавления нового типа данных (например, "Задачи"):

1. Добавьте новый метод в `api.py`:
   ```python
   def get_tasks_page(self, page: int) -> tuple[list[dict[str, Any]], bool]:
       """Get a specific page of tasks"""
       return self._get_entity_page('tasks', page)
   ```

2. Добавьте метод экспорта в `parallel_exporter.py`:
   ```python
   def export_tasks(self, force_restart: bool = False, batch_save: bool = True, batch_size: int = 10):
       """Export tasks in a separate thread"""
       self._start_export_thread('tasks', self._export_tasks_worker, force_restart, batch_save, batch_size)

   def _export_tasks_worker(self, batch_save: bool = True, batch_size: int = 10):
       """Worker function for exporting tasks"""
       try:
           self._export_entities_worker('tasks', self.api.get_tasks_page, batch_save, batch_size)
       except Exception as e:
           log_event('exporter', 'error', f'Error in tasks export worker: {e}')
       finally:
           self.state_manager.mark_export_stopped('tasks')
   ```

3. Обновите конфигурацию в `config.py` для нового типа данных

4. Добавьте опцию в `modern_ui_server.py`

### Использование альтернативных баз данных

В дополнение к MongoDB можно добавить поддержку других баз данных:

1. Создайте новый класс-адаптер в `storage.py` для работы с нужной БД
2. Реализуйте методы для чтения, записи и обновления данных
3. Обновите конфигурацию и добавьте параметры для новой БД

### Оптимизация производительности

Для ускорения экспорта данных:

1. **Увеличьте количество страниц в пакете**:
   ```bash
   python modern_ui_server.py --fetch-all --batch-size 30
   ```

2. **Настройте лимиты запросов** в `config.py`:
   ```python
   MAX_REQUESTS_PER_SECOND = 10  # Увеличьте в соответствии с лимитами AmoCRM
   ```

3. **Измените размер страницы**:
   ```python
   PAGE_SIZE = 100  # Максимально допустимое значение для AmoCRM
   ```

4. **Используйте индексы в MongoDB** для ускорения запросов

## Scalable Worker System

The system now implements a scalable worker architecture using RabbitMQ and FastStream for distributed task processing. This allows for better resource utilization and horizontal scaling.

### Key Components

- **Message Broker (RabbitMQ)**: Central message queue for distributing export tasks
- **FastStream Framework**: Integration with RabbitMQ for worker orchestration
- **Worker Processes**: Dedicated processes that handle export tasks
- **Task Queue**: Prioritized queue for export jobs

### Running the Worker System

#### Standard Configuration

```bash
# Start the entire system with RabbitMQ
bash run_resource_constrained.sh
```

This will start:
- RabbitMQ message broker on port 5672
- RabbitMQ management UI on port 15672
- Two worker processes for export tasks
- MongoDB for storage
- Mongo Express UI on port 8081

#### Minimal Resource Configuration

For systems with very limited resources (1 CPU, ~1.8GB RAM):

```bash
# Start minimal configuration
bash run_resource_constrained.sh --minimal
```

This starts a resource-optimized configuration:
- RabbitMQ (without management UI)
- Single worker process
- MongoDB (without Mongo Express)
- API server with minimal resource usage

### Managing the Worker System

- **View logs**: `bash run_resource_constrained.sh --logs`
- **Shutdown**: `bash run_resource_constrained.sh --down`

### API for Task Management

The modern UI now includes API endpoints for managing export tasks:

- `POST /api/tasks/export_deals` - Create deal export task
- `POST /api/tasks/export_contacts` - Create contact export task
- `POST /api/tasks/export_companies` - Create company export task
- `POST /api/tasks/export_events` - Create event export task
- `POST /api/tasks/export_all` - Create tasks for all entity types
- `GET /api/tasks` - List all tasks
- `GET /api/tasks/{task_id}` - Get task details
- `GET /api/workers` - Get worker status
- `GET /health` - Check system health

### Resource Optimization

The system includes several optimizations for resource-constrained environments:

1. **RabbitMQ Settings**:
   - Memory watermark limits
   - Reduced statistics collection

2. **Worker Process Management**:
   - Configurable worker count
   - Batch size optimization
   - Retry mechanisms with backoff

3. **Docker Resource Limits**:
   - CPU and memory constraints for each service
   - Minimal container configuration options

4. **MongoDB Optimization**:
   - Reduced cache size
   - Constrained memory usage

## Message Queue-Based Worker System

The application now supports a scalable, distributed worker system using RabbitMQ and FastStream. This architecture allows for better scalability, resilience, and resource utilization.

### Features

- **Distributed Processing**: Run multiple worker processes across different machines
- **Message Durability**: Tasks are persisted in RabbitMQ queues, surviving application restarts
- **Automatic Retries**: Failed tasks are automatically retried
- **Priority Queuing**: Tasks can be prioritized for execution
- **Monitoring**: Task status and progress can be tracked in real-time
- **Horizontal Scaling**: Add more workers to handle increased load

### Architecture

The system consists of the following components:

1. **RabbitMQ**: Message broker for task distribution
2. **FastStream**: Python library for interacting with RabbitMQ
3. **Worker Processes**: Consume tasks from queues and process them
4. **API Server**: Publishes tasks to queues and provides status information

### Running with Docker Compose

The easiest way to run the system is using the provided Docker Compose file:

```bash
docker-compose -f rabbitmq.docker-compose.yml up -d
```

This will start:
- RabbitMQ server with management UI (port 15672)
- Multiple worker instances
- API server (port 8000)
- MongoDB (port 27017)
- Mongo Express UI (port 8081)

### Running Workers Manually

To run workers manually:

```bash
# Install dependencies
pip install -r requirements.txt

# Run a worker with 4 processes
python run_worker.py --workers 4

# Enable hot reload for development
python run_worker.py --reload

# Connect to a specific RabbitMQ instance
python run_worker.py --host rabbitmq.example.com --port 5672 --user myuser --password mypass
```

### Environment Variables

The following environment variables can be set to configure the system:

- `RABBITMQ_HOST`: RabbitMQ server hostname
- `RABBITMQ_PORT`: RabbitMQ server port
- `RABBITMQ_USER`: RabbitMQ username
- `RABBITMQ_PASSWORD`: RabbitMQ password
- `RABBITMQ_URL`: Full RabbitMQ connection URL (overrides individual settings)

### Monitoring

You can monitor the RabbitMQ server using the management UI:
- URL: http://localhost:15672
- Username: guest
- Password: guest

### Resource-Constrained Deployment

For systems with limited resources, two Docker Compose configurations are provided:

1. **Standard Resource-Limited Configuration**:
   ```bash
   docker-compose -f rabbitmq.docker-compose.yml up -d
   ```
   - Suitable for systems with ~1 CPU and ~2GB RAM
   - Includes RabbitMQ with management UI, 2 worker processes, API server, MongoDB, and Mongo Express
   - Each service has CPU and memory limits configured

2. **Minimal Development Configuration**:
   ```bash
   docker-compose -f minimal.docker-compose.yml up -d
   ```
   - For very resource-constrained environments (less than 1 CPU and 1GB RAM)
   - Includes RabbitMQ (without management UI), 1 worker process, API server, and MongoDB
   - No Mongo Express admin UI to save resources
   - Minimal configuration settings for all services

#### Using the Resource Constrained Script

For convenience, a script is provided to easily run the application in resource-constrained mode:

```bash
# Start with standard resource-limited configuration
./run_resource_constrained.sh

# Start with minimal configuration for very limited resources
./run_resource_constrained.sh --minimal

# View logs
./run_resource_constrained.sh --logs

# Stop all services
./run_resource_constrained.sh --down
```

The script sets appropriate environment variables to optimize the application for limited resources:
- Reduced retry count for failed tasks
- Smaller batch sizes
- Longer retry delays to reduce resource contention

#### Resource Allocation

The configurations allocate resources as follows:

**Standard configuration**:
- RabbitMQ: 0.3 CPU, 400MB RAM
- Worker: 0.3 CPU, 400MB RAM (2 worker processes)
- API: 0.2 CPU, 300MB RAM
- MongoDB: 0.15 CPU, 500MB RAM
- Mongo Express: 0.05 CPU, 200MB RAM

**Minimal configuration**:
- RabbitMQ: 0.2 CPU, 300MB RAM
- Worker: 0.2 CPU, 300MB RAM (1 worker process)
- API: 0.1 CPU, 200MB RAM
- MongoDB: 0.1 CPU, 300MB RAM

These allocations ensure that the application can run efficiently even on resource-constrained environments.