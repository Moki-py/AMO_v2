# Структура проекта AmoCRM Data Exporter v2.0

## Обзор новой организации

Проект был реорганизован в соответствии с современными стандартами Python разработки. Все модули теперь организованы в логические группы внутри пакета `amocrm_exporter`.

## Структура директорий

```
AMO_v2/
├── src/                                # Исходный код приложения
│   └── amocrm_exporter/               # Основной пакет
│       ├── __init__.py                # Инициализация пакета
│       ├── cli.py                     # CLI интерфейс и точки входа
│       │
│       ├── core/                      # Основные модули системы
│       │   ├── __init__.py
│       │   ├── config.py              # Конфигурация приложения
│       │   ├── api.py                 # Работа с API AmoCRM
│       │   ├── auth.py                # Аутентификация
│       │   ├── logger.py              # Система логирования
│       │   └── api_responses.py       # Обработка ответов API
│       │
│       ├── exporters/                 # Модули экспорта данных
│       │   ├── __init__.py
│       │   ├── parallel_exporter.py   # Параллельный экспорт
│       │   ├── excel_exporter.py      # Экспорт в Excel
│       │   └── sheets_exporter.py     # Экспорт в Google Sheets
│       │
│       ├── processors/                # Обработка данных
│       │   ├── __init__.py
│       │   ├── batch_processor.py     # Пакетная обработка
│       │   ├── flattening_processor.py # Сплющивание данных
│       │   └── precomputed_statistics.py # Предвычисленная статистика
│       │
│       ├── enrichment/                # Обогащение данных
│       │   ├── __init__.py
│       │   ├── data_enrichment.py     # Основное обогащение
│       │   └── smart_field_detector.py # Умное детектирование полей
│       │
│       ├── storage/                   # Хранение данных
│       │   ├── __init__.py
│       │   ├── storage.py             # Основные операции хранения
│       │   ├── cache_manager.py       # Управление кэшем
│       │   └── state_manager.py       # Управление состоянием
│       │
│       ├── workers/                   # Фоновые задачи
│       │   ├── __init__.py
│       │   ├── worker.py              # Основной worker
│       │   ├── run_worker.py          # Запуск worker'а
│       │   ├── background_statistics_updater.py # Обновление статистики
│       │   ├── monitoring.py          # Мониторинг
│       │   └── message_broker.py      # Message broker
│       │
│       ├── utils/                     # Утилиты
│       │   ├── __init__.py
│       │   ├── exceptions.py          # Исключения
│       │   ├── entity_types.py        # Типы сущностей
│       │   ├── rate_limiter.py        # Ограничитель скорости
│       │   ├── backup_manager.py      # Управление резервными копиями
│       │   └── resilience.py          # Устойчивость к сбоям
│       │
│       └── web/                       # Веб-интерфейс
│           ├── __init__.py
│           ├── modern_ui_server.py    # FastAPI сервер
│           ├── export_settings.py     # Настройки экспорта
│           └── templates/             # HTML шаблоны
│               ├── index.html
│               └── export-settings.html
│
├── tests/                             # Тесты
│   ├── unit/                          # Модульные тесты
│   └── integration/                   # Интеграционные тесты
│
├── docs/                              # Документация
├── scripts/                           # Вспомогательные скрипты
├── config/                            # Конфигурационные файлы
│
├── data/                              # Данные (создается автоматически)
├── exports/                           # Экспортированные файлы
├── mongo_data/                        # Данные MongoDB
│
├── main.py                            # Основная точка входа для экспорта
├── web_server.py                      # Точка входа для веб-сервера
├── setup.py                           # Установочный скрипт (legacy)
├── pyproject.toml                     # Современная конфигурация проекта
├── requirements.txt                   # Зависимости Python
├── README.md                          # Основная документация
│
├── run_modern_ui.sh                   # Скрипт запуска для Linux/Mac
├── run_modern_ui.bat                  # Скрипт запуска для Windows
│
├── docker-compose.yml                 # Docker Compose конфигурация
├── Dockerfile                         # Docker образ
├── Dockerfile.production              # Production Docker образ
└── production.docker-compose.yml      # Production конфигурация
```

## Основные модули

### Core (src/amocrm_exporter/core/)
- **config.py** - Центральная конфигурация с использованием pydantic-settings
- **api.py** - Работа с AmoCRM API, включая аутентификацию и запросы
- **auth.py** - Управление токенами и авторизацией
- **logger.py** - Система логирования с ротацией и уровнями
- **api_responses.py** - Обработка и валидация ответов API

### Exporters (src/amocrm_exporter/exporters/)
- **parallel_exporter.py** - Основной модуль параллельного экспорта данных
- **excel_exporter.py** - Экспорт данных в Excel файлы
- **sheets_exporter.py** - Интеграция с Google Sheets

### Processors (src/amocrm_exporter/processors/)
- **batch_processor.py** - Пакетная обработка больших объемов данных
- **flattening_processor.py** - Сплющивание сложных структур AmoCRM
- **precomputed_statistics.py** - Предвычисление статистики для ускорения

### Enrichment (src/amocrm_exporter/enrichment/)
- **data_enrichment.py** - Обогащение данных пользователями и воронками
- **smart_field_detector.py** - Интеллектуальное детектирование полей

### Storage (src/amocrm_exporter/storage/)
- **storage.py** - Основные операции с хранилищем (MongoDB/JSON)
- **cache_manager.py** - Управление кэшем Redis
- **state_manager.py** - Управление состоянием экспорта

### Workers (src/amocrm_exporter/workers/)
- **worker.py** - Основной фоновый worker
- **run_worker.py** - Запуск worker'а
- **background_statistics_updater.py** - Фоновое обновление статистики
- **monitoring.py** - Мониторинг производительности
- **message_broker.py** - Message broker для задач

### Utils (src/amocrm_exporter/utils/)
- **exceptions.py** - Кастомные исключения
- **entity_types.py** - Определения типов сущностей
- **rate_limiter.py** - Ограничение скорости запросов
- **backup_manager.py** - Управление резервными копиями
- **resilience.py** - Обеспечение устойчивости к сбоям

### Web (src/amocrm_exporter/web/)
- **modern_ui_server.py** - FastAPI веб-сервер
- **export_settings.py** - Управление настройками экспорта
- **templates/** - HTML шаблоны для интерфейса

## Точки входа

### CLI команды (после установки пакета)
```bash
# Основной экспорт
amocrm-exporter

# Веб-интерфейс
amocrm-web

# Worker для фоновых задач
amocrm-worker
```

### Прямой запуск скриптов
```bash
# Основной экспорт
python main.py

# Веб-интерфейс
python web_server.py

# Или через скрипты оболочки
./run_modern_ui.sh        # Linux/Mac
run_modern_ui.bat         # Windows
```

## Установка в режиме разработки

```bash
# Установка в editable режиме
pip install -e .

# Установка с dev зависимостями
pip install -e ".[dev]"
```

## Преимущества новой структуры

1. **Модульность** - четкое разделение функциональности
2. **Переиспользование** - легкий импорт модулей между компонентами
3. **Тестируемость** - простое написание unit и integration тестов
4. **Масштабируемость** - легкое добавление новых модулей
5. **Стандартность** - соответствие Python packaging стандартам
6. **CI/CD готовность** - структура готова для автоматизации

## Миграция с предыдущей версии

Старые скрипты остаются работоспособными благодаря точкам входа `main.py` и `web_server.py`. Для использования новой структуры рекомендуется:

1. Установить пакет: `pip install -e .`
2. Использовать новые CLI команды: `amocrm-web`, `amocrm-exporter`
3. Импортировать модули через новые пути: `from amocrm_exporter.core import config`