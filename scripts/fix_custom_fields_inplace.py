#!/usr/bin/env python3
"""
Скрипт для исправления кастомных полей в существующих данных MongoDB

Этот скрипт обновляет документы в базе данных, добавляя дружелюбные названия
кастомных полей на основе field_name из AmoCRM HAL формата.

Использование:
    python scripts/fix_custom_fields_inplace.py --dry-run  # Показать что будет изменено
    python scripts/fix_custom_fields_inplace.py --backup  # Создать backup перед изменениями
    python scripts/fix_custom_fields_inplace.py --fix     # Применить исправления
"""

import sys
import os
import json
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional

# Добавляем путь к модулям проекта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    import pymongo
    from pymongo import MongoClient
    # Простое получение конфигурации из переменных окружения
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:4RFV5tgb6YHN@5.129.201.34:27017')
    mongodb_db = os.getenv('MONGODB_DB', 'amocrm_exporter')
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что все зависимости установлены: pip install -r requirements.txt")
    sys.exit(1)


class CustomFieldsFixer:
    """Класс для исправления кастомных полей в данных"""

    def __init__(self, mongodb_uri: str, database_name: str) -> None:
        self.client: MongoClient = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.stats: Dict[str, int] = {
            'documents_processed': 0,
            'documents_updated': 0,
            'fields_added': 0,
            'errors': 0
        }

    def sanitize_field_name(self, field_name: str) -> str:
        """Санитизация имени поля для использования в качестве ключа"""
        return (field_name
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
                .replace('/', '_')
                .replace('\\', '_')
                .replace('[', '')
                .replace(']', '')
                .replace('-', '_')
                .lower())

    def process_custom_fields(self, custom_fields_values: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Обработка кастомных полей и создание дружелюбных названий"""
        new_fields: Dict[str, Any] = {}

        for field in custom_fields_values:
            if not isinstance(field, dict):
                continue

            field_id = field.get('field_id')
            field_name = field.get('field_name', '')
            values = field.get('values', [])

            if not field_id or not values:
                continue

            # Создаем дружелюбное имя поля
            if field_name:
                sanitized_name = self.sanitize_field_name(field_name)
                primary_key = sanitized_name
            else:
                primary_key = f"custom_field_{field_id}"

            # Извлекаем значения
            if len(values) == 1:
                # Одно значение
                value = values[0]
                if isinstance(value, dict) and 'value' in value:
                    extracted_value = value['value']
                else:
                    extracted_value = str(value)

                new_fields[primary_key] = extracted_value

            else:
                # Множественные значения
                all_values = []
                for i, value in enumerate(values):
                    if isinstance(value, dict) and 'value' in value:
                        extracted_value = value['value']
                    else:
                        extracted_value = str(value)

                    all_values.append(extracted_value)
                    new_fields[f"{primary_key}_{i}"] = extracted_value

                # Объединенные значения
                new_fields[f"{primary_key}_all"] = "; ".join(str(v) for v in all_values if v)
                new_fields[f"{primary_key}_count"] = len(all_values)

        return new_fields

    def backup_collection(self, collection_name: str) -> str:
        """Создание backup коллекции"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{collection_name}_backup_{timestamp}"

        print(f"📦 Создание backup коллекции {collection_name} -> {backup_name}")

        collection = self.db[collection_name]
        backup_collection = self.db[backup_name]

        # Копируем все документы
        documents = list(collection.find())
        if documents:
            backup_collection.insert_many(documents)

        print(f"✅ Backup создан: {len(documents)} документов")
        return backup_name

    def process_collection(self, collection_name: str, dry_run: bool = True) -> Dict[str, Any]:
        """Обработка коллекции"""
        collection = self.db[collection_name]

        # Найти документы с custom_fields_values
        query: Dict[str, Any] = {"custom_fields_values": {"$exists": True, "$ne": None, "$ne": []}}
        documents = list(collection.find(query))

        if not documents:
            print(f"🔍 Коллекция {collection_name}: документы с кастомными полями не найдены")
            return {'processed': 0, 'updated': 0}

        print(f"🔍 Коллекция {collection_name}: найдено {len(documents)} документов с кастомными полями")

        updated_count = 0
        fields_added_count = 0
        total_docs = len(documents)

        for i, doc in enumerate(documents):
            try:
                # Простой прогресс без tqdm
                if i % max(1, total_docs // 10) == 0:
                    progress = int((i / total_docs) * 100)
                    print(f"  📊 Прогресс: {progress}% ({i}/{total_docs})")

                custom_fields_values = doc.get('custom_fields_values', [])
                if not custom_fields_values:
                    continue

                # Обрабатываем кастомные поля
                new_fields = self.process_custom_fields(custom_fields_values)

                if new_fields:
                    if dry_run:
                        print(f"  📝 Документ {doc.get('id', 'unknown')}: добавится {len(new_fields)} полей")
                        for key, value in list(new_fields.items())[:3]:  # Показываем первые 3
                            print(f"    • {key}: {str(value)[:50]}...")
                        if len(new_fields) > 3:
                            print(f"    ... и еще {len(new_fields) - 3} полей")
                    else:
                        # Обновляем документ
                        update_data = {"$set": new_fields}
                        collection.update_one({"_id": doc["_id"]}, update_data)
                        updated_count += 1
                        fields_added_count += len(new_fields)

                self.stats['documents_processed'] += 1

            except Exception as e:
                print(f"❌ Ошибка обработки документа {doc.get('id', 'unknown')}: {e}")
                self.stats['errors'] += 1

        if not dry_run:
            self.stats['documents_updated'] += updated_count
            self.stats['fields_added'] += fields_added_count

        print(f"  ✅ Завершено: {total_docs} документов")
        return {'processed': len(documents), 'updated': updated_count}

    def run(self, collections: List[str], dry_run: bool = True, create_backup: bool = False) -> None:
        """Запуск обработки"""
        print("🚀 Начинаем исправление кастомных полей...")
        print(f"📊 Режим: {'DRY RUN (только просмотр)' if dry_run else 'РЕАЛЬНЫЕ ИЗМЕНЕНИЯ'}")

        if create_backup and not dry_run:
            print("\n📦 Создание backup...")
            for collection_name in collections:
                try:
                    self.backup_collection(collection_name)
                except Exception as e:
                    print(f"❌ Ошибка создания backup для {collection_name}: {e}")
                    return

        print(f"\n🔧 Обработка коллекций: {', '.join(collections)}")

        results: Dict[str, Any] = {}
        for collection_name in collections:
            print(f"\n{'='*50}")
            try:
                results[collection_name] = self.process_collection(collection_name, dry_run)
            except Exception as e:
                print(f"❌ Ошибка обработки коллекции {collection_name}: {e}")
                self.stats['errors'] += 1

        # Итоговая статистика
        print(f"\n{'='*50}")
        print("📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"  • Документов обработано: {self.stats['documents_processed']}")
        if not dry_run:
            print(f"  • Документов обновлено: {self.stats['documents_updated']}")
            print(f"  • Полей добавлено: {self.stats['fields_added']}")
        print(f"  • Ошибок: {self.stats['errors']}")

        if dry_run:
            print("\n💡 Для применения изменений запустите скрипт с флагом --fix")

    def close(self) -> None:
        """Закрытие соединения"""
        self.client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Исправление кастомных полей в MongoDB")

    # Режимы работы (взаимоисключающие)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--dry-run', action='store_true',
                           help='Показать что будет изменено (без изменения данных)')
    mode_group.add_argument('--fix', action='store_true',
                           help='Применить исправления к данным')

    # Опции
    parser.add_argument('--backup', action='store_true',
                       help='Создать backup перед изменениями')
    parser.add_argument('--collections', nargs='+',
                       default=['deals', 'contacts', 'companies', 'events'],
                       help='Коллекции для обработки')
    parser.add_argument('--mongodb-uri',
                       default=mongodb_uri,
                       help='URI подключения к MongoDB')
    parser.add_argument('--database',
                       default=mongodb_db,
                       help='Имя базы данных')

    args = parser.parse_args()

    print("🔧 Скрипт исправления кастомных полей AmoCRM")
    print("=" * 50)

    # Проверяем подключение к MongoDB
    try:
        client: MongoClient = MongoClient(args.mongodb_uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # Проверяем соединение
        available_collections = client[args.database].list_collection_names()
        client.close()

        print(f"✅ Подключение к MongoDB успешно")
        print(f"📁 База данных: {args.database}")
        print(f"📊 Доступные коллекции: {', '.join(available_collections)}")

        # Проверяем, что запрашиваемые коллекции существуют
        missing_collections = set(args.collections) - set(available_collections)
        if missing_collections:
            print(f"⚠️  Коллекции не найдены: {', '.join(missing_collections)}")
            args.collections = [c for c in args.collections if c in available_collections]
            if not args.collections:
                print("❌ Нет доступных коллекций для обработки")
                return

    except Exception as e:
        print(f"❌ Ошибка подключения к MongoDB: {e}")
        return

    # Запускаем обработку
    fixer = CustomFieldsFixer(args.mongodb_uri, args.database)

    try:
        if args.dry_run:
            fixer.run(args.collections, dry_run=True, create_backup=False)
        else:
            # Подтверждение для реальных изменений
            print(f"\n⚠️  ВНИМАНИЕ: Вы собираетесь изменить данные в коллекциях: {', '.join(args.collections)}")
            if args.backup:
                print("📦 Будет создан backup перед изменениями")

            confirm = input("\nПродолжить? (yes/no): ").lower().strip()
            if confirm != 'yes':
                print("❌ Операция отменена")
                return

            fixer.run(args.collections, dry_run=False, create_backup=args.backup)
            print("\n✅ Исправление завершено!")

    finally:
        fixer.close()


if __name__ == "__main__":
    main()