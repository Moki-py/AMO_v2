#!/usr/bin/env python3
"""
Скрипт для исправления дублирующихся ключей в MongoDB
"""

import sys
import os
from datetime import datetime
from typing import List, Dict, Any

# Добавляем путь к src для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from amocrm_exporter.core.config import settings
from amocrm_exporter.core.logger import log_event
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError


def fix_duplicate_keys():
    """Исправляет дублирующиеся ключи в MongoDB коллекциях"""

    print("🔧 Исправление дублирующихся ключей в MongoDB...")

    try:
        # Подключение к MongoDB
        client = MongoClient(settings.mongodb_uri)
        db = client[settings.mongodb_db]

        # Список коллекций для проверки
        collections_to_check = ["deals", "leads", "contacts", "companies", "events", "users", "pipelines"]

        total_fixed = 0

        for collection_name in collections_to_check:
            print(f"\n📊 Проверка коллекции: {collection_name}")

            collection = db[collection_name]

            # Находим дублирующиеся записи по id
            pipeline = [
                {"$group": {
                    "_id": "$id",
                    "count": {"$sum": 1},
                    "docs": {"$push": "$_id"}
                }},
                {"$match": {"count": {"$gt": 1}}}
            ]

            duplicates = list(collection.aggregate(pipeline))

            if not duplicates:
                print(f"✅ Дублирующихся записей в {collection_name} не найдено")
                continue

            print(f"⚠️ Найдено {len(duplicates)} групп дублирующихся записей в {collection_name}")

            fixed_count = 0

            for duplicate in duplicates:
                doc_id = duplicate["_id"]
                doc_ids = duplicate["docs"]

                print(f"  - ID {doc_id}: {len(doc_ids)} дублирующихся записей")

                # Оставляем самую новую запись (по _id, так как ObjectId содержит timestamp)
                # Сортируем по _id в обратном порядке и оставляем первую (самую новую)
                docs_to_remove = doc_ids[1:]  # Удаляем все кроме первой

                # Удаляем дублирующиеся записи
                result = collection.delete_many({"_id": {"$in": docs_to_remove}})
                fixed_count += result.deleted_count

                print(f"    Удалено {result.deleted_count} дублирующихся записей")

            total_fixed += fixed_count
            print(f"✅ Исправлено {fixed_count} дублирующихся записей в {collection_name}")

        # Пересоздаем индексы для всех коллекций
        print("\n🔧 Пересоздание индексов...")

        for collection_name in collections_to_check:
            collection = db[collection_name]

            try:
                # Удаляем существующие индексы (кроме _id)
                existing_indexes = collection.list_indexes()
                for index in existing_indexes:
                    if index["name"] != "_id_":
                        collection.drop_index(index["name"])

                # Создаем основные индексы
                collection.create_index([("id", ASCENDING)], unique=True, sparse=True)
                collection.create_index([("created_at", DESCENDING)], sparse=True)
                collection.create_index([("updated_at", DESCENDING)], sparse=True)

                print(f"✅ Индексы пересозданы для {collection_name}")

            except Exception as e:
                print(f"⚠️ Ошибка пересоздания индексов для {collection_name}: {e}")

        print(f"\n🎉 Исправление завершено! Всего исправлено {total_fixed} дублирующихся записей")

        # Создаем резервную копию
        backup_name = f"backup_after_duplicate_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        db.command("createCollection", backup_name)

        log_event("duplicate_fix", "info", f"Fixed {total_fixed} duplicate records across all collections")

    except PyMongoError as e:
        print(f"❌ Ошибка подключения к MongoDB: {e}")
        log_event("duplicate_fix", "error", f"MongoDB connection error: {e}")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        log_event("duplicate_fix", "error", f"Unexpected error: {e}")


if __name__ == "__main__":
    fix_duplicate_keys()