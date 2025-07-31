#!/usr/bin/env python3
"""
Скрипт для исправления всех найденных ошибок в системе
"""

import sys
import os
import subprocess
from datetime import datetime

# Добавляем путь к src для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from amocrm_exporter.core.config import settings
from amocrm_exporter.core.logger import log_event


def run_command(command: str, description: str) -> bool:
    """Выполняет команду и возвращает результат"""
    print(f"\n🔧 {description}")
    print(f"Выполняется: {command}")

    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} - успешно")
        if result.stdout:
            print(f"Вывод: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - ошибка")
        print(f"Ошибка: {e.stderr.strip()}")
        return False


def fix_all_errors():
    """Исправляет все найденные ошибки"""

    print("🚀 Запуск исправления всех ошибок...")
    print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Исправление дублирующихся ключей в MongoDB
    print("\n📊 Шаг 1: Исправление дублирующихся ключей в MongoDB")
    duplicate_fix_script = os.path.join(os.path.dirname(__file__), 'fix_duplicate_keys.py')
    if os.path.exists(duplicate_fix_script):
        success = run_command(f"python {duplicate_fix_script}", "Исправление дублирующихся ключей")
        if not success:
            print("⚠️ Пропускаем исправление дублирующихся ключей из-за ошибки")
    else:
        print("⚠️ Скрипт исправления дублирующихся ключей не найден")

    # 2. Перезапуск сервисов для применения исправлений индексов
    print("\n🔄 Шаг 2: Перезапуск сервисов")

    # Останавливаем сервисы
    run_command("docker-compose down", "Остановка сервисов")

    # Запускаем сервисы заново
    run_command("docker-compose up -d", "Запуск сервисов")

    # 3. Проверка состояния сервисов
    print("\n🔍 Шаг 3: Проверка состояния сервисов")
    run_command("docker-compose ps", "Проверка статуса сервисов")

    # 4. Проверка логов на наличие ошибок
    print("\n📋 Шаг 4: Проверка логов")
    run_command("docker-compose logs --tail=50 app", "Проверка логов приложения")

    print("\n🎉 Исправление ошибок завершено!")
    print("\n📝 Рекомендации:")
    print("1. Проверьте логи на наличие новых ошибок")
    print("2. Если проблемы с Google Sheets продолжаются, проверьте настройки в .env файле")
    print("3. Убедитесь, что все необходимые переменные окружения установлены")

    log_event("error_fix", "info", "Completed all error fixes")


if __name__ == "__main__":
    fix_all_errors()