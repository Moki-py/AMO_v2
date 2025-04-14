#!/bin/bash

# Скрипт для запуска AmoCRM экспортера в Linux/Mac

# Создаем директорию для данных, если она не существует
mkdir -p data

# Проверяем наличие необходимых пакетов
pip install -r requirements.txt > /dev/null 2>&1

# Запускаем современный веб-интерфейс
echo "Запуск AmoCRM Data Exporter..."
python main.py --server