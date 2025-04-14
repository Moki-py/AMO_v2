@echo off
REM Скрипт для запуска современного веб-интерфейса AmoCRM экспортера в Windows

REM Создаем директорию для данных, если она не существует
if not exist data mkdir data

REM Проверяем наличие необходимых пакетов
pip install python-dotenv requests psutil > nul 2>&1

REM Запускаем современный веб-интерфейс
echo Запуск современного веб-интерфейса AmoCRM экспортера...
python modern_ui_server.py

pause