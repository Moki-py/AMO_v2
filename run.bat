@echo off
REM Скрипт для запуска AmoCRM экспортера в Windows

REM Создаем директорию для данных, если она не существует
if not exist data mkdir data

REM Проверяем наличие необходимых пакетов
pip install -r requirements.txt > nul 2>&1

REM Запускаем веб-интерфейс
echo Запуск AmoCRM Data Exporter...
python main.py --server

pause