"""
Command Line Interface для AmoCRM Data Exporter
"""

import sys
import asyncio
from pathlib import Path

def main():
    """Основная точка входа для CLI"""
    import argparse

    parser = argparse.ArgumentParser(description='AmoCRM Data Exporter')
    parser.add_argument('command', nargs='?', choices=['export', 'web', 'worker'], default='export',
                       help='Команда для выполнения')
    parser.add_argument('--entity', choices=['deals', 'contacts', 'companies', 'events', 'users', 'pipelines', 'all'],
                       default='all', help='Тип сущности для экспорта')
    parser.add_argument('--force-restart', action='store_true', help='Принудительный перезапуск экспорта')
    parser.add_argument('--batch-size', type=int, default=10, help='Размер пакета для обработки')
    parser.add_argument('--max-workers', type=int, default=4, help='Максимальное количество потоков')
    parser.add_argument('--date-from', help='Экспорт данных с этой даты (YYYY-MM-DD)')
    parser.add_argument('--date-to', help='Экспорт данных до этой даты (YYYY-MM-DD)')

    args = parser.parse_args()

    if args.command == 'web':
        web_main()
    elif args.command == 'worker':
        worker_main()
    elif args.command == 'export':
        try:
            from .exporters.parallel_exporter import ParallelExporter

            exporter = ParallelExporter(max_workers=args.max_workers)

            if args.entity == 'all':
                exporter.export_all(
                    force_restart=args.force_restart,
                    batch_size=args.batch_size,
                    date_from=args.date_from,
                    date_to=args.date_to
                )
            elif args.entity == 'deals':
                exporter.export_deals(
                    force_restart=args.force_restart,
                    batch_size=args.batch_size,
                    date_from=args.date_from,
                    date_to=args.date_to
                )
            elif args.entity == 'contacts':
                exporter.export_contacts(
                    force_restart=args.force_restart,
                    batch_size=args.batch_size,
                    date_from=args.date_from,
                    date_to=args.date_to
                )
            elif args.entity == 'companies':
                exporter.export_companies(
                    force_restart=args.force_restart,
                    batch_size=args.batch_size,
                    date_from=args.date_from,
                    date_to=args.date_to
                )
            elif args.entity == 'events':
                exporter.export_events(
                    force_restart=args.force_restart,
                    batch_size=args.batch_size,
                    date_from=args.date_from,
                    date_to=args.date_to
                )
            elif args.entity == 'users':
                exporter.export_users(
                    force_restart=args.force_restart,
                    batch_size=args.batch_size,
                    date_from=args.date_from,
                    date_to=args.date_to
                )
            elif args.entity == 'pipelines':
                exporter.export_pipelines(
                    force_restart=args.force_restart,
                    batch_size=args.batch_size,
                    date_from=args.date_from,
                    date_to=args.date_to
                )

            print(f"Экспорт {args.entity} завершен успешно!")

        except ImportError as e:
            print(f"Ошибка импорта: {e}")
            print("Убедитесь, что все зависимости установлены: pip install -r requirements.txt")
            sys.exit(1)
        except Exception as e:
            print(f"Ошибка при экспорте: {e}")
            sys.exit(1)

def web_main():
    """Точка входа для веб-интерфейса"""
    try:
        import uvicorn
        from .web.modern_ui_server import app
        uvicorn.run(app, host="127.0.0.1", port=8000)
    except ImportError as e:
        print(f"Ошибка импорта: {e}")
        print("Убедитесь, что все зависимости установлены: pip install -r requirements.txt")
        sys.exit(1)

def worker_main():
    """Точка входа для worker'а"""
    try:
        from .workers.run_worker import main as worker_main_func
        worker_main_func()
    except ImportError as e:
        print(f"Ошибка импорта: {e}")
        print("Убедитесь, что все зависимости установлены: pip install -r requirements.txt")
        sys.exit(1)

if __name__ == "__main__":
    main()