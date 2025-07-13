"""
Background Statistics Updater для AmoCRM Data Exporter

Фоновое обновление предвычисленных статистик для обеспечения актуальности данных.
"""

import time
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from ..storage.storage import Storage
from ..processors.precomputed_statistics import get_precomputed_stats_manager, PrecomputedStatisticsManager
from ..core.logger import log_event
from ..core import config


class BackgroundStatisticsUpdater:
    """Фоновый процесс для обновления предвычисленных статистик"""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.stats_manager = get_precomputed_stats_manager(storage)
        self.is_running = False
        self.worker_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

        # Configuration
        self.update_interval_minutes = getattr(config.settings, 'STATS_UPDATE_INTERVAL', 60)
        self.max_updates_per_run = getattr(config.settings, 'STATS_MAX_UPDATES_PER_RUN', 50)
        self.entity_types = ["leads", "contacts", "companies", "events", "users", "pipelines"]

        # Statistics
        self.last_update_time: Optional[datetime] = None
        self.updates_performed = 0
        self.errors_encountered = 0

    def start(self):
        """Запуск фонового обновления статистик"""
        if self.is_running:
            log_event("stats_updater", "warning", "Background statistics updater already running")
            return

        self.is_running = True
        self.stop_event.clear()

        # Start worker thread
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()

        log_event("stats_updater", "info",
                 f"Started background statistics updater with {self.update_interval_minutes}min interval")

    def stop(self):
        """Остановка фонового обновления статистик"""
        if not self.is_running:
            return

        self.is_running = False
        self.stop_event.set()

        # Wait for worker thread to finish
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=30)

        log_event("stats_updater", "info", "Stopped background statistics updater")

    def _worker_loop(self):
        """Основной цикл фонового процесса"""
        while self.is_running and not self.stop_event.is_set():
            try:
                # Выполняем обновление статистик
                self._update_statistics()

                # Очищаем устаревшие записи
                self._cleanup_expired_statistics()

                # Обновляем время последнего обновления
                self.last_update_time = datetime.now()

                # Спим до следующего цикла
                time.sleep(self.update_interval_minutes * 60)

            except Exception as e:
                log_event("stats_updater", "error", f"Error in worker loop: {e}")
                self.errors_encountered += 1
                time.sleep(60)  # Wait longer on error

    def _update_statistics(self):
        """Обновление статистик, которые нуждаются в обновлении"""
        try:
            log_event("stats_updater", "info", "Starting background statistics update")

            # Получаем статистики, которые нужно обновить
            result = self.stats_manager.background_update_statistics(self.entity_types)

            if "error" in result:
                log_event("stats_updater", "error", f"Background update failed: {result['error']}")
                return

            updated_count = result.get("updated_count", 0)
            errors = result.get("errors", [])

            self.updates_performed += updated_count
            self.errors_encountered += len(errors)

            if updated_count > 0:
                log_event("stats_updater", "info",
                         f"Updated {updated_count} statistics entries")

            if errors:
                log_event("stats_updater", "warning",
                         f"Encountered {len(errors)} errors during update")
                for error in errors[:5]:  # Log first 5 errors
                    log_event("stats_updater", "error", f"Update error: {error}")

        except Exception as e:
            log_event("stats_updater", "error", f"Error updating statistics: {e}")
            self.errors_encountered += 1

    def _cleanup_expired_statistics(self):
        """Очистка устаревших статистик"""
        try:
            cleaned_count = self.stats_manager.cleanup_expired_entries()

            if cleaned_count > 0:
                log_event("stats_updater", "info",
                         f"Cleaned up {cleaned_count} expired statistics entries")

        except Exception as e:
            log_event("stats_updater", "error", f"Error cleaning up expired statistics: {e}")

    def force_update_all_statistics(self, entity_type: Optional[str] = None) -> Dict[str, Any]:
        """Принудительное обновление всех статистик"""
        try:
            start_time = time.time()

            entity_types = [entity_type] if entity_type else self.entity_types
            total_updated = 0
            total_errors = []

            for et in entity_types:
                if self.stop_event.is_set():
                    break

                # Получаем все поля для данного типа сущности
                sample_entities = self.storage.get_entities(et, limit=100)
                if not sample_entities:
                    continue

                # Извлекаем все имена полей
                all_fields = set()
                for entity in sample_entities:
                    all_fields.update(entity.keys())

                # Обновляем статистику для каждого поля
                for field_name in all_fields:
                    if self.stop_event.is_set():
                        break

                    try:
                        stats = self.stats_manager.get_statistics(et, field_name, force_refresh=True)
                        if stats:
                            total_updated += 1
                    except Exception as e:
                        total_errors.append(f"{et}.{field_name}: {e}")

                log_event("stats_updater", "info",
                         f"Updated statistics for {et}: {len(all_fields)} fields")

            duration = time.time() - start_time

            return {
                "success": True,
                "updated_count": total_updated,
                "errors": total_errors,
                "duration": round(duration, 2),
                "entity_types": entity_types,
                "message": f"Successfully updated {total_updated} statistics in {duration:.2f} seconds"
            }

        except Exception as e:
            log_event("stats_updater", "error", f"Error in force update: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to update statistics: {e}"
            }

    def get_update_status(self) -> Dict[str, Any]:
        """Получение статуса обновления статистик"""
        try:
            # Получаем статус кэша
            cache_status = self.stats_manager.get_cache_status()

            return {
                "is_running": self.is_running,
                "update_interval_minutes": self.update_interval_minutes,
                "last_update_time": self.last_update_time.isoformat() if self.last_update_time else None,
                "updates_performed": self.updates_performed,
                "errors_encountered": self.errors_encountered,
                "cache_status": cache_status,
                "entity_types": self.entity_types
            }

        except Exception as e:
            log_event("stats_updater", "error", f"Error getting update status: {e}")
            return {
                "is_running": self.is_running,
                "error": str(e)
            }

    def invalidate_entity_statistics(self, entity_type: str, field_name: Optional[str] = None):
        """Инвалидация статистик для сущности"""
        try:
            self.stats_manager.invalidate_statistics(entity_type, field_name)

            if field_name:
                log_event("stats_updater", "info", f"Invalidated statistics for {entity_type}.{field_name}")
            else:
                log_event("stats_updater", "info", f"Invalidated all statistics for {entity_type}")

        except Exception as e:
            log_event("stats_updater", "error", f"Error invalidating statistics: {e}")

    def trigger_immediate_update(self, entity_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Запуск немедленного обновления статистик"""
        try:
            if not entity_types:
                entity_types = self.entity_types

            log_event("stats_updater", "info", f"Triggering immediate update for: {entity_types}")

            result = self.stats_manager.background_update_statistics(entity_types)

            if "error" not in result:
                self.updates_performed += result.get("updated_count", 0)
                self.errors_encountered += len(result.get("errors", []))

            return result

        except Exception as e:
            log_event("stats_updater", "error", f"Error in immediate update: {e}")
            return {
                "error": str(e),
                "message": f"Failed to trigger immediate update: {e}"
            }

    def export_update_metrics(self) -> Dict[str, Any]:
        """Экспорт метрик обновления статистик"""
        try:
            cache_status = self.stats_manager.get_cache_status()

            return {
                "updater_metrics": {
                    "is_running": self.is_running,
                    "update_interval_minutes": self.update_interval_minutes,
                    "last_update_time": self.last_update_time.isoformat() if self.last_update_time else None,
                    "total_updates_performed": self.updates_performed,
                    "total_errors_encountered": self.errors_encountered,
                    "uptime_hours": (datetime.now() - self.last_update_time).total_seconds() / 3600 if self.last_update_time else 0
                },
                "cache_metrics": cache_status,
                "configuration": {
                    "entity_types": self.entity_types,
                    "max_updates_per_run": self.max_updates_per_run,
                    "update_interval_minutes": self.update_interval_minutes
                }
            }

        except Exception as e:
            log_event("stats_updater", "error", f"Error exporting metrics: {e}")
            return {"error": str(e)}


# Global instance
_background_statistics_updater: Optional[BackgroundStatisticsUpdater] = None


def get_background_statistics_updater(storage: Optional[Storage] = None) -> BackgroundStatisticsUpdater:
    """Получить или создать глобальный экземпляр обновлятора статистик"""
    global _background_statistics_updater

    if _background_statistics_updater is None:
        if storage is None:
            storage = Storage()
        _background_statistics_updater = BackgroundStatisticsUpdater(storage)

    return _background_statistics_updater


def start_background_statistics_updater():
    """Запустить фоновое обновление статистик"""
    updater = get_background_statistics_updater()
    updater.start()


def stop_background_statistics_updater():
    """Остановить фоновое обновление статистик"""
    updater = get_background_statistics_updater()
    updater.stop()


def invalidate_statistics_for_entity(entity_type: str, field_name: Optional[str] = None):
    """Helper функция для инвалидации статистик"""
    updater = get_background_statistics_updater()
    updater.invalidate_entity_statistics(entity_type, field_name)