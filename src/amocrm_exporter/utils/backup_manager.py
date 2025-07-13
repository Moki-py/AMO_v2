"""
Backup Manager для AmoCRM Data Exporter

Управление резервными копиями MongoDB и конфигурационных файлов.
"""

import os
import shutil
import subprocess
import gzip
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path

from ..storage.storage import Storage
from ..core.logger import log_event
from ..core import config


class BackupManager:
    """Менеджер резервных копий"""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.backup_dir = Path(getattr(config.settings, 'BACKUP_DIR', './backups'))
        self.retention_days = getattr(config.settings, 'BACKUP_RETENTION_DAYS', 30)
        self.mongodb_host = getattr(config.settings, 'MONGODB_HOST', 'localhost')
        self.mongodb_port = getattr(config.settings, 'MONGODB_PORT', 27017)
        self.mongodb_db = getattr(config.settings, 'MONGODB_DB', 'amocrm_exporter')

        # Создаем директории для бэкапов
        self._ensure_backup_directories()

    def _ensure_backup_directories(self):
        """Создает необходимые директории для бэкапов"""
        subdirs = ['mongodb', 'configs', 'logs', 'statistics']

        for subdir in subdirs:
            backup_path = self.backup_dir / subdir
            backup_path.mkdir(parents=True, exist_ok=True)

        log_event("backup_manager", "info", f"Backup directories created at {self.backup_dir}")

    def create_mongodb_backup(self, compress: bool = True) -> Dict[str, Any]:
        """Создает резервную копию MongoDB"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"mongodb_backup_{timestamp}"
            backup_path = self.backup_dir / "mongodb" / backup_name

            # Создаем директорию для бэкапа
            backup_path.mkdir(parents=True, exist_ok=True)

            # Команда mongodump
            cmd = [
                "mongodump",
                "--host", f"{self.mongodb_host}:{self.mongodb_port}",
                "--db", self.mongodb_db,
                "--out", str(backup_path)
            ]

            log_event("backup_manager", "info", f"Starting MongoDB backup: {backup_name}")

            # Выполняем mongodump
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                error_msg = f"mongodump failed: {result.stderr}"
                log_event("backup_manager", "error", error_msg)
                return {
                    "success": False,
                    "error": error_msg,
                    "backup_name": backup_name
                }

            # Сжимаем бэкап если нужно
            final_backup_path = backup_path
            if compress:
                final_backup_path = self._compress_backup(backup_path)
                # Удаляем несжатую версию
                shutil.rmtree(backup_path)

            # Получаем размер бэкапа
            backup_size = self._get_directory_size(final_backup_path) if not compress else os.path.getsize(final_backup_path)

            # Сохраняем метаданные
            metadata = {
                "backup_name": backup_name,
                "created_at": timestamp,
                "database": self.mongodb_db,
                "backup_type": "mongodb",
                "compressed": compress,
                "size_bytes": backup_size,
                "path": str(final_backup_path)
            }

            self._save_backup_metadata(backup_name, metadata)

            log_event("backup_manager", "info",
                     f"MongoDB backup completed: {backup_name}, size: {backup_size / 1024 / 1024:.2f} MB")

            return {
                "success": True,
                "backup_name": backup_name,
                "backup_path": str(final_backup_path),
                "size_mb": round(backup_size / 1024 / 1024, 2),
                "compressed": compress
            }

        except Exception as e:
            log_event("backup_manager", "error", f"Error creating MongoDB backup: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def create_config_backup(self) -> Dict[str, Any]:
        """Создает резервную копию конфигурационных файлов"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"config_backup_{timestamp}"
            backup_path = self.backup_dir / "configs" / f"{backup_name}.tar.gz"

            # Список файлов для бэкапа
            config_files = [
                "config.py",
                "docker-compose.yml",
                "production.docker-compose.yml",
                "mongod.conf",
                "rabbitmq.conf",
                ".env"
            ]

            # Создаем архив с конфигурационными файлами
            import tarfile

            with tarfile.open(backup_path, 'w:gz') as tar:
                for config_file in config_files:
                    if os.path.exists(config_file):
                        tar.add(config_file, arcname=config_file)

                # Добавляем директории с конфигурациями
                config_dirs = ["nginx", "monitoring", "scripts"]
                for config_dir in config_dirs:
                    if os.path.exists(config_dir):
                        tar.add(config_dir, arcname=config_dir)

            backup_size = os.path.getsize(backup_path)

            # Сохраняем метаданные
            metadata = {
                "backup_name": backup_name,
                "created_at": timestamp,
                "backup_type": "config",
                "files_included": config_files,
                "size_bytes": backup_size,
                "path": str(backup_path)
            }

            self._save_backup_metadata(backup_name, metadata)

            log_event("backup_manager", "info",
                     f"Config backup completed: {backup_name}, size: {backup_size / 1024:.2f} KB")

            return {
                "success": True,
                "backup_name": backup_name,
                "backup_path": str(backup_path),
                "size_kb": round(backup_size / 1024, 2)
            }

        except Exception as e:
            log_event("backup_manager", "error", f"Error creating config backup: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def create_statistics_backup(self) -> Dict[str, Any]:
        """Создает резервную копию предвычисленных статистик"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"statistics_backup_{timestamp}"
            backup_path = self.backup_dir / "statistics" / f"{backup_name}.json.gz"

            # Экспортируем предвычисленные статистики
            from precomputed_statistics import get_precomputed_stats_manager
            stats_manager = get_precomputed_stats_manager(self.storage)

            all_statistics = {}
            for entity_type in ["leads", "contacts", "companies", "events", "users", "pipelines"]:
                entity_stats = stats_manager.export_statistics(entity_type)
                if "error" not in entity_stats:
                    all_statistics[entity_type] = entity_stats

            # Сохраняем в сжатом JSON файле
            with gzip.open(backup_path, 'wt', encoding='utf-8') as f:
                json.dump(all_statistics, f, indent=2, default=str)

            backup_size = os.path.getsize(backup_path)

            # Сохраняем метаданные
            metadata = {
                "backup_name": backup_name,
                "created_at": timestamp,
                "backup_type": "statistics",
                "entity_types": list(all_statistics.keys()),
                "size_bytes": backup_size,
                "path": str(backup_path)
            }

            self._save_backup_metadata(backup_name, metadata)

            log_event("backup_manager", "info",
                     f"Statistics backup completed: {backup_name}, size: {backup_size / 1024:.2f} KB")

            return {
                "success": True,
                "backup_name": backup_name,
                "backup_path": str(backup_path),
                "size_kb": round(backup_size / 1024, 2),
                "entity_types": list(all_statistics.keys())
            }

        except Exception as e:
            log_event("backup_manager", "error", f"Error creating statistics backup: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def create_full_backup(self) -> Dict[str, Any]:
        """Создает полную резервную копию системы"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            results = {
                "timestamp": timestamp,
                "backups": {}
            }

            # MongoDB backup
            mongodb_result = self.create_mongodb_backup()
            results["backups"]["mongodb"] = mongodb_result

            # Config backup
            config_result = self.create_config_backup()
            results["backups"]["config"] = config_result

            # Statistics backup
            stats_result = self.create_statistics_backup()
            results["backups"]["statistics"] = stats_result

            # Проверяем успешность всех бэкапов
            all_successful = all(
                backup.get("success", False)
                for backup in results["backups"].values()
            )

            if all_successful:
                log_event("backup_manager", "info", f"Full backup completed successfully: {timestamp}")
            else:
                log_event("backup_manager", "warning", f"Full backup completed with some errors: {timestamp}")

            results["success"] = all_successful
            return results

        except Exception as e:
            log_event("backup_manager", "error", f"Error creating full backup: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def restore_mongodb_backup(self, backup_name: str) -> Dict[str, Any]:
        """Восстанавливает MongoDB из резервной копии"""
        try:
            # Находим бэкап
            metadata = self._load_backup_metadata(backup_name)
            if not metadata:
                return {
                    "success": False,
                    "error": f"Backup metadata not found: {backup_name}"
                }

            backup_path = Path(metadata["path"])
            if not backup_path.exists():
                return {
                    "success": False,
                    "error": f"Backup file not found: {backup_path}"
                }

            # Распаковываем если нужно
            restore_path = backup_path
            if metadata.get("compressed", False):
                restore_path = self._decompress_backup(backup_path)

            # Команда mongorestore
            cmd = [
                "mongorestore",
                "--host", f"{self.mongodb_host}:{self.mongodb_port}",
                "--db", self.mongodb_db,
                "--drop",  # Удаляет существующие коллекции
                str(restore_path / self.mongodb_db)
            ]

            log_event("backup_manager", "info", f"Starting MongoDB restore from: {backup_name}")

            # Выполняем mongorestore
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                error_msg = f"mongorestore failed: {result.stderr}"
                log_event("backup_manager", "error", error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            # Удаляем временную распакованную директорию
            if metadata.get("compressed", False) and restore_path != backup_path:
                shutil.rmtree(restore_path)

            log_event("backup_manager", "info", f"MongoDB restore completed from: {backup_name}")

            return {
                "success": True,
                "backup_name": backup_name,
                "restored_to": self.mongodb_db
            }

        except Exception as e:
            log_event("backup_manager", "error", f"Error restoring MongoDB backup: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def list_backups(self, backup_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получает список доступных резервных копий"""
        try:
            backups = []
            metadata_dir = self.backup_dir / "metadata"

            if metadata_dir.exists():
                for metadata_file in metadata_dir.glob("*.json"):
                    try:
                        metadata = self._load_backup_metadata(metadata_file.stem)
                        if metadata and (not backup_type or metadata.get("backup_type") == backup_type):
                            # Проверяем существование файла бэкапа
                            backup_path = Path(metadata["path"])
                            metadata["exists"] = backup_path.exists()
                            metadata["age_days"] = (datetime.now() - datetime.strptime(metadata["created_at"], "%Y%m%d_%H%M%S")).days
                            backups.append(metadata)
                    except Exception as e:
                        log_event("backup_manager", "warning", f"Error reading backup metadata {metadata_file}: {e}")

            # Сортируем по дате создания (новые первыми)
            backups.sort(key=lambda x: x["created_at"], reverse=True)

            return backups

        except Exception as e:
            log_event("backup_manager", "error", f"Error listing backups: {e}")
            return []

    def cleanup_old_backups(self) -> Dict[str, Any]:
        """Удаляет старые резервные копии"""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.retention_days)

            deleted_backups = []
            total_freed_space = 0

            backups = self.list_backups()

            for backup in backups:
                backup_date = datetime.strptime(backup["created_at"], "%Y%m%d_%H%M%S")

                if backup_date < cutoff_date:
                    backup_path = Path(backup["path"])

                    if backup_path.exists():
                        # Получаем размер перед удалением
                        size = backup["size_bytes"]

                        # Удаляем бэкап
                        if backup_path.is_file():
                            backup_path.unlink()
                        else:
                            shutil.rmtree(backup_path)

                        # Удаляем метаданные
                        metadata_path = self.backup_dir / "metadata" / f"{backup['backup_name']}.json"
                        if metadata_path.exists():
                            metadata_path.unlink()

                        deleted_backups.append(backup["backup_name"])
                        total_freed_space += size

                        log_event("backup_manager", "info", f"Deleted old backup: {backup['backup_name']}")

            return {
                "success": True,
                "deleted_backups": deleted_backups,
                "count": len(deleted_backups),
                "freed_space_mb": round(total_freed_space / 1024 / 1024, 2),
                "retention_days": self.retention_days
            }

        except Exception as e:
            log_event("backup_manager", "error", f"Error cleaning up old backups: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _compress_backup(self, backup_path: Path) -> Path:
        """Сжимает директорию бэкапа в tar.gz архив"""
        import tarfile

        compressed_path = backup_path.with_suffix('.tar.gz')

        with tarfile.open(compressed_path, 'w:gz') as tar:
            tar.add(backup_path, arcname=backup_path.name)

        return compressed_path

    def _decompress_backup(self, compressed_path: Path) -> Path:
        """Распаковывает сжатый бэкап"""
        import tarfile

        extract_path = compressed_path.parent / compressed_path.stem.replace('.tar', '')

        with tarfile.open(compressed_path, 'r:gz') as tar:
            tar.extractall(path=extract_path.parent)

        return extract_path

    def _get_directory_size(self, path: Path) -> int:
        """Получает размер директории в байтах"""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
        return total_size

    def _save_backup_metadata(self, backup_name: str, metadata: Dict[str, Any]):
        """Сохраняет метаданные бэкапа"""
        metadata_dir = self.backup_dir / "metadata"
        metadata_dir.mkdir(exist_ok=True)

        metadata_path = metadata_dir / f"{backup_name}.json"

        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, default=str)

    def _load_backup_metadata(self, backup_name: str) -> Optional[Dict[str, Any]]:
        """Загружает метаданные бэкапа"""
        metadata_path = self.backup_dir / "metadata" / f"{backup_name}.json"

        if not metadata_path.exists():
            return None

        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            log_event("backup_manager", "error", f"Error loading backup metadata: {e}")
            return None


# Helper functions
def create_mongodb_backup(compress: bool = True) -> Dict[str, Any]:
    """Helper функция для создания MongoDB бэкапа"""
    storage = Storage()
    backup_manager = BackupManager(storage)
    return backup_manager.create_mongodb_backup(compress)


def create_full_backup() -> Dict[str, Any]:
    """Helper функция для создания полного бэкапа"""
    storage = Storage()
    backup_manager = BackupManager(storage)
    return backup_manager.create_full_backup()


def cleanup_old_backups() -> Dict[str, Any]:
    """Helper функция для очистки старых бэкапов"""
    storage = Storage()
    backup_manager = BackupManager(storage)
    return backup_manager.cleanup_old_backups()