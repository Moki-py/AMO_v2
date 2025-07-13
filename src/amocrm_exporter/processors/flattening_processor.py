"""
Background processor for flattening data
"""

import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..storage.storage import Storage
from ..enrichment.data_enrichment import DataEnricher
from ..storage.state_manager import StateManager
from ..core.logger import log_event
from .batch_processor import get_batch_processor
from ..enrichment.smart_field_detector import analyze_entity_fields, generate_field_report
from ..core import config


class FlatteningProcessor:
    """Background processor for managing flattened data synchronization"""

    def __init__(self, storage: Storage, state_manager: StateManager):
        self.storage = storage
        self.state_manager = state_manager
        self.enricher = DataEnricher(storage)
        self.is_running = False
        self.worker_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

        # Configuration
        self.batch_size = getattr(config.settings, 'FLATTENING_BATCH_SIZE', 100)
        self.sync_interval_minutes = getattr(config.settings, 'FLATTENING_SYNC_INTERVAL', 30)
        self.max_entities_per_run = getattr(config.settings, 'FLATTENING_MAX_ENTITIES', 1000)
        self.max_workers = getattr(config.settings, 'FLATTENING_MAX_WORKERS', 4)
        self.enable_parallel_processing = getattr(config.settings, 'FLATTENING_ENABLE_PARALLEL', True)

        # Supported entity types
        self.entity_types = ["leads", "contacts", "companies", "events", "users", "pipelines"]

        # Batch processor for parallel operations
        self.batch_processor = get_batch_processor("flattening")

    def start(self):
        """Start the background flattening processor"""
        if self.is_running:
            log_event("flattening", "warning", "Flattening processor already running")
            return

        self.is_running = True
        self.stop_event.clear()

        # Start worker thread
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()

        log_event("flattening", "info", f"Started flattening processor with {self.sync_interval_minutes}min interval")

    def stop(self):
        """Stop the background flattening processor"""
        if not self.is_running:
            return

        self.is_running = False
        self.stop_event.set()

        # Wait for worker thread to finish
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=30)

        log_event("flattening", "info", "Stopped flattening processor")

    def _worker_loop(self):
        """Main worker loop for background processing"""
        while self.is_running and not self.stop_event.is_set():
            try:
                # Periodic sync based on interval
                self._sync_all_entities()

                # Sleep for sync interval (converted to seconds)
                time.sleep(self.sync_interval_minutes * 60)

            except Exception as e:
                log_event("flattening", "error", f"Error in worker loop: {e}")
                time.sleep(60)  # Wait longer on error

    def _sync_all_entities(self):
        """Synchronize all entity types"""
        try:
            log_event("flattening", "info", "Starting scheduled flattening synchronization")

            total_processed = 0

            for entity_type in self.entity_types:
                if self.stop_event.is_set():
                    break

                processed = self._sync_entity_type(entity_type)
                total_processed += processed

                if total_processed >= self.max_entities_per_run:
                    log_event("flattening", "info", f"Reached max entities limit ({self.max_entities_per_run})")
                    break

            log_event("flattening", "info", f"Completed synchronization, processed {total_processed} entities")

        except Exception as e:
            log_event("flattening", "error", f"Error in scheduled sync: {e}")

    def _sync_entity_type(self, entity_type: str) -> int:
        """Synchronize flattened data for a specific entity type"""
        try:
            # Get entities that need flattening
            entities_to_flatten = self.storage.get_entities_needing_flattening(
                entity_type, self.batch_size
            )

            if not entities_to_flatten:
                return 0

            log_event("flattening", "info", f"Processing {len(entities_to_flatten)} {entity_type} entities for flattening")

            # Choose processing method based on configuration
            if self.enable_parallel_processing and len(entities_to_flatten) > self.batch_size:
                return self._sync_entity_type_parallel(entity_type, entities_to_flatten)
            else:
                return self._sync_entity_type_sequential(entity_type, entities_to_flatten)

        except Exception as e:
            log_event("flattening", "error", f"Error syncing {entity_type}: {e}")
            return 0

    def _sync_entity_type_sequential(self, entity_type: str, entities_to_flatten: List[Dict[str, Any]]) -> int:
        """Sequential processing of entities (original method)"""
        processed_count = 0
        for i in range(0, len(entities_to_flatten), self.batch_size):
            if self.stop_event.is_set():
                break

            batch = entities_to_flatten[i:i + self.batch_size]

            # Flatten the batch
            flattened_batch = []
            for entity in batch:
                try:
                    flattened_entity = self.enricher.flatten_custom_fields(entity)
                    flattened_batch.append(flattened_entity)
                except Exception as e:
                    log_event("flattening", "error", f"Error flattening entity {entity.get('id', 'unknown')}: {e}")

            # Save flattened data
            if flattened_batch:
                success = self.storage.save_flattened_entities(entity_type, flattened_batch)
                if success:
                    processed_count += len(flattened_batch)
                else:
                    log_event("flattening", "error", f"Failed to save flattened batch for {entity_type}")

            # Small delay between batches
            time.sleep(0.1)

        return processed_count

    def _sync_entity_type_parallel(self, entity_type: str, entities_to_flatten: List[Dict[str, Any]]) -> int:
        """Parallel processing of entities using ThreadPoolExecutor"""
        log_event("flattening", "info", f"Using parallel processing for {entity_type} with {self.max_workers} workers")

        # Create batches
        batches = []
        for i in range(0, len(entities_to_flatten), self.batch_size):
            batch = entities_to_flatten[i:i + self.batch_size]
            batches.append(batch)

        processed_count = 0

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all flattening tasks
            future_to_batch = {
                executor.submit(self._process_flattening_batch, entity_type, batch): batch
                for batch in batches
            }

            # Collect results
            for future in as_completed(future_to_batch):
                if self.stop_event.is_set():
                    break

                batch = future_to_batch[future]
                try:
                    batch_result = future.result()
                    processed_count += batch_result

                    # Log progress
                    log_event("flattening", "info",
                             f"Processed batch for {entity_type}: {batch_result} entities")

                except Exception as e:
                    log_event("flattening", "error", f"Error processing batch for {entity_type}: {e}")

        return processed_count

    def _process_flattening_batch(self, entity_type: str, batch: List[Dict[str, Any]]) -> int:
        """Process a single batch of entities for flattening"""
        flattened_batch = []

        for entity in batch:
            try:
                flattened_entity = self.enricher.flatten_custom_fields(entity)
                flattened_batch.append(flattened_entity)
            except Exception as e:
                log_event("flattening", "error", f"Error flattening entity {entity.get('id', 'unknown')}: {e}")

        # Save flattened data
        if flattened_batch:
            success = self.storage.save_flattened_entities(entity_type, flattened_batch)
            if success:
                return len(flattened_batch)
            else:
                log_event("flattening", "error", f"Failed to save flattened batch for {entity_type}")
                return 0

        return 0

    def force_sync_entity_type(self, entity_type: str) -> Dict[str, Any]:
        """Force immediate synchronization of a specific entity type"""
        try:
            start_time = time.time()

            # Get all entities of this type
            all_entities = self.storage.get_entities(entity_type)

            if not all_entities:
                return {
                    "success": True,
                    "entity_type": entity_type,
                    "processed": 0,
                    "duration": 0,
                    "message": "No entities to process"
                }

            log_event("flattening", "info", f"Force syncing {len(all_entities)} {entity_type} entities")

            # Choose processing method based on configuration
            if self.enable_parallel_processing and len(all_entities) > self.batch_size:
                processed_count = self._sync_entity_type_parallel(entity_type, all_entities)
            else:
                processed_count = self._sync_entity_type_sequential(entity_type, all_entities)

            duration = time.time() - start_time

            return {
                "success": True,
                "entity_type": entity_type,
                "processed": processed_count,
                "duration": round(duration, 2),
                "message": f"Successfully processed {processed_count} entities in {duration:.2f} seconds",
                "parallel_processing": self.enable_parallel_processing and len(all_entities) > self.batch_size
            }

        except Exception as e:
            log_event("flattening", "error", f"Error in force sync for {entity_type}: {e}")
            return {
                "success": False,
                "entity_type": entity_type,
                "error": str(e),
                "message": f"Failed to sync {entity_type}: {e}"
            }

    def get_flattening_status(self) -> Dict[str, Any]:
        """Get current status of flattening operations"""
        try:
            status: Dict[str, Any] = {
                "is_running": self.is_running,
                "sync_interval_minutes": self.sync_interval_minutes,
                "batch_size": self.batch_size,
                "entity_types": self.entity_types,
                "statistics": {}
            }

            # Get statistics for each entity type
            for entity_type in self.entity_types:
                try:
                    original_count = self.storage.get_entity_count(entity_type)
                    flattened_count = len(self.storage.get_flattened_entities(entity_type))
                    needs_flattening = len(self.storage.get_entities_needing_flattening(entity_type, 10))

                    status["statistics"][entity_type] = {
                        "original_entities": original_count,
                        "flattened_entities": flattened_count,
                        "needs_flattening": needs_flattening,
                        "sync_percentage": round((flattened_count / original_count * 100) if original_count > 0 else 0, 2)
                    }
                except Exception as e:
                    status["statistics"][entity_type] = {
                        "error": str(e)
                    }

            return status

        except Exception as e:
            log_event("flattening", "error", f"Error getting flattening status: {e}")
            return {
                "is_running": self.is_running,
                "error": str(e)
            }

    def cleanup_old_flattened_data(self, days_old: int = 30) -> Dict[str, Any]:
        """Clean up old flattened data that no longer has corresponding original entities"""
        try:
            cleaned_counts = {}
            total_cleaned = 0

            for entity_type in self.entity_types:
                # Get all original entity IDs
                original_entities = self.storage.get_entities(entity_type)
                original_ids = {entity["id"] for entity in original_entities if "id" in entity}

                # Get all flattened entity IDs
                flattened_entities = self.storage.get_flattened_entities(entity_type)
                flattened_ids = {
                    entity["_flattened_metadata"]["original_id"]
                    for entity in flattened_entities
                    if "_flattened_metadata" in entity
                }

                # Find orphaned flattened entities
                orphaned_ids = flattened_ids - original_ids

                if orphaned_ids:
                    success = self.storage.delete_flattened_entities(entity_type, list(orphaned_ids))
                    if success:
                        cleaned_counts[entity_type] = len(orphaned_ids)
                        total_cleaned += len(orphaned_ids)

            return {
                "success": True,
                "cleaned_counts": cleaned_counts,
                "total_cleaned": total_cleaned,
                "message": f"Cleaned up {total_cleaned} orphaned flattened entities"
            }

        except Exception as e:
            log_event("flattening", "error", f"Error cleaning up old flattened data: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to clean up old data: {e}"
            }

    def analyze_flattened_fields(self, entity_type: str, sample_size: int = 1000) -> Dict[str, Any]:
        """Analyze flattened fields using smart field detection"""
        try:
            # Get sample of flattened entities
            flattened_entities = self.storage.get_flattened_entities(entity_type, limit=sample_size)

            if not flattened_entities:
                return {
                    "success": False,
                    "message": f"No flattened entities found for {entity_type}"
                }

            # Extract flattened data
            flattened_data = []
            for entity in flattened_entities:
                if "_flattened_metadata" in entity:
                    # Remove metadata for analysis
                    clean_entity = {k: v for k, v in entity.items() if not k.startswith("_")}
                    flattened_data.append(clean_entity)

            if not flattened_data:
                return {
                    "success": False,
                    "message": f"No valid flattened data found for {entity_type}"
                }

            # Analyze fields
            field_stats = analyze_entity_fields(flattened_data, sample_size)

            # Generate report
            report = generate_field_report(field_stats)

            log_event("flattening", "info",
                     f"Analyzed {len(flattened_data)} flattened {entity_type} entities, "
                     f"found {report['summary']['total_fields']} fields")

            return {
                "success": True,
                "entity_type": entity_type,
                "analyzed_entities": len(flattened_data),
                "field_analysis": report,
                "message": f"Successfully analyzed {len(flattened_data)} flattened entities"
            }

        except Exception as e:
            log_event("flattening", "error", f"Error analyzing flattened fields for {entity_type}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to analyze flattened fields: {e}"
            }

    def optimize_flattened_storage(self, entity_type: str) -> Dict[str, Any]:
        """Optimize storage for flattened entities based on field analysis"""
        try:
            # First analyze the fields
            analysis_result = self.analyze_flattened_fields(entity_type)

            if not analysis_result["success"]:
                return analysis_result

            field_analysis = analysis_result["field_analysis"]
            optimizations = []

            # Create recommended indexes based on field analysis
            for recommendation in field_analysis["recommendations"]["indexing"]:
                field_name = recommendation["field"]
                index_type = recommendation["recommendation"]

                try:
                    # Create index on flattened collection
                    collection_name = f"{entity_type}_flattened"
                    collection = self.storage.db[collection_name]

                    if "уникальный" in index_type.lower():
                        # Create unique index
                        collection.create_index(
                            f"flattened_data.{field_name}",
                            unique=True,
                            sparse=True,
                            background=True
                        )
                        optimizations.append(f"Created unique index on {field_name}")

                    elif "дата" in index_type.lower():
                        # Create date index
                        collection.create_index(
                            f"flattened_data.{field_name}",
                            background=True
                        )
                        optimizations.append(f"Created date index on {field_name}")

                    elif "числовых" in index_type.lower():
                        # Create numeric index
                        collection.create_index(
                            f"flattened_data.{field_name}",
                            background=True
                        )
                        optimizations.append(f"Created numeric index on {field_name}")

                    elif "перечисление" in index_type.lower():
                        # Create enum index
                        collection.create_index(
                            f"flattened_data.{field_name}",
                            background=True
                        )
                        optimizations.append(f"Created enum index on {field_name}")

                except Exception as e:
                    log_event("flattening", "warning", f"Failed to create index for {field_name}: {e}")

            return {
                "success": True,
                "entity_type": entity_type,
                "optimizations": optimizations,
                "field_analysis_summary": field_analysis["summary"],
                "message": f"Applied {len(optimizations)} optimizations to flattened storage"
            }

        except Exception as e:
            log_event("flattening", "error", f"Error optimizing flattened storage for {entity_type}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to optimize storage: {e}"
            }


# Global instance
_flattening_processor: Optional[FlatteningProcessor] = None


def get_flattening_processor(storage: Optional[Storage] = None, state_manager: Optional[StateManager] = None) -> FlatteningProcessor:
    """Get or create the global flattening processor instance"""
    global _flattening_processor

    if _flattening_processor is None:
        if storage is None:
            storage = Storage()
        if state_manager is None:
            state_manager = StateManager()

        _flattening_processor = FlatteningProcessor(storage, state_manager)

    return _flattening_processor


def start_flattening_processor():
    """Start the background flattening processor"""
    processor = get_flattening_processor()
    processor.start()


def stop_flattening_processor():
    """Stop the background flattening processor"""
    processor = get_flattening_processor()
    processor.stop()