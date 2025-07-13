"""
Adaptive Batch Processing Module для AmoCRM Data Exporter
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable, TypeVar, Generic, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

from ..core.logger import log_event

T = TypeVar('T')
R = TypeVar('R')


@dataclass
class BatchMetrics:
    """Метрики для отслеживания производительности batch операций"""
    batch_size: int
    processing_time: float
    success_count: int
    error_count: int
    timestamp: datetime
    throughput: float  # элементов в секунду

    @property
    def success_rate(self) -> float:
        """Процент успешных операций"""
        total = self.success_count + self.error_count
        return (self.success_count / total * 100) if total > 0 else 0


class AdaptiveBatchProcessor(Generic[T, R]):
    """Адаптивный batch processor с динамической оптимизацией размера batch'ей"""

    def __init__(self,
                 min_batch_size: int = 10,
                 max_batch_size: int = 1000,
                 initial_batch_size: int = 50,
                 max_workers: int = 5,
                 target_processing_time: float = 2.0,
                 adaptation_factor: float = 1.2):
        """
        Args:
            min_batch_size: Минимальный размер batch'а
            max_batch_size: Максимальный размер batch'а
            initial_batch_size: Начальный размер batch'а
            max_workers: Максимальное количество worker threads
            target_processing_time: Целевое время обработки batch'а в секундах
            adaptation_factor: Коэффициент адаптации размера batch'а
        """
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.current_batch_size = initial_batch_size
        self.max_workers = max_workers
        self.target_processing_time = target_processing_time
        self.adaptation_factor = adaptation_factor

        # Метрики и статистика
        self.metrics_history: List[BatchMetrics] = []
        self.lock = threading.Lock()
        self.total_processed = 0
        self.total_errors = 0
        self.start_time = datetime.now()

        # Настройки для стабилизации
        self.stability_threshold = 5  # Количество batch'ей для стабилизации
        self.recent_metrics_count = 10  # Количество последних метрик для анализа

    def _calculate_optimal_batch_size(self) -> int:
        """Вычисление оптимального размера batch'а на основе метрик"""
        if len(self.metrics_history) < 2:
            return self.current_batch_size

        # Берем последние метрики для анализа
        recent_metrics = self.metrics_history[-self.recent_metrics_count:]

        # Находим среднее время обработки
        avg_processing_time = sum(m.processing_time for m in recent_metrics) / len(recent_metrics)
        avg_throughput = sum(m.throughput for m in recent_metrics) / len(recent_metrics)
        avg_success_rate = sum(m.success_rate for m in recent_metrics) / len(recent_metrics)

        # Если время обработки слишком большое, уменьшаем batch size
        if avg_processing_time > self.target_processing_time * 1.5:
            new_size = int(self.current_batch_size / self.adaptation_factor)
            log_event("batch_processor", "info",
                     f"Уменьшаем batch size с {self.current_batch_size} до {new_size} (время: {avg_processing_time:.2f}s)")

        # Если время обработки приемлемое и высокий success rate, увеличиваем batch size
        elif avg_processing_time < self.target_processing_time * 0.7 and avg_success_rate > 95:
            new_size = int(self.current_batch_size * self.adaptation_factor)
            log_event("batch_processor", "info",
                     f"Увеличиваем batch size с {self.current_batch_size} до {new_size} (время: {avg_processing_time:.2f}s)")

        # Если много ошибок, уменьшаем batch size
        elif avg_success_rate < 80:
            new_size = int(self.current_batch_size * 0.8)
            log_event("batch_processor", "warning",
                     f"Уменьшаем batch size из-за ошибок с {self.current_batch_size} до {new_size} (success rate: {avg_success_rate:.1f}%)")

        else:
            new_size = self.current_batch_size

        # Ограничиваем размер batch'а
        return max(self.min_batch_size, min(new_size, self.max_batch_size))

    def _record_metrics(self, metrics: BatchMetrics):
        """Записать метрики batch операции"""
        with self.lock:
            self.metrics_history.append(metrics)

            # Ограничиваем размер истории метрик
            if len(self.metrics_history) > 100:
                self.metrics_history = self.metrics_history[-50:]

            self.total_processed += metrics.success_count
            self.total_errors += metrics.error_count

            # Адаптируем размер batch'а
            if len(self.metrics_history) >= self.stability_threshold:
                self.current_batch_size = self._calculate_optimal_batch_size()

    def _create_batches(self, items: List[T]) -> List[List[T]]:
        """Создание batch'ей из списка элементов"""
        batches = []
        for i in range(0, len(items), self.current_batch_size):
            batch = items[i:i + self.current_batch_size]
            batches.append(batch)
        return batches

    def process_batch(self, batch: List[T], processor_func: Callable[[List[T]], List[R]]) -> BatchMetrics:
        """Обработка одного batch'а"""
        start_time = time.time()
        success_count = 0
        error_count = 0

        try:
            results = processor_func(batch)
            success_count = len(results) if results else 0

        except Exception as e:
            error_count = len(batch)
            log_event("batch_processor", "error", f"Ошибка обработки batch'а: {e}")

        processing_time = time.time() - start_time
        throughput = len(batch) / processing_time if processing_time > 0 else 0

        metrics = BatchMetrics(
            batch_size=len(batch),
            processing_time=processing_time,
            success_count=success_count,
            error_count=error_count,
            timestamp=datetime.now(),
            throughput=throughput
        )

        self._record_metrics(metrics)
        return metrics

    def process_items(self, items: List[T], processor_func: Callable[[List[T]], List[R]],
                     max_workers: Optional[int] = None) -> Dict[str, Any]:
        """
        Обработка списка элементов с адаптивным batch sizing

        Args:
            items: Список элементов для обработки
            processor_func: Функция обработки batch'а
            max_workers: Максимальное количество worker threads

        Returns:
            Словарь с результатами обработки
        """
        if not items:
            return {"success": True, "processed": 0, "errors": 0, "batches": 0}

        workers = max_workers or self.max_workers
        total_start_time = time.time()

        log_event("batch_processor", "info",
                 f"Начинаем обработку {len(items)} элементов с batch size {self.current_batch_size}")

        # Создаем batch'и
        batches = self._create_batches(items)

        total_processed = 0
        total_errors = 0
        batch_metrics = []

        # Обрабатываем batch'и параллельно
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Отправляем задачи на выполнение
            future_to_batch = {
                executor.submit(self.process_batch, batch, processor_func): batch
                for batch in batches
            }

            # Собираем результаты
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    metrics = future.result()
                    batch_metrics.append(metrics)
                    total_processed += metrics.success_count
                    total_errors += metrics.error_count

                    # Логируем прогресс
                    if len(batch_metrics) % 10 == 0:
                        log_event("batch_processor", "info",
                                 f"Обработано {len(batch_metrics)}/{len(batches)} batch'ей")

                except Exception as e:
                    log_event("batch_processor", "error", f"Ошибка в future: {e}")
                    total_errors += len(batch)

        total_processing_time = time.time() - total_start_time

        # Финальная статистика
        avg_throughput = sum(m.throughput for m in batch_metrics) / len(batch_metrics) if batch_metrics else 0
        success_rate = (total_processed / len(items) * 100) if items else 0

        result = {
            "success": total_errors == 0,
            "processed": total_processed,
            "errors": total_errors,
            "batches": len(batches),
            "total_time": total_processing_time,
            "avg_throughput": avg_throughput,
            "success_rate": success_rate,
            "final_batch_size": self.current_batch_size
        }

        log_event("batch_processor", "info",
                 f"✅ Обработка завершена: {total_processed} успешно, {total_errors} ошибок, "
                 f"время: {total_processing_time:.2f}s, throughput: {avg_throughput:.1f} эл/с")

        return result

    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику работы batch processor'а"""
        with self.lock:
            if not self.metrics_history:
                return {"total_processed": 0, "total_errors": 0, "avg_throughput": 0}

            recent_metrics = self.metrics_history[-self.recent_metrics_count:]
            avg_throughput = sum(m.throughput for m in recent_metrics) / len(recent_metrics)
            avg_processing_time = sum(m.processing_time for m in recent_metrics) / len(recent_metrics)
            avg_success_rate = sum(m.success_rate for m in recent_metrics) / len(recent_metrics)

            runtime = datetime.now() - self.start_time

            return {
                "total_processed": self.total_processed,
                "total_errors": self.total_errors,
                "current_batch_size": self.current_batch_size,
                "avg_throughput": avg_throughput,
                "avg_processing_time": avg_processing_time,
                "avg_success_rate": avg_success_rate,
                "total_batches": len(self.metrics_history),
                "runtime_seconds": runtime.total_seconds(),
                "metrics_history_size": len(self.metrics_history)
            }

    def reset_metrics(self):
        """Сброс метрик"""
        with self.lock:
            self.metrics_history.clear()
            self.total_processed = 0
            self.total_errors = 0
            self.start_time = datetime.now()
            self.current_batch_size = 50  # Возвращаем к начальному размеру

        log_event("batch_processor", "info", "Метрики batch processor'а сброшены")


class StreamingProcessor:
    """Streaming processor для обработки больших объемов данных"""

    def __init__(self, chunk_size: int = 1000, buffer_size: int = 10000):
        """
        Args:
            chunk_size: Размер chunk'а для streaming
            buffer_size: Размер буфера для streaming
        """
        self.chunk_size = chunk_size
        self.buffer_size = buffer_size
        self.buffer: Queue[Any] = Queue(maxsize=buffer_size)
        self.is_streaming = False

    def stream_process(self, data_generator, processor_func: Callable[[List[T]], List[R]]) -> Dict[str, Any]:
        """
        Streaming обработка данных

        Args:
            data_generator: Генератор данных
            processor_func: Функция обработки chunk'а

        Returns:
            Результаты обработки
        """
        self.is_streaming = True
        total_processed = 0
        total_errors = 0

        try:
            chunk = []
            for item in data_generator:
                chunk.append(item)

                # Когда chunk достигает нужного размера, обрабатываем его
                if len(chunk) >= self.chunk_size:
                    try:
                        results = processor_func(chunk)
                        total_processed += len(results) if results else 0
                    except Exception as e:
                        total_errors += len(chunk)
                        log_event("streaming_processor", "error", f"Ошибка обработки chunk'а: {e}")

                    chunk = []

            # Обрабатываем остаток
            if chunk:
                try:
                    results = processor_func(chunk)
                    total_processed += len(results) if results else 0
                except Exception as e:
                    total_errors += len(chunk)
                    log_event("streaming_processor", "error", f"Ошибка обработки последнего chunk'а: {e}")

        finally:
            self.is_streaming = False

        return {
            "success": total_errors == 0,
            "processed": total_processed,
            "errors": total_errors
        }


# Глобальные экземпляры batch processor'ов для разных типов операций
mongodb_batch_processor = AdaptiveBatchProcessor[Dict[str, Any], bool](
    min_batch_size=10,
    max_batch_size=500,
    initial_batch_size=50,
    max_workers=3,
    target_processing_time=2.0
)

api_batch_processor = AdaptiveBatchProcessor[Dict[str, Any], Dict[str, Any]](
    min_batch_size=5,
    max_batch_size=100,
    initial_batch_size=25,
    max_workers=2,
    target_processing_time=3.0
)

export_batch_processor = AdaptiveBatchProcessor[Dict[str, Any], str](
    min_batch_size=50,
    max_batch_size=1000,
    initial_batch_size=100,
    max_workers=4,
    target_processing_time=5.0
)

flattening_batch_processor = AdaptiveBatchProcessor[Dict[str, Any], Dict[str, Any]](
    min_batch_size=20,
    max_batch_size=200,
    initial_batch_size=50,
    max_workers=4,
    target_processing_time=3.0
)


def get_batch_processor(operation_type: str) -> AdaptiveBatchProcessor[Any, Any]:
    """Получить batch processor для конкретного типа операции"""
    processors: Dict[str, AdaptiveBatchProcessor[Any, Any]] = {
        "mongodb": mongodb_batch_processor,  # type: ignore
        "api": api_batch_processor,  # type: ignore
        "export": export_batch_processor,  # type: ignore
        "flattening": flattening_batch_processor  # type: ignore
    }

    return processors.get(operation_type, mongodb_batch_processor)  # type: ignore