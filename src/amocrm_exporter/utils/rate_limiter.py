"""
Rate Limiter для контроля частоты API вызовов
"""

import time
import asyncio
import threading
from typing import Dict, Optional, Any, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import deque
from enum import Enum

from ..core import config
from ..core.logger import log_event


class RateLimitStrategy(Enum):
    """Стратегии rate limiting"""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


@dataclass
class RateLimitConfig:
    """Конфигурация rate limiter"""
    max_requests: int
    time_window: int  # в секундах
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    burst_allowance: int = 0  # дополнительные запросы для burst


class TokenBucketRateLimiter:
    """Rate limiter использующий алгоритм Token Bucket"""

    def __init__(self, max_requests: int, time_window: int, burst_allowance: int = 0):
        """
        Args:
            max_requests: Максимальное количество запросов
            time_window: Временное окно в секундах
            burst_allowance: Дополнительные токены для burst запросов
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.burst_allowance = burst_allowance
        self.bucket_size = max_requests + burst_allowance
        self.refill_rate = max_requests / time_window  # токенов в секунду

        self.tokens = float(self.bucket_size)  # Начинаем с полного bucket
        self.last_refill = time.time()
        self.lock = threading.Lock()

    def _refill_bucket(self):
        """Пополнение bucket новыми токенами"""
        now = time.time()
        elapsed = now - self.last_refill

        # Добавляем новые токены
        new_tokens = elapsed * self.refill_rate
        self.tokens = min(self.bucket_size, self.tokens + new_tokens)
        self.last_refill = now

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Получение токенов из bucket

        Args:
            tokens: Количество токенов для получения
            timeout: Максимальное время ожидания

        Returns:
            True если токены получены, False если превышен timeout
        """
        start_time = time.time()

        while True:
            with self.lock:
                self._refill_bucket()

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

                # Вычисляем время ожидания для следующего токена
                wait_time = (tokens - self.tokens) / self.refill_rate

            # Проверяем timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed + wait_time > timeout:
                    return False

            # Ждем до следующего пополнения
            time.sleep(min(wait_time, 0.1))

    async def acquire_async(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Асинхронная версия acquire"""
        start_time = time.time()

        while True:
            with self.lock:
                self._refill_bucket()

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

                wait_time = (tokens - self.tokens) / self.refill_rate

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed + wait_time > timeout:
                    return False

            await asyncio.sleep(min(wait_time, 0.1))

    def get_available_tokens(self) -> int:
        """Получение количества доступных токенов"""
        with self.lock:
            self._refill_bucket()
            return int(self.tokens)


class SlidingWindowRateLimiter:
    """Rate limiter использующий алгоритм Sliding Window"""

    def __init__(self, max_requests: int, time_window: int):
        """
        Args:
            max_requests: Максимальное количество запросов
            time_window: Временное окно в секундах
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = threading.Lock()

    def _cleanup_old_requests(self):
        """Удаление старых запросов из окна"""
        current_time = time.time()
        cutoff_time = current_time - self.time_window

        while self.requests and self.requests[0] <= cutoff_time:
            self.requests.popleft()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Получение разрешения на запрос

        Args:
            timeout: Максимальное время ожидания

        Returns:
            True если разрешение получено
        """
        start_time = time.time()

        while True:
            with self.lock:
                self._cleanup_old_requests()

                if len(self.requests) < self.max_requests:
                    self.requests.append(time.time())
                    return True

                # Вычисляем время ожидания
                oldest_request = self.requests[0]
                wait_time = oldest_request + self.time_window - time.time()

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed + wait_time > timeout:
                    return False

            time.sleep(min(wait_time, 0.1))

    def get_current_usage(self) -> int:
        """Получение текущего использования"""
        with self.lock:
            self._cleanup_old_requests()
            return len(self.requests)


class AdaptiveRateLimiter:
    """Адаптивный rate limiter который настраивается на основе ответов API"""

    def __init__(self, initial_requests: int, time_window: int, min_requests: int = 1, max_requests: int = 100):
        """
        Args:
            initial_requests: Начальное количество запросов
            time_window: Временное окно в секундах
            min_requests: Минимальное количество запросов
            max_requests: Максимальное количество запросов
        """
        self.current_requests = initial_requests
        self.time_window = time_window
        self.min_requests = min_requests
        self.max_requests = max_requests

        # Используем token bucket как базовый механизм
        self.token_bucket = TokenBucketRateLimiter(initial_requests, time_window)

        # Статистика для адаптации
        self.success_count = 0
        self.error_count = 0
        self.last_adjustment = time.time()
        self.adjustment_interval = 60  # секунд

        self.lock = threading.Lock()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Получение разрешения на запрос"""
        return self.token_bucket.acquire(1, timeout)

    def record_success(self):
        """Запись успешного запроса"""
        with self.lock:
            self.success_count += 1
            self._maybe_adjust_rate()

    def record_error(self, is_rate_limit_error: bool = False):
        """Запись ошибки запроса"""
        with self.lock:
            self.error_count += 1

            # Если это rate limit error, немедленно уменьшаем лимит
            if is_rate_limit_error:
                self._decrease_rate_limit()
            else:
                self._maybe_adjust_rate()

    def _maybe_adjust_rate(self):
        """Возможная корректировка лимита на основе статистики"""
        now = time.time()
        if now - self.last_adjustment < self.adjustment_interval:
            return

        total_requests = self.success_count + self.error_count
        if total_requests < 10:  # Недостаточно данных для корректировки
            return

        success_rate = self.success_count / total_requests

        # Если высокий success rate, можем увеличить лимит
        if success_rate > 0.95 and self.current_requests < self.max_requests:
            new_rate = min(self.max_requests, int(self.current_requests * 1.1))
            self._update_rate_limit(new_rate)
            log_event("rate_limiter", "info", f"Увеличиваем rate limit до {new_rate}")

        # Если низкий success rate, уменьшаем лимит
        elif success_rate < 0.8:
            new_rate = max(self.min_requests, int(self.current_requests * 0.9))
            self._update_rate_limit(new_rate)
            log_event("rate_limiter", "warning", f"Уменьшаем rate limit до {new_rate}")

        # Сбрасываем счетчики
        self.success_count = 0
        self.error_count = 0
        self.last_adjustment = now

    def _decrease_rate_limit(self):
        """Немедленное уменьшение лимита при rate limit error"""
        new_rate = max(self.min_requests, int(self.current_requests * 0.5))
        self._update_rate_limit(new_rate)
        log_event("rate_limiter", "warning", f"Rate limit error! Уменьшаем до {new_rate}")

    def _update_rate_limit(self, new_rate: int):
        """Обновление rate limit"""
        self.current_requests = new_rate
        self.token_bucket = TokenBucketRateLimiter(new_rate, self.time_window)

    def get_current_rate(self) -> int:
        """Получение текущего лимита"""
        return self.current_requests


class RateLimitManager:
    """Менеджер для управления множественными rate limiter'ами"""

    def __init__(self):
        """Инициализация менеджера"""
        self.limiters: Dict[str, Any] = {}
        self.lock = threading.Lock()

        # Создаем основные rate limiter'ы
        self._create_default_limiters()

    def _create_default_limiters(self):
        """Создание rate limiter'ов по умолчанию"""
        # API rate limiter на основе настроек
        max_rps = config.settings.max_requests_per_second
        self.limiters["api"] = AdaptiveRateLimiter(
            initial_requests=max_rps,
            time_window=1,
            min_requests=1,
            max_requests=max_rps * 2
        )

        # Rate limiter для экспорта
        self.limiters["export"] = TokenBucketRateLimiter(
            max_requests=10,
            time_window=1,
            burst_allowance=5
        )

        # Rate limiter для статистики
        self.limiters["stats"] = SlidingWindowRateLimiter(
            max_requests=20,
            time_window=60
        )

    def get_limiter(self, name: str) -> Optional[Any]:
        """Получение rate limiter по имени"""
        return self.limiters.get(name)

    def acquire(self, limiter_name: str, timeout: Optional[float] = None) -> bool:
        """Получение разрешения от указанного rate limiter"""
        limiter = self.get_limiter(limiter_name)
        if limiter is None:
            log_event("rate_limiter", "warning", f"Rate limiter '{limiter_name}' не найден")
            return True

        return limiter.acquire(timeout=timeout)

    def record_api_success(self):
        """Запись успешного API запроса"""
        api_limiter = self.get_limiter("api")
        if isinstance(api_limiter, AdaptiveRateLimiter):
            api_limiter.record_success()

    def record_api_error(self, is_rate_limit_error: bool = False):
        """Запись ошибки API запроса"""
        api_limiter = self.get_limiter("api")
        if isinstance(api_limiter, AdaptiveRateLimiter):
            api_limiter.record_error(is_rate_limit_error)

            def get_stats(self) -> Dict[str, Any]:
        """Получение статистики rate limiter'ов"""
        stats: Dict[str, Any] = {}

        for name, limiter in self.limiters.items():
            limiter_stats: Dict[str, Any] = {"type": type(limiter).__name__}

            if isinstance(limiter, TokenBucketRateLimiter):
                limiter_stats["available_tokens"] = limiter.get_available_tokens()
                limiter_stats["max_requests"] = limiter.max_requests
                limiter_stats["time_window"] = limiter.time_window
            elif isinstance(limiter, SlidingWindowRateLimiter):
                limiter_stats["current_usage"] = limiter.get_current_usage()
                limiter_stats["max_requests"] = limiter.max_requests
                limiter_stats["time_window"] = limiter.time_window
            elif isinstance(limiter, AdaptiveRateLimiter):
                limiter_stats["current_rate"] = limiter.get_current_rate()
                limiter_stats["available_tokens"] = limiter.token_bucket.get_available_tokens()
                limiter_stats["min_requests"] = limiter.min_requests
                limiter_stats["max_requests"] = limiter.max_requests

            stats[name] = limiter_stats

        return stats


def rate_limit(limiter_name: str, timeout: Optional[float] = None):
    """
    Декоратор для применения rate limiting к функциям

    Args:
        limiter_name: Имя rate limiter'а
        timeout: Максимальное время ожидания
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            if not rate_limit_manager.acquire(limiter_name, timeout):
                raise Exception(f"Rate limit exceeded for {limiter_name}")

            try:
                result = func(*args, **kwargs)
                # Записываем успех для адаптивных limiter'ов
                if limiter_name == "api":
                    rate_limit_manager.record_api_success()
                return result
            except Exception as e:
                # Записываем ошибку для адаптивных limiter'ов
                if limiter_name == "api":
                    is_rate_limit = "rate limit" in str(e).lower() or "too many requests" in str(e).lower()
                    rate_limit_manager.record_api_error(is_rate_limit)
                raise

        return wrapper
    return decorator


# Глобальный экземпляр rate limit manager
rate_limit_manager = RateLimitManager()


def get_rate_limit_manager() -> RateLimitManager:
    """Получение глобального rate limit manager"""
    return rate_limit_manager