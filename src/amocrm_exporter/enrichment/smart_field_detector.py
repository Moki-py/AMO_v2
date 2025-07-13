"""
Smart Field Detection Module для AmoCRM Data Exporter

Автоматически определяет типы полей, их характеристики и предлагает
оптимизации для обработки данных.
"""

import re
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from collections import Counter
import json
from dataclasses import dataclass, field
from enum import Enum

from ..core.logger import log_event


class FieldType(Enum):
    """Типы полей для классификации"""
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    JSON = "json"
    ARRAY = "array"
    OBJECT = "object"
    ENUM = "enum"
    CURRENCY = "currency"
    MULTISELECT = "multiselect"
    UNKNOWN = "unknown"


@dataclass
class FieldStatistics:
    """Статистика поля"""
    field_name: str
    detected_type: FieldType
    confidence: float  # 0-1
    total_values: int
    non_null_values: int
    unique_values: int
    null_percentage: float
    fill_percentage: float

    # Характеристики для разных типов
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None

    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    avg_value: Optional[float] = None

    common_values: List[Tuple[Any, int]] = field(default_factory=list)
    patterns: List[str] = field(default_factory=list)

    # Рекомендации
    recommendations: List[str] = field(default_factory=list)
    suggested_indexes: List[str] = field(default_factory=list)
    processing_hints: List[str] = field(default_factory=list)


class SmartFieldDetector:
    """Класс для автоматического определения типов полей и их характеристик"""

    def __init__(self):
        self.patterns = {
            FieldType.EMAIL: [
                r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            ],
            FieldType.PHONE: [
                r'^\+?[\d\s\-\(\)]{7,}$',
                r'^8\d{10}$',
                r'^\+7\d{10}$'
            ],
            FieldType.URL: [
                r'^https?://[^\s/$.?#].[^\s]*$',
                r'^www\.[^\s/$.?#].[^\s]*$'
            ],
            FieldType.DATE: [
                r'^\d{4}-\d{2}-\d{2}$',
                r'^\d{2}\.\d{2}\.\d{4}$',
                r'^\d{2}/\d{2}/\d{4}$'
            ],
            FieldType.DATETIME: [
                r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
                r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
            ],
            FieldType.CURRENCY: [
                r'^\$?\d+(?:\.\d{2})?$',
                r'^\d+(?:\.\d{2})?\s?(?:руб|рубл|₽|USD|EUR)$'
            ]
        }

        # Ключевые слова для определения типов
        self.field_keywords = {
            FieldType.EMAIL: ['email', 'e-mail', 'mail', 'почта'],
            FieldType.PHONE: ['phone', 'tel', 'telephone', 'телефон', 'тел'],
            FieldType.URL: ['url', 'link', 'site', 'website', 'ссылка'],
            FieldType.DATE: ['date', 'дата', 'created', 'updated'],
            FieldType.CURRENCY: ['price', 'cost', 'amount', 'sum', 'цена', 'стоимость']
        }

    def analyze_field(self, field_name: str, values: List[Any],
                      sample_size: int = 1000) -> FieldStatistics:
        """
        Анализирует поле и определяет его тип и характеристики

        Args:
            field_name: Имя поля
            values: Список значений поля
            sample_size: Максимальное количество значений для анализа

        Returns:
            FieldStatistics с результатами анализа
        """

        # Берем выборку для анализа
        sample_values = values[:sample_size] if len(values) > sample_size else values

        # Базовая статистика
        total_values = len(sample_values)
        non_null_values = [v for v in sample_values if v is not None and v != ""]
        non_null_count = len(non_null_values)
        unique_values = len(set(str(v) for v in non_null_values))

        null_percentage = ((total_values - non_null_count) / total_values * 100) if total_values > 0 else 0
        fill_percentage = (non_null_count / total_values * 100) if total_values > 0 else 0

        # Определяем тип поля
        detected_type, confidence = self._detect_field_type(field_name, non_null_values)

        # Создаем базовую статистику
        stats = FieldStatistics(
            field_name=field_name,
            detected_type=detected_type,
            confidence=confidence,
            total_values=total_values,
            non_null_values=non_null_count,
            unique_values=unique_values,
            null_percentage=round(null_percentage, 2),
            fill_percentage=round(fill_percentage, 2)
        )

        # Дополнительный анализ в зависимости от типа
        self._analyze_by_type(stats, non_null_values)

        # Генерируем рекомендации
        self._generate_recommendations(stats)

        return stats

    def _detect_field_type(self, field_name: str, values: List[Any]) -> Tuple[FieldType, float]:
        """
        Определяет тип поля на основе имени и значений

        Returns:
            Tuple[FieldType, confidence] где confidence от 0 до 1
        """
        if not values:
            return FieldType.UNKNOWN, 0.0

        # Проверяем по ключевым словам в имени поля
        field_name_lower = field_name.lower()
        for field_type, keywords in self.field_keywords.items():
            if any(keyword in field_name_lower for keyword in keywords):
                return field_type, 0.8

                # Анализируем значения
        type_votes: Counter[FieldType] = Counter()

        for value in values[:100]:  # Анализируем первые 100 значений
            value_str = str(value).strip()

            if not value_str:
                continue

            # Проверяем паттерны
            for field_type, patterns in self.patterns.items():
                if any(re.match(pattern, value_str) for pattern in patterns):
                    type_votes[field_type] += 1
                    continue

            # Проверяем базовые типы
            if self._is_boolean(value):
                type_votes[FieldType.BOOLEAN] += 1
            elif self._is_integer(value):
                type_votes[FieldType.INTEGER] += 1
            elif self._is_float(value):
                type_votes[FieldType.FLOAT] += 1
            elif self._is_json(value_str):
                type_votes[FieldType.JSON] += 1
            elif self._is_array(value):
                type_votes[FieldType.ARRAY] += 1
            elif isinstance(value, dict):
                type_votes[FieldType.OBJECT] += 1
            else:
                type_votes[FieldType.STRING] += 1

        # Определяем победителя
        if not type_votes:
            return FieldType.UNKNOWN, 0.0

        most_common_type, count = type_votes.most_common(1)[0]
        confidence = count / len(values) if values else 0.0

        # Специальные случаи
        if confidence < 0.3:
            return FieldType.UNKNOWN, confidence

        # Проверяем на ENUM (если уникальных значений мало)
        unique_count = len(set(str(v) for v in values))
        if unique_count <= 10 and unique_count > 1 and most_common_type == FieldType.STRING:
            return FieldType.ENUM, confidence

        # Проверяем на MULTISELECT (если есть разделители)
        if most_common_type == FieldType.STRING:
            multiselect_indicators = [';', ',', '|', '\n']
            multiselect_count = sum(1 for v in values if any(sep in str(v) for sep in multiselect_indicators))
            if multiselect_count > len(values) * 0.3:
                return FieldType.MULTISELECT, confidence

        return most_common_type, confidence

    def _is_boolean(self, value: Any) -> bool:
        """Проверяет, является ли значение булевым"""
        if isinstance(value, bool):
            return True
        if isinstance(value, str):
            return value.lower() in ['true', 'false', '1', '0', 'yes', 'no', 'да', 'нет']
        return False

    def _is_integer(self, value: Any) -> bool:
        """Проверяет, является ли значение целым числом"""
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_float(self, value: Any) -> bool:
        """Проверяет, является ли значение числом с плавающей точкой"""
        try:
            float(value)
            return '.' in str(value)
        except (ValueError, TypeError):
            return False

    def _is_json(self, value_str: str) -> bool:
        """Проверяет, является ли значение JSON"""
        try:
            json.loads(value_str)
            return value_str.strip().startswith(('{', '['))
        except (json.JSONDecodeError, AttributeError):
            return False

    def _is_array(self, value: Any) -> bool:
        """Проверяет, является ли значение массивом"""
        return isinstance(value, (list, tuple))

    def _analyze_by_type(self, stats: FieldStatistics, values: List[Any]):
        """Дополнительный анализ в зависимости от типа поля"""

        if stats.detected_type in [FieldType.STRING, FieldType.EMAIL, FieldType.PHONE]:
            # Анализ длины строк
            lengths = [len(str(v)) for v in values]
            if lengths:
                stats.min_length = min(lengths)
                stats.max_length = max(lengths)
                stats.avg_length = round(sum(lengths) / len(lengths), 2)

        elif stats.detected_type in [FieldType.NUMBER, FieldType.INTEGER, FieldType.FLOAT]:
            # Анализ числовых значений
            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (ValueError, TypeError):
                    continue

            if numeric_values:
                stats.min_value = min(numeric_values)
                stats.max_value = max(numeric_values)
                stats.avg_value = round(sum(numeric_values) / len(numeric_values), 2)

        # Частые значения
        value_counts = Counter(str(v) for v in values)
        stats.common_values = value_counts.most_common(10)

        # Паттерны для строковых полей
        if stats.detected_type == FieldType.STRING:
            stats.patterns = self._extract_patterns(values)

    def _extract_patterns(self, values: List[Any]) -> List[str]:
        """Извлекает паттерны из строковых значений"""
        patterns = []

        # Проверяем общие паттерны
        sample_values = [str(v) for v in values[:50]]

        # Проверяем постоянную длину
        lengths = [len(v) for v in sample_values]
        if len(set(lengths)) == 1:
            patterns.append(f"Фиксированная длина: {lengths[0]}")

        # Проверяем цифры/буквы
        if all(v.isdigit() for v in sample_values):
            patterns.append("Только цифры")
        elif all(v.isalpha() for v in sample_values):
            patterns.append("Только буквы")
        elif all(v.isalnum() for v in sample_values):
            patterns.append("Буквы и цифры")

        # Проверяем префиксы/суффиксы
        if len(sample_values) > 1:
            common_prefix = self._find_common_prefix(sample_values)
            if len(common_prefix) > 2:
                patterns.append(f"Общий префикс: '{common_prefix}'")

        return patterns

    def _find_common_prefix(self, strings: List[str]) -> str:
        """Находит общий префикс в списке строк"""
        if not strings:
            return ""

        prefix = strings[0]
        for string in strings[1:]:
            while not string.startswith(prefix):
                prefix = prefix[:-1]
                if not prefix:
                    return ""
        return prefix

    def _generate_recommendations(self, stats: FieldStatistics):
        """Генерирует рекомендации для поля"""

        # Рекомендации по заполненности
        if stats.fill_percentage < 50:
            stats.recommendations.append("Поле заполнено менее чем на 50% - рассмотрите валидацию")

        # Рекомендации по индексам
        if stats.unique_values > stats.non_null_values * 0.8:
            stats.suggested_indexes.append("Уникальный индекс")
        elif stats.detected_type in [FieldType.DATE, FieldType.DATETIME]:
            stats.suggested_indexes.append("Индекс для сортировки по дате")
        elif stats.detected_type in [FieldType.INTEGER, FieldType.FLOAT] and stats.unique_values > 10:
            stats.suggested_indexes.append("Индекс для числовых запросов")
        elif stats.detected_type == FieldType.ENUM:
            stats.suggested_indexes.append("Индекс для перечисления")

        # Рекомендации по обработке
        if stats.detected_type == FieldType.EMAIL:
            stats.processing_hints.append("Рекомендуется валидация email адресов")
        elif stats.detected_type == FieldType.PHONE:
            stats.processing_hints.append("Рекомендуется нормализация номеров телефонов")
        elif stats.detected_type == FieldType.MULTISELECT:
            stats.processing_hints.append("Рекомендуется разделение на отдельные поля")
        elif stats.detected_type == FieldType.JSON:
            stats.processing_hints.append("Рекомендуется извлечение отдельных полей из JSON")

        # Рекомендации по производительности
        if stats.max_length and stats.max_length > 1000:
            stats.recommendations.append("Длинные строки - рассмотрите индексацию только префикса")

        if stats.unique_values < 10 and stats.non_null_values > 100:
            stats.recommendations.append("Мало уникальных значений - подходит для группировки")


def analyze_entity_fields(entities: List[Dict[str, Any]],
                         sample_size: int = 1000) -> Dict[str, FieldStatistics]:
    """
    Анализирует все поля в списке сущностей

    Args:
        entities: Список сущностей для анализа
        sample_size: Максимальное количество значений для анализа каждого поля

    Returns:
        Словарь с результатами анализа для каждого поля
    """
    detector = SmartFieldDetector()

        # Собираем все поля и их значения
    field_values: Dict[str, List[Any]] = {}

    for entity in entities:
        for field_name, value in entity.items():
            if field_name not in field_values:
                field_values[field_name] = []
            field_values[field_name].append(value)

    # Анализируем каждое поле
    results = {}
    for field_name, values in field_values.items():
        try:
            stats = detector.analyze_field(field_name, values, sample_size)
            results[field_name] = stats
            log_event("field_detector", "info",
                     f"Analyzed field '{field_name}': {stats.detected_type.value} "
                     f"(confidence: {stats.confidence:.2f})")
        except Exception as e:
            log_event("field_detector", "error",
                     f"Error analyzing field '{field_name}': {e}")

    return results


def generate_field_report(field_stats: Dict[str, FieldStatistics]) -> Dict[str, Any]:
    """
    Генерирует отчет по анализу полей

    Args:
        field_stats: Результаты анализа полей

    Returns:
        Структурированный отчет
    """
    report = {
        "summary": {
            "total_fields": len(field_stats),
            "high_confidence_fields": 0,
            "low_confidence_fields": 0,
            "fields_with_recommendations": 0
        },
        "type_distribution": {},
        "recommendations": {
            "indexing": [],
            "validation": [],
            "optimization": []
        },
        "fields": {}
    }

        # Анализируем статистику
    type_counts: Counter[str] = Counter()

    for field_name, stats in field_stats.items():
        # Подсчитываем типы
        type_counts[stats.detected_type.value] += 1

        # Подсчитываем уверенность
        if stats.confidence >= 0.8:
            report["summary"]["high_confidence_fields"] += 1  # type: ignore
        elif stats.confidence < 0.5:
            report["summary"]["low_confidence_fields"] += 1  # type: ignore

        # Подсчитываем поля с рекомендациями
        if stats.recommendations or stats.suggested_indexes or stats.processing_hints:
            report["summary"]["fields_with_recommendations"] += 1  # type: ignore

        # Собираем рекомендации
        for index in stats.suggested_indexes:
            report["recommendations"]["indexing"].append({  # type: ignore
                "field": field_name,
                "recommendation": index
            })

        for hint in stats.processing_hints:
            report["recommendations"]["validation"].append({  # type: ignore
                "field": field_name,
                "hint": hint
            })

        for rec in stats.recommendations:
            report["recommendations"]["optimization"].append({  # type: ignore
                "field": field_name,
                "recommendation": rec
            })

        # Добавляем в детальную информацию
        report["fields"][field_name] = {  # type: ignore
            "type": stats.detected_type.value,
            "confidence": stats.confidence,
            "fill_percentage": stats.fill_percentage,
            "unique_values": stats.unique_values,
            "recommendations_count": len(stats.recommendations) + len(stats.suggested_indexes) + len(stats.processing_hints)
        }

    # Распределение типов
    report["type_distribution"] = dict(type_counts)

    return report