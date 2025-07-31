"""
Graceful degradation for formatting failures in Google Sheets export
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum

from .exceptions import (
    ExportFormatError,
    ErrorContext,
    create_error_context
)
from ..core.logger import log_event


class DegradationLevel(str, Enum):
    """Levels of graceful degradation"""
    NONE = "none"              # No degradation, fail on error
    MINIMAL = "minimal"        # Basic fallbacks
    MODERATE = "moderate"      # More aggressive fallbacks
    AGGRESSIVE = "aggressive"  # Maximum compatibility


@dataclass
class FormattingResult:
    """Result of formatting operation with degradation info"""
    value: Any
    degraded: bool = False
    degradation_level: DegradationLevel = DegradationLevel.NONE
    original_value: Optional[Any] = None
    warning_message: Optional[str] = None
    error_message: Optional[str] = None


class GracefulFormattingHandler:
    """Handles formatting with graceful degradation"""

    def __init__(self, degradation_level: DegradationLevel = DegradationLevel.MODERATE):
        self.degradation_level = degradation_level
        self.logger = logging.getLogger(__name__)
        self.formatting_warnings: List[str] = []
        self.formatting_errors: List[str] = []

    def format_value_safely(
        self,
        value: Any,
        field_name: str = "unknown",
        field_type: str = "text"
    ) -> FormattingResult:
        """Format value with graceful degradation on failure"""
        try:
            # Try normal formatting first
            formatted_value = self._format_value_normal(value, field_type)
            return FormattingResult(value=formatted_value)

        except Exception as e:
            # Apply graceful degradation
            return self._apply_degradation(value, field_name, field_type, e)

    def format_row_safely(
        self,
        row_data: Dict[str, Any],
        headers: List[str],
        field_types: Optional[Dict[str, str]] = None
    ) -> Tuple[List[Any], List[str]]:
        """Format entire row with graceful degradation"""
        formatted_row = []
        warnings = []

        field_types = field_types or {}

        for header in headers:
            value = row_data.get(header, '')
            field_type = field_types.get(header, 'text')

            result = self.format_value_safely(value, header, field_type)
            formatted_row.append(result.value)

            if result.warning_message:
                warnings.append(f"{header}: {result.warning_message}")

        return formatted_row, warnings

    def format_batch_safely(
        self,
        batch_data: List[Dict[str, Any]],
        headers: List[str],
        field_types: Optional[Dict[str, str]] = None
    ) -> Tuple[List[List[Any]], List[str], List[str]]:
        """Format entire batch with graceful degradation"""
        formatted_batch = []
        all_warnings = []
        all_errors = []

        for i, row_data in enumerate(batch_data):
            try:
                formatted_row, warnings = self.format_row_safely(row_data, headers, field_types)
                formatted_batch.append(formatted_row)

                if warnings:
                    all_warnings.extend([f"Row {i+1} - {w}" for w in warnings])

            except Exception as e:
                # Even row-level formatting failed, create minimal row
                error_msg = f"Row {i+1} formatting failed: {str(e)}"
                all_errors.append(error_msg)

                if self.degradation_level != DegradationLevel.NONE:
                    minimal_row = self._create_minimal_row(row_data, headers)
                    formatted_batch.append(minimal_row)
                    all_warnings.append(f"Row {i+1} - Used minimal formatting due to errors")
                else:
                    raise ExportFormatError(
                        f"Row formatting failed and degradation is disabled: {error_msg}",
                        format_type="row"
                    )

        return formatted_batch, all_warnings, all_errors

    def _format_value_normal(self, value: Any, field_type: str) -> Any:
        """Normal formatting without degradation"""
        if value is None:
            return ""

        if field_type == 'numeric':
            return self._format_numeric(value)
        elif field_type in ('date', 'datetime'):
            return self._format_datetime(value, field_type)
        elif field_type == 'multiselect':
            return self._format_multiselect(value)
        elif field_type == 'text':
            return self._format_text(value)
        elif field_type in ('select', 'checkbox', 'url'):
            return self._format_simple(value)
        else:
            return self._format_generic(value)

    def _apply_degradation(
        self,
        value: Any,
        field_name: str,
        field_type: str,
        error: Exception
    ) -> FormattingResult:
        """Apply graceful degradation based on level"""
        original_value = value
        error_msg = str(error)

        if self.degradation_level == DegradationLevel.NONE:
            raise ExportFormatError(
                f"Formatting failed for field '{field_name}': {error_msg}",
                format_type=field_type
            )

        # Try progressively simpler formatting approaches
        degraded_value, warning = self._try_degraded_formatting(value, field_type, error)

        self.formatting_warnings.append(f"Field '{field_name}': {warning}")

        log_event(
            "graceful_degradation",
            "warning",
            f"Applied degradation to field '{field_name}' (type: {field_type}): {warning}"
        )

        return FormattingResult(
            value=degraded_value,
            degraded=True,
            degradation_level=self.degradation_level,
            original_value=original_value,
            warning_message=warning,
            error_message=error_msg
        )

    def _try_degraded_formatting(
        self,
        value: Any,
        field_type: str,
        error: Exception
    ) -> Tuple[Any, str]:
        """Try different levels of degraded formatting"""

        # Level 1: Try simpler version of same type
        if self.degradation_level in [DegradationLevel.MINIMAL, DegradationLevel.MODERATE, DegradationLevel.AGGRESSIVE]:
            try:
                if field_type == 'numeric':
                    # Try simple string conversion
                    return str(value), "Converted numeric to string"
                elif field_type in ('date', 'datetime'):
                    # Try simple timestamp or string
                    if isinstance(value, (int, float)):
                        return str(int(value)), "Converted timestamp to string"
                    return str(value), "Converted date to string"
                elif field_type == 'multiselect':
                    # Try simple join
                    if isinstance(value, list):
                        return ', '.join(str(v) for v in value), "Simplified multiselect formatting"
                    return str(value), "Converted multiselect to string"
            except Exception:
                pass

        # Level 2: Try JSON serialization
        if self.degradation_level in [DegradationLevel.MODERATE, DegradationLevel.AGGRESSIVE]:
            try:
                if isinstance(value, (dict, list)):
                    return json.dumps(value), "Converted complex data to JSON string"
            except Exception:
                pass

        # Level 3: Simple string conversion
        if self.degradation_level == DegradationLevel.AGGRESSIVE:
            try:
                return str(value), "Converted to simple string"
            except Exception:
                pass

        # Final fallback: error placeholder
        return f"[FORMAT_ERROR: {type(value).__name__}]", "Used error placeholder"

    def _create_minimal_row(self, row_data: Dict[str, Any], headers: List[str]) -> List[Any]:
        """Create minimal row with basic string conversion"""
        minimal_row = []

        for header in headers:
            value = row_data.get(header, '')
            try:
                if value is None:
                    minimal_row.append("")
                elif isinstance(value, str):
                    # Protect values that might cause issues
                    if value.startswith('+'):
                        minimal_row.append(f"'{value}")
                    else:
                        minimal_row.append(value)
                elif isinstance(value, (int, float, bool)):
                    minimal_row.append(value)
                else:
                    minimal_row.append(str(value))
            except Exception:
                minimal_row.append("[ERROR]")

        return minimal_row

    def _format_numeric(self, value: Any) -> Union[int, float, str]:
        """Format numeric value"""
        if isinstance(value, (int, float)):
            return value

        if isinstance(value, str):
            # Remove any quotes
            value = value.replace("'", "").replace('"', '')

            # Try int first, then float
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                raise ValueError(f"Cannot convert '{value}' to number")

        raise ValueError(f"Cannot format {type(value)} as numeric")

    def _format_datetime(self, value: Any, format_type: str) -> str:
        """Format datetime value"""
        if isinstance(value, (int, float)):
            # Unix timestamp
            from datetime import datetime
            try:
                dt = datetime.fromtimestamp(value)
                if format_type == 'date':
                    return f'=DATE({dt.year};{dt.month};{dt.day})'
                else:
                    return f'=DATE({dt.year};{dt.month};{dt.day}) + TIME({dt.hour};{dt.minute};{dt.second})'
            except (ValueError, OSError):
                raise ValueError(f"Invalid timestamp: {value}")

        if isinstance(value, str):
            # Try to parse date string
            from datetime import datetime
            try:
                if format_type == 'date':
                    dt = datetime.strptime(value, '%Y-%m-%d')
                    return f'=DATE({dt.year};{dt.month};{dt.day})'
                else:
                    try:
                        dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        dt = datetime.strptime(value, '%Y-%m-%d')
                    return f'=DATE({dt.year};{dt.month};{dt.day}) + TIME({dt.hour};{dt.minute};{dt.second})'
            except ValueError:
                raise ValueError(f"Cannot parse date string: {value}")

        raise ValueError(f"Cannot format {type(value)} as datetime")

    def _format_multiselect(self, value: Any) -> str:
        """Format multiselect value"""
        if isinstance(value, list):
            return ', '.join(str(v) for v in value if v is not None)
        elif isinstance(value, str):
            return value
        else:
            raise ValueError(f"Cannot format {type(value)} as multiselect")

    def _format_text(self, value: Any) -> str:
        """Format text value"""
        if isinstance(value, str):
            # Protect values that start with +
            if value.startswith('+'):
                return f"'{value}"
            return value
        else:
            return str(value)

    def _format_simple(self, value: Any) -> Any:
        """Format simple value types"""
        if value is None:
            return ""
        return value

    def _format_generic(self, value: Any) -> str:
        """Format generic value"""
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)

    def get_degradation_summary(self) -> Dict[str, Any]:
        """Get summary of degradation applied"""
        return {
            "degradation_level": self.degradation_level.value,
            "warnings_count": len(self.formatting_warnings),
            "errors_count": len(self.formatting_errors),
            "warnings": self.formatting_warnings,
            "errors": self.formatting_errors
        }

    def clear_warnings(self) -> None:
        """Clear accumulated warnings and errors"""
        self.formatting_warnings.clear()
        self.formatting_errors.clear()


# Global formatting handler instance
formatting_handler = GracefulFormattingHandler()


def format_with_degradation(
    value: Any,
    field_name: str = "unknown",
    field_type: str = "text",
    degradation_level: DegradationLevel = DegradationLevel.MODERATE
) -> FormattingResult:
    """Convenience function for formatting with degradation"""
    handler = GracefulFormattingHandler(degradation_level)
    return handler.format_value_safely(value, field_name, field_type)


def format_batch_with_degradation(
    batch_data: List[Dict[str, Any]],
    headers: List[str],
    field_types: Optional[Dict[str, str]] = None,
    degradation_level: DegradationLevel = DegradationLevel.MODERATE
) -> Tuple[List[List[Any]], List[str], List[str]]:
    """Convenience function for batch formatting with degradation"""
    handler = GracefulFormattingHandler(degradation_level)
    return handler.format_batch_safely(batch_data, headers, field_types)