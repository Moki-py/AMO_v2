"""
Exception hierarchy for AmoCRM Data Exporter

This module provides:
- Structured exception classes
- Error categorization
- Contextual error information
- Retry and recovery strategies
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from enum import Enum
import traceback
from dataclasses import dataclass, field


class ErrorSeverity(str, Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(str, Enum):
    """Error categories for better classification"""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    VALIDATION = "validation"
    NETWORK = "network"
    DATABASE = "database"
    EXTERNAL_API = "external_api"
    CONFIGURATION = "configuration"
    PROCESSING = "processing"
    STORAGE = "storage"
    EXPORT = "export"
    SYSTEM = "system"
    BUSINESS_LOGIC = "business_logic"


@dataclass
class ErrorContext:
    """Context information for errors"""
    timestamp: datetime = field(default_factory=datetime.now)
    component: Optional[str] = None
    operation: Optional[str] = None
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    additional_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "component": self.component,
            "operation": self.operation,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "user_id": self.user_id,
            "request_id": self.request_id,
            "additional_data": self.additional_data
        }


class BaseAmoException(Exception):
    """Base exception class for AmoCRM Data Exporter"""

    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: Optional[ErrorContext] = None,
        original_error: Optional[Exception] = None,
        is_retryable: bool = False,
        retry_after: Optional[int] = None
    ):
        super().__init__(message)
        self.message = message
        self.category = category
        self.severity = severity
        self.context = context or ErrorContext()
        self.original_error = original_error
        self.is_retryable = is_retryable
        self.retry_after = retry_after
        self.error_code = self.__class__.__name__

        # Capture stack trace
        self.stack_trace = traceback.format_exc()

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for structured logging"""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "category": self.category.value,
            "severity": self.severity.value,
            "is_retryable": self.is_retryable,
            "retry_after": self.retry_after,
            "context": self.context.to_dict(),
            "original_error": str(self.original_error) if self.original_error else None,
            "stack_trace": self.stack_trace
        }

    def __str__(self) -> str:
        """String representation of the exception"""
        return f"[{self.category.value.upper()}] {self.message}"


# Authentication and Authorization Errors
class AuthenticationError(BaseAmoException):
    """Authentication failed"""

    def __init__(self, message: str = "Authentication failed", **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.AUTHENTICATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )


class AuthorizationError(BaseAmoException):
    """Authorization failed"""

    def __init__(self, message: str = "Authorization failed", **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.AUTHORIZATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )


class TokenExpiredError(AuthenticationError):
    """Access token expired"""

    def __init__(self, message: str = "Access token expired", **kwargs):
        super().__init__(
            message,
            is_retryable=True,
            retry_after=60,
            **kwargs
        )


# Validation Errors
class ValidationError(BaseAmoException):
    """Data validation failed"""

    def __init__(self, message: str, field_name: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
        self.field_name = field_name


class EntityTypeError(ValidationError):
    """Invalid entity type"""

    def __init__(self, entity_type: str, valid_types: List[str], **kwargs):
        message = f"Invalid entity type: {entity_type}. Valid types: {valid_types}"
        super().__init__(message, field_name="entity_type", **kwargs)
        self.entity_type = entity_type
        self.valid_types = valid_types


class ConfigurationError(BaseAmoException):
    """Configuration error"""

    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.config_key = config_key


# Network and API Errors
class NetworkError(BaseAmoException):
    """Network communication error"""

    def __init__(self, message: str, **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.MEDIUM,
            is_retryable=True,
            retry_after=30,
            **filtered_kwargs
        )


class ApiError(BaseAmoException):
    """AmoCRM API error"""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_data: Optional[Dict] = None,
        **kwargs
    ):
        super().__init__(
            message,
            category=ErrorCategory.EXTERNAL_API,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
        self.status_code = status_code
        self.response_data = response_data


class RateLimitError(ApiError):
    """API rate limit exceeded"""

    def __init__(self, message: str = "Rate limit exceeded", retry_after: int = 60, **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            **filtered_kwargs
        )
        # Set the specific attributes for this error type
        self.is_retryable = True
        self.retry_after = retry_after
        self.severity = ErrorSeverity.LOW


class ApiTimeoutError(ApiError):
    """API request timeout"""

    def __init__(self, message: str = "API request timeout", **kwargs):
        super().__init__(
            message,
            is_retryable=True,
            retry_after=30,
            **kwargs
        )


# Database Errors
class DatabaseError(BaseAmoException):
    """Database operation error"""

    def __init__(self, message: str, operation: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATABASE,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.operation = operation


class DatabaseConnectionError(DatabaseError):
    """Database connection error"""

    def __init__(self, message: str = "Database connection failed", **kwargs):
        super().__init__(
            message,
            is_retryable=True,
            retry_after=30,
            severity=ErrorSeverity.CRITICAL,
            **kwargs
        )


class DatabaseTransactionError(DatabaseError):
    """Database transaction error"""

    def __init__(self, message: str = "Database transaction failed", **kwargs):
        super().__init__(
            message,
            is_retryable=True,
            retry_after=10,
            **kwargs
        )


# Processing Errors
class ProcessingError(BaseAmoException):
    """Data processing error"""


class BatchProcessingError(ProcessingError):
    """Batch processing operation error"""

    def __init__(self, message: str, stage: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.PROCESSING,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
        self.stage = stage


class DataEnrichmentError(ProcessingError):
    """Data enrichment error"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            stage="data_enrichment",
            **kwargs
        )


class FlatteningError(ProcessingError):
    """Data flattening error"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            stage="flattening",
            **kwargs
        )


# Export Errors
class ExportError(BaseAmoException):
    """Export operation error"""

    def __init__(self, message: str, export_type: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXPORT,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
        self.export_type = export_type


class ExportFileError(ExportError):
    """Export file creation error"""

    def __init__(self, message: str, file_path: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            **kwargs
        )
        self.file_path = file_path


class ExportFormatError(ExportError):
    """Export format error"""

    def __init__(self, message: str, format_type: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            **kwargs
        )
        self.format_type = format_type


# Google Sheets specific errors
class GoogleSheetsError(ExportError):
    """Google Sheets operation error"""

    def __init__(self, message: str, spreadsheet_id: Optional[str] = None, **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            export_type="google_sheets",
            **filtered_kwargs
        )
        self.spreadsheet_id = spreadsheet_id


class GoogleSheetsAuthError(GoogleSheetsError):
    """Google Sheets authentication error"""

    def __init__(self, message: str = "Google Sheets authentication failed", **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            **filtered_kwargs
        )
        # Set the specific attributes for this error type
        self.category = ErrorCategory.AUTHENTICATION
        self.severity = ErrorSeverity.HIGH
        self.is_retryable = True
        self.retry_after = 60


class GoogleSheetsPermissionError(GoogleSheetsError):
    """Google Sheets permission error"""

    def __init__(self, message: str, spreadsheet_id: Optional[str] = None, **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            spreadsheet_id=spreadsheet_id,
            **filtered_kwargs
        )
        # Set the specific attributes for this error type
        self.category = ErrorCategory.AUTHORIZATION
        self.severity = ErrorSeverity.HIGH


class GoogleSheetsQuotaError(GoogleSheetsError):
    """Google Sheets API quota exceeded"""

    def __init__(self, message: str = "Google Sheets API quota exceeded", retry_after: int = 100, **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            **filtered_kwargs
        )
        # Set the specific attributes for this error type
        self.is_retryable = True
        self.retry_after = retry_after
        self.severity = ErrorSeverity.MEDIUM


class GoogleSheetsRateLimitError(GoogleSheetsError):
    """Google Sheets API rate limit exceeded"""

    def __init__(self, message: str = "Google Sheets API rate limit exceeded", retry_after: int = 100, **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            **filtered_kwargs
        )
        # Set the specific attributes for this error type
        self.is_retryable = True
        self.retry_after = retry_after
        self.severity = ErrorSeverity.LOW


class GoogleSheetsConfigError(GoogleSheetsError):
    """Google Sheets configuration error"""

    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            **filtered_kwargs
        )
        # Set the specific attributes for this error type
        self.category = ErrorCategory.CONFIGURATION
        self.severity = ErrorSeverity.HIGH
        self.config_key = config_key


class GoogleSheetsSpreadsheetNotFoundError(GoogleSheetsError):
    """Google Sheets spreadsheet not found"""

    def __init__(self, spreadsheet_id: str, **kwargs):
        message = f"Spreadsheet not found: {spreadsheet_id}"
        super().__init__(
            message,
            spreadsheet_id=spreadsheet_id,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )


class GoogleSheetsSheetNotFoundError(GoogleSheetsError):
    """Google Sheets sheet not found"""

    def __init__(self, sheet_name: str, spreadsheet_id: Optional[str] = None, **kwargs):
        message = f"Sheet '{sheet_name}' not found"
        if spreadsheet_id:
            message += f" in spreadsheet {spreadsheet_id}"
        super().__init__(
            message,
            spreadsheet_id=spreadsheet_id,
            **kwargs
        )
        self.sheet_name = sheet_name


class GoogleSheetsBatchError(GoogleSheetsError):
    """Google Sheets batch operation error"""

    def __init__(self, message: str, batch_size: Optional[int] = None, failed_rows: Optional[int] = None, **kwargs):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            **filtered_kwargs
        )
        # Set the specific attributes for this error type
        self.is_retryable = True
        self.retry_after = 30
        self.batch_size = batch_size
        self.failed_rows = failed_rows


class GoogleSheetsPartialExportError(GoogleSheetsError):
    """Google Sheets partial export error"""

    def __init__(
        self,
        message: str,
        exported_entities: Optional[Dict[str, int]] = None,
        failed_entities: Optional[List[str]] = None,
        **kwargs
    ):
        # Remove conflicting parameters from kwargs before passing to parent
        filtered_kwargs = {k: v for k, v in kwargs.items()
                          if k not in ['category', 'severity', 'is_retryable', 'retry_after']}
        super().__init__(
            message,
            **filtered_kwargs
        )
        # Set the specific attributes for this error type
        self.severity = ErrorSeverity.MEDIUM
        self.exported_entities = exported_entities or {}
        self.failed_entities = failed_entities or []


# Storage Errors
class StorageError(BaseAmoException):
    """Storage operation error"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.STORAGE,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )


class CacheError(StorageError):
    """Cache operation error"""

    def __init__(self, message: str, cache_key: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            is_retryable=True,
            retry_after=5,
            severity=ErrorSeverity.LOW,
            **kwargs
        )
        self.cache_key = cache_key


# Concurrent Export Errors
class ConcurrentExportError(BaseAmoException):
    """Concurrent export management error"""

    def __init__(self, message: str, export_id: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.PROCESSING,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
        self.export_id = export_id


class RetrySchedulingError(BaseAmoException):
    """Retry scheduling error"""

    def __init__(self, message: str, batch_id: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.PROCESSING,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
        self.batch_id = batch_id


# Business Logic Errors
class BusinessLogicError(BaseAmoException):
    """Business logic error"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.BUSINESS_LOGIC,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )


class StateError(BusinessLogicError):
    """State management error"""

    def __init__(self, message: str, state_type: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            **kwargs
        )
        self.state_type = state_type


class WorkerError(BusinessLogicError):
    """Worker process error"""

    def __init__(self, message: str, worker_id: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            **kwargs
        )
        self.worker_id = worker_id


# System Errors
class SystemError(BaseAmoException):
    """System-level error"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )


class ResourceError(SystemError):
    """Resource unavailable error"""

    def __init__(self, message: str, resource_type: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            is_retryable=True,
            retry_after=60,
            **kwargs
        )
        self.resource_type = resource_type


class HealthCheckError(SystemError):
    """Health check failed"""

    def __init__(self, message: str, component: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            **kwargs
        )
        self.component = component


# Helper functions
def create_error_context(
    component: Optional[str] = None,
    operation: Optional[str] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    **kwargs
) -> ErrorContext:
    """Create error context with provided information"""
    return ErrorContext(
        component=component,
        operation=operation,
        entity_type=entity_type,
        entity_id=entity_id,
        additional_data=kwargs
    )


def wrap_exception(
    original_error: Exception,
    message: Optional[str] = None,
    category: ErrorCategory = ErrorCategory.SYSTEM,
    severity: ErrorSeverity = ErrorSeverity.MEDIUM,
    context: Optional[ErrorContext] = None,
    **kwargs
) -> BaseAmoException:
    """Wrap a generic exception into BaseAmoException"""
    if isinstance(original_error, BaseAmoException):
        return original_error

    error_message = message or str(original_error)
    return BaseAmoException(
        error_message,
        category=category,
        severity=severity,
        context=context,
        original_error=original_error,
        **kwargs
    )


def is_retryable_error(error: Exception) -> bool:
    """Check if error is retryable"""
    if isinstance(error, BaseAmoException):
        return error.is_retryable
    return False


def get_retry_delay(error: Exception) -> int:
    """Get retry delay for error"""
    if isinstance(error, BaseAmoException) and error.retry_after:
        return error.retry_after
    return 30  # Default retry delay