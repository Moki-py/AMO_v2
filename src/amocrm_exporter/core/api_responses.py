"""
Structured API responses for AmoCRM Data Exporter

This module provides:
- Standardized response formats
- Error response handling
- Success response formatting
- Response validation
- HTTP status code management
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
from fastapi import HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from exceptions import (
    BaseAmoException,
    ErrorSeverity,
    ErrorCategory,
    ValidationError,
    AuthenticationError,
    AuthorizationError,
    ApiError,
    DatabaseError,
    NetworkError,
    ProcessingError,
    ExportError,
    StorageError,
    BusinessLogicError,
    SystemError
)
from .logger import log_event


class ResponseStatus(str, Enum):
    """Response status types"""
    SUCCESS = "success"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ErrorDetail:
    """Error detail information"""
    code: str
    message: str
    field: Optional[str] = None
    category: Optional[str] = None
    severity: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "code": self.code,
            "message": self.message,
            "field": self.field,
            "category": self.category,
            "severity": self.severity,
            "details": self.details
        }


class ApiResponse(BaseModel):
    """Standard API response model"""

    status: ResponseStatus = Field(..., description="Response status")
    message: str = Field(..., description="Response message")
    data: Optional[Dict[str, Any]] = Field(None, description="Response data")
    errors: Optional[List[ErrorDetail]] = Field(None, description="Error details")
    meta: Optional[Dict[str, Any]] = Field(None, description="Response metadata")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(), description="Response timestamp")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = {
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat()
        }

        if self.data is not None:
            result["data"] = self.data

        if self.errors is not None:
            result["errors"] = [error.to_dict() if isinstance(error, ErrorDetail) else error for error in self.errors]

        if self.meta is not None:
            result["meta"] = self.meta

        return result


class PaginatedResponse(BaseModel):
    """Paginated API response model"""

    status: ResponseStatus = Field(default=ResponseStatus.SUCCESS)
    message: str = Field(default="Success")
    data: List[Dict[str, Any]] = Field(default_factory=list)
    pagination: Dict[str, Any] = Field(..., description="Pagination info")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=lambda: datetime.now())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "status": self.status.value,
            "message": self.message,
            "data": self.data,
            "pagination": self.pagination,
            "total": self.total,
            "timestamp": self.timestamp.isoformat()
        }


class ApiResponseBuilder:
    """Builder for API responses"""

    @staticmethod
    def success(
        message: str = "Success",
        data: Optional[Dict[str, Any]] = None,
        meta: Optional[Dict[str, Any]] = None
    ) -> ApiResponse:
        """Create success response"""
        return ApiResponse(
            status=ResponseStatus.SUCCESS,
            message=message,
            data=data,
            meta=meta
        )

    @staticmethod
    def error(
        message: str,
        errors: Optional[List[ErrorDetail]] = None,
        meta: Optional[Dict[str, Any]] = None
    ) -> ApiResponse:
        """Create error response"""
        return ApiResponse(
            status=ResponseStatus.ERROR,
            message=message,
            errors=errors,
            meta=meta
        )

    @staticmethod
    def warning(
        message: str,
        data: Optional[Dict[str, Any]] = None,
        meta: Optional[Dict[str, Any]] = None
    ) -> ApiResponse:
        """Create warning response"""
        return ApiResponse(
            status=ResponseStatus.WARNING,
            message=message,
            data=data,
            meta=meta
        )

    @staticmethod
    def info(
        message: str,
        data: Optional[Dict[str, Any]] = None,
        meta: Optional[Dict[str, Any]] = None
    ) -> ApiResponse:
        """Create info response"""
        return ApiResponse(
            status=ResponseStatus.INFO,
            message=message,
            data=data,
            meta=meta
        )

    @staticmethod
    def paginated(
        data: List[Dict[str, Any]],
        page: int,
        page_size: int,
        total: int,
        message: str = "Success"
    ) -> PaginatedResponse:
        """Create paginated response"""
        total_pages = (total + page_size - 1) // page_size

        pagination = {
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_previous": page > 1
        }

        return PaginatedResponse(
            message=message,
            data=data,
            pagination=pagination,
            total=total
        )

    @staticmethod
    def from_exception(exception: Exception) -> ApiResponse:
        """Create error response from exception"""
        if isinstance(exception, BaseAmoException):
            return ApiResponseBuilder._handle_amo_exception(exception)
        elif isinstance(exception, HTTPException):
            return ApiResponseBuilder._handle_http_exception(exception)
        else:
            return ApiResponseBuilder._handle_generic_exception(exception)

    @staticmethod
    def _handle_amo_exception(exception: BaseAmoException) -> ApiResponse:
        """Handle AmoCRM specific exceptions"""
        error_detail = ErrorDetail(
            code=exception.error_code,
            message=exception.message,
            category=exception.category.value,
            severity=exception.severity.value,
            details=exception.context.to_dict() if exception.context else {}
        )

        # Add original error if available
        if exception.original_error:
            error_detail.details["original_error"] = str(exception.original_error)

        return ApiResponse(
            status=ResponseStatus.ERROR,
            message=exception.message,
            errors=[error_detail],
            meta={
                "error_code": exception.error_code,
                "category": exception.category.value,
                "severity": exception.severity.value,
                "is_retryable": exception.is_retryable,
                "retry_after": exception.retry_after
            }
        )

    @staticmethod
    def _handle_http_exception(exception: HTTPException) -> ApiResponse:
        """Handle HTTP exceptions"""
        error_detail = ErrorDetail(
            code="HTTP_ERROR",
            message=exception.detail,
            details={"status_code": exception.status_code}
        )

        return ApiResponse(
            status=ResponseStatus.ERROR,
            message=exception.detail,
            errors=[error_detail],
            meta={"status_code": exception.status_code}
        )

    @staticmethod
    def _handle_generic_exception(exception: Exception) -> ApiResponse:
        """Handle generic exceptions"""
        error_detail = ErrorDetail(
            code="GENERIC_ERROR",
            message=str(exception),
            details={"exception_type": type(exception).__name__}
        )

        return ApiResponse(
            status=ResponseStatus.ERROR,
            message="An unexpected error occurred",
            errors=[error_detail],
            meta={"exception_type": type(exception).__name__}
        )


class HttpStatusMapper:
    """Maps exception types to HTTP status codes"""

    EXCEPTION_STATUS_MAP = {
        ValidationError: 400,
        AuthenticationError: 401,
        AuthorizationError: 403,
        ApiError: 502,
        DatabaseError: 503,
        NetworkError: 502,
        ProcessingError: 500,
        ExportError: 500,
        StorageError: 500,
        BusinessLogicError: 400,
        SystemError: 500
    }

    @classmethod
    def get_status_code(cls, exception: Exception) -> int:
        """Get appropriate HTTP status code for exception"""
        if isinstance(exception, HTTPException):
            return exception.status_code

        if isinstance(exception, BaseAmoException):
            # Map by exception type
            for exc_type, status_code in cls.EXCEPTION_STATUS_MAP.items():
                if isinstance(exception, exc_type):
                    return status_code

            # Default based on severity
            if exception.severity == ErrorSeverity.CRITICAL:
                return 500
            elif exception.severity == ErrorSeverity.HIGH:
                return 500
            elif exception.severity == ErrorSeverity.MEDIUM:
                return 400
            else:
                return 200

        return 500  # Default for unknown exceptions


def create_json_response(
    response: Union[ApiResponse, PaginatedResponse],
    status_code: Optional[int] = None
) -> JSONResponse:
    """Create JSON response with proper status code"""

    if status_code is None:
        if response.status == ResponseStatus.SUCCESS:
            status_code = 200
        elif response.status == ResponseStatus.ERROR:
            status_code = 500
        elif response.status == ResponseStatus.WARNING:
            status_code = 200
        else:
            status_code = 200

    return JSONResponse(
        content=response.to_dict(),
        status_code=status_code
    )


def create_error_response(
    exception: Exception,
    status_code: Optional[int] = None
) -> JSONResponse:
    """Create error response from exception"""

    # Log the exception
    log_event("api_error", "error", f"API Error: {str(exception)}")

    response = ApiResponseBuilder.from_exception(exception)

    if status_code is None:
        status_code = HttpStatusMapper.get_status_code(exception)

    return create_json_response(response, status_code)


def success_response(
    message: str = "Success",
    data: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None
) -> JSONResponse:
    """Create success response"""
    response = ApiResponseBuilder.success(message, data, meta)
    return create_json_response(response, 200)


def error_response(
    message: str,
    errors: Optional[List[Dict[str, Any]]] = None,
    status_code: int = 500,
    meta: Optional[Dict[str, Any]] = None
) -> JSONResponse:
    """Create error response"""
    error_details = []
    if errors:
        for error in errors:
            if isinstance(error, dict):
                error_details.append(ErrorDetail(
                    code=error.get("code", "UNKNOWN"),
                    message=error.get("message", "Unknown error"),
                    field=error.get("field"),
                    category=error.get("category"),
                    severity=error.get("severity"),
                    details=error.get("details", {})
                ))
            else:
                error_details.append(error)

    response = ApiResponseBuilder.error(message, error_details, meta)
    return create_json_response(response, status_code)


def paginated_response(
    data: List[Dict[str, Any]],
    page: int,
    page_size: int,
    total: int,
    message: str = "Success"
) -> JSONResponse:
    """Create paginated response"""
    response = ApiResponseBuilder.paginated(data, page, page_size, total, message)
    return create_json_response(response, 200)


def validation_error_response(
    message: str = "Validation failed",
    field_errors: Optional[Dict[str, List[str]]] = None
) -> JSONResponse:
    """Create validation error response"""
    errors = []

    if field_errors:
        for field, field_messages in field_errors.items():
            for field_message in field_messages:
                errors.append(ErrorDetail(
                    code="VALIDATION_ERROR",
                    message=field_message,
                    field=field,
                    category="validation",
                    severity="medium"
                ))

    response = ApiResponseBuilder.error(message, errors)
    return create_json_response(response, 400)


# Decorator for consistent error handling
def handle_api_errors(func):
    """Decorator for consistent API error handling"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return create_error_response(e)
    return wrapper


# Async version of the decorator
def handle_api_errors_async(func):
    """Async decorator for consistent API error handling"""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            return create_error_response(e)
    return wrapper