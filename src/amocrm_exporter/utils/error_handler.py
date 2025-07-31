"""
Comprehensive error handling system for Google Sheets export
"""

import logging
import traceback
from typing import Dict, Any, Optional, List, Tuple, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

from .exceptions import (
    BaseAmoException,
    GoogleSheetsError,
    GoogleSheetsAuthError,
    GoogleSheetsPermissionError,
    GoogleSheetsQuotaError,
    GoogleSheetsRateLimitError,
    GoogleSheetsConfigError,
    GoogleSheetsSpreadsheetNotFoundError,
    GoogleSheetsSheetNotFoundError,
    GoogleSheetsBatchError,
    GoogleSheetsPartialExportError,
    ErrorContext,
    ErrorSeverity,
    create_error_context
)


class UserActionType(str, Enum):
    """Types of actions users can take to resolve errors"""
    RETRY = "retry"
    RECONFIGURE = "reconfigure"
    CHECK_PERMISSIONS = "check_permissions"
    CONTACT_ADMIN = "contact_admin"
    WAIT = "wait"
    UPDATE_CREDENTIALS = "update_credentials"
    CHECK_NETWORK = "check_network"
    REDUCE_BATCH_SIZE = "reduce_batch_size"
    NONE = "none"


@dataclass
class UserAction:
    """Suggested user action for error resolution"""
    action_type: UserActionType
    title: str
    description: str
    details: Optional[str] = None
    url: Optional[str] = None
    estimated_time: Optional[str] = None


@dataclass
class ErrorReport:
    """Comprehensive error report with user-friendly information"""
    error_id: str
    timestamp: datetime
    error_type: str
    severity: ErrorSeverity
    user_message: str
    technical_message: str
    context: Dict[str, Any]
    suggested_actions: List[UserAction]
    is_retryable: bool
    retry_after: Optional[int] = None
    additional_info: Dict[str, Any] = field(default_factory=dict)


class GoogleSheetsErrorHandler:
    """Comprehensive error handler for Google Sheets operations"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._error_counter = 0

    def handle_error(
        self,
        error: Exception,
        context: Optional[ErrorContext] = None,
        operation: Optional[str] = None
    ) -> ErrorReport:
        """Handle any error and create comprehensive error report"""
        self._error_counter += 1
        error_id = f"GSE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self._error_counter:04d}"

        if context is None:
            context = create_error_context(
                component="google_sheets_exporter",
                operation=operation or "unknown"
            )

        # Log the error with full context
        self._log_error_with_context(error, context, error_id)

        # Create error report based on error type
        if isinstance(error, GoogleSheetsError):
            return self._handle_google_sheets_error(error, context, error_id)
        elif isinstance(error, BaseAmoException):
            return self._handle_base_amo_error(error, context, error_id)
        else:
            return self._handle_generic_error(error, context, error_id)

    def _log_error_with_context(
        self,
        error: Exception,
        context: ErrorContext,
        error_id: str
    ) -> None:
        """Log error with full context information"""
        log_data = {
            "error_id": error_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context.to_dict(),
            "stack_trace": traceback.format_exc()
        }

        if isinstance(error, BaseAmoException):
            log_data.update({
                "category": error.category.value,
                "severity": error.severity.value,
                "is_retryable": error.is_retryable,
                "retry_after": error.retry_after
            })

        # Log at appropriate level based on severity
        if isinstance(error, BaseAmoException):
            if error.severity == ErrorSeverity.CRITICAL:
                self.logger.critical("Critical error occurred", extra=log_data)
            elif error.severity == ErrorSeverity.HIGH:
                self.logger.error("High severity error occurred", extra=log_data)
            elif error.severity == ErrorSeverity.MEDIUM:
                self.logger.warning("Medium severity error occurred", extra=log_data)
            else:
                self.logger.info("Low severity error occurred", extra=log_data)
        else:
            self.logger.error("Unhandled error occurred", extra=log_data)

    def _handle_google_sheets_error(
        self,
        error: GoogleSheetsError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Handle Google Sheets specific errors"""
        if isinstance(error, GoogleSheetsAuthError):
            return self._create_auth_error_report(error, context, error_id)
        elif isinstance(error, GoogleSheetsPermissionError):
            return self._create_permission_error_report(error, context, error_id)
        elif isinstance(error, GoogleSheetsQuotaError):
            return self._create_quota_error_report(error, context, error_id)
        elif isinstance(error, GoogleSheetsRateLimitError):
            return self._create_rate_limit_error_report(error, context, error_id)
        elif isinstance(error, GoogleSheetsConfigError):
            return self._create_config_error_report(error, context, error_id)
        elif isinstance(error, GoogleSheetsSpreadsheetNotFoundError):
            return self._create_spreadsheet_not_found_error_report(error, context, error_id)
        elif isinstance(error, GoogleSheetsSheetNotFoundError):
            return self._create_sheet_not_found_error_report(error, context, error_id)
        elif isinstance(error, GoogleSheetsBatchError):
            return self._create_batch_error_report(error, context, error_id)
        elif isinstance(error, GoogleSheetsPartialExportError):
            return self._create_partial_export_error_report(error, context, error_id)
        else:
            return self._create_generic_sheets_error_report(error, context, error_id)

    def _create_auth_error_report(
        self,
        error: GoogleSheetsAuthError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for authentication errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsAuthError",
            severity=error.severity,
            user_message="Google Sheets authentication failed. Your credentials may have expired or been revoked.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.UPDATE_CREDENTIALS,
                    title="Update Google Credentials",
                    description="Re-authenticate with Google Sheets API",
                    details="Delete the token.json file and restart the application to re-authenticate",
                    estimated_time="2-5 minutes"
                ),
                UserAction(
                    action_type=UserActionType.CHECK_PERMISSIONS,
                    title="Check API Permissions",
                    description="Verify that the Google Sheets API is enabled in your Google Cloud Console",
                    url="https://console.cloud.google.com/apis/library/sheets.googleapis.com",
                    estimated_time="1-2 minutes"
                )
            ],
            is_retryable=error.is_retryable,
            retry_after=error.retry_after
        )

    def _create_permission_error_report(
        self,
        error: GoogleSheetsPermissionError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for permission errors"""
        spreadsheet_url = ""
        if error.spreadsheet_id:
            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{error.spreadsheet_id}"

        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsPermissionError",
            severity=error.severity,
            user_message="Insufficient permissions to access the Google Spreadsheet. The application needs edit access to write data.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.CHECK_PERMISSIONS,
                    title="Grant Spreadsheet Access",
                    description="Share the spreadsheet with the service account email or your Google account",
                    details="Open the spreadsheet and click 'Share', then add the email with 'Editor' permissions",
                    url=spreadsheet_url,
                    estimated_time="1-2 minutes"
                ),
                UserAction(
                    action_type=UserActionType.RECONFIGURE,
                    title="Check Spreadsheet ID",
                    description="Verify that the spreadsheet ID in configuration is correct",
                    details="The spreadsheet ID is the long string in the Google Sheets URL",
                    estimated_time="1 minute"
                )
            ],
            is_retryable=False,
            additional_info={"spreadsheet_id": error.spreadsheet_id}
        )

    def _create_quota_error_report(
        self,
        error: GoogleSheetsQuotaError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for quota errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsQuotaError",
            severity=error.severity,
            user_message="Google Sheets API quota has been exceeded. The export will automatically retry after the quota resets.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.WAIT,
                    title="Wait for Quota Reset",
                    description="API quotas typically reset every 24 hours",
                    details="The system will automatically retry when the quota resets",
                    estimated_time=f"{error.retry_after // 60} minutes"
                ),
                UserAction(
                    action_type=UserActionType.REDUCE_BATCH_SIZE,
                    title="Reduce Export Size",
                    description="Try exporting smaller datasets or use date filters",
                    details="Consider filtering by date range to reduce the amount of data being exported",
                    estimated_time="2-3 minutes"
                )
            ],
            is_retryable=error.is_retryable,
            retry_after=error.retry_after
        )

    def _create_rate_limit_error_report(
        self,
        error: GoogleSheetsRateLimitError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for rate limit errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsRateLimitError",
            severity=error.severity,
            user_message="Google Sheets API rate limit exceeded. The export will automatically retry with appropriate delays.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.WAIT,
                    title="Automatic Retry",
                    description="The system will automatically retry with exponential backoff",
                    details="No action needed - the export will continue automatically",
                    estimated_time=f"{error.retry_after} seconds"
                )
            ],
            is_retryable=error.is_retryable,
            retry_after=error.retry_after
        )

    def _create_config_error_report(
        self,
        error: GoogleSheetsConfigError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for configuration errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsConfigError",
            severity=error.severity,
            user_message="Google Sheets configuration is invalid or incomplete. Please check your settings.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.RECONFIGURE,
                    title="Check Configuration",
                    description="Verify all Google Sheets settings in your .env file",
                    details="Ensure GOOGLE_SHEETS_*_ID variables are set with valid spreadsheet IDs",
                    estimated_time="2-5 minutes"
                ),
                UserAction(
                    action_type=UserActionType.UPDATE_CREDENTIALS,
                    title="Check Credentials",
                    description="Verify that credentials.json file exists and is valid",
                    details="Download the credentials file from Google Cloud Console if missing",
                    estimated_time="3-5 minutes"
                )
            ],
            is_retryable=False,
            additional_info={"config_key": getattr(error, 'config_key', None)}
        )

    def _create_spreadsheet_not_found_error_report(
        self,
        error: GoogleSheetsSpreadsheetNotFoundError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for spreadsheet not found errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsSpreadsheetNotFoundError",
            severity=error.severity,
            user_message=f"The Google Spreadsheet could not be found. Please check the spreadsheet ID: {error.spreadsheet_id}",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.RECONFIGURE,
                    title="Verify Spreadsheet ID",
                    description="Check that the spreadsheet ID in configuration is correct",
                    details="The spreadsheet ID is the long string between '/d/' and '/edit' in the Google Sheets URL",
                    estimated_time="1-2 minutes"
                ),
                UserAction(
                    action_type=UserActionType.CHECK_PERMISSIONS,
                    title="Check Spreadsheet Access",
                    description="Ensure the spreadsheet exists and you have access to it",
                    details="Try opening the spreadsheet URL directly in your browser",
                    estimated_time="1 minute"
                )
            ],
            is_retryable=False,
            additional_info={"spreadsheet_id": error.spreadsheet_id}
        )

    def _create_sheet_not_found_error_report(
        self,
        error: GoogleSheetsSheetNotFoundError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for sheet not found errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsSheetNotFoundError",
            severity=error.severity,
            user_message=f"The sheet '{error.sheet_name}' was not found in the spreadsheet.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.RETRY,
                    title="Retry Export",
                    description="The system will create the sheet automatically on retry",
                    details="Sheets are created automatically if they don't exist",
                    estimated_time="30 seconds"
                )
            ],
            is_retryable=True,
            retry_after=5,
            additional_info={
                "sheet_name": error.sheet_name,
                "spreadsheet_id": error.spreadsheet_id
            }
        )

    def _create_batch_error_report(
        self,
        error: GoogleSheetsBatchError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for batch operation errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsBatchError",
            severity=error.severity,
            user_message="A batch operation failed while writing data to Google Sheets. The system will retry with a smaller batch size.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.RETRY,
                    title="Automatic Retry",
                    description="The system will automatically retry with a smaller batch size",
                    details="Batch size will be reduced to handle the data more reliably",
                    estimated_time="1-2 minutes"
                )
            ],
            is_retryable=error.is_retryable,
            retry_after=error.retry_after,
            additional_info={
                "batch_size": error.batch_size,
                "failed_rows": error.failed_rows
            }
        )

    def _create_partial_export_error_report(
        self,
        error: GoogleSheetsPartialExportError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for partial export errors"""
        exported_count = sum(error.exported_entities.values())
        failed_entities_str = ", ".join(error.failed_entities)

        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsPartialExportError",
            severity=error.severity,
            user_message=f"Export partially completed. {exported_count} entities exported successfully. Failed entities: {failed_entities_str}",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.RETRY,
                    title="Retry Failed Entities",
                    description="Retry exporting only the failed entity types",
                    details="The successfully exported entities don't need to be re-exported",
                    estimated_time="2-5 minutes"
                ),
                UserAction(
                    action_type=UserActionType.CHECK_PERMISSIONS,
                    title="Check Entity Permissions",
                    description="Verify permissions for the failed entity spreadsheets",
                    details="Some entity types may have different permission requirements",
                    estimated_time="2-3 minutes"
                )
            ],
            is_retryable=True,
            retry_after=30,
            additional_info={
                "exported_entities": error.exported_entities,
                "failed_entities": error.failed_entities
            }
        )

    def _create_generic_sheets_error_report(
        self,
        error: GoogleSheetsError,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Create error report for generic Google Sheets errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type="GoogleSheetsError",
            severity=error.severity,
            user_message="An error occurred while working with Google Sheets. Please check your configuration and try again.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.RETRY,
                    title="Retry Operation",
                    description="Try the operation again",
                    estimated_time="30 seconds"
                ),
                UserAction(
                    action_type=UserActionType.CHECK_NETWORK,
                    title="Check Network Connection",
                    description="Verify your internet connection is stable",
                    estimated_time="1 minute"
                )
            ],
            is_retryable=error.is_retryable,
            retry_after=error.retry_after
        )

    def _handle_base_amo_error(
        self,
        error: BaseAmoException,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Handle BaseAmoException errors"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type=type(error).__name__,
            severity=error.severity,
            user_message=self._generate_user_friendly_message(error),
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=self._generate_suggested_actions(error),
            is_retryable=error.is_retryable,
            retry_after=error.retry_after
        )

    def _handle_generic_error(
        self,
        error: Exception,
        context: ErrorContext,
        error_id: str
    ) -> ErrorReport:
        """Handle generic Python exceptions"""
        return ErrorReport(
            error_id=error_id,
            timestamp=datetime.now(),
            error_type=type(error).__name__,
            severity=ErrorSeverity.HIGH,
            user_message="An unexpected error occurred during the Google Sheets export operation.",
            technical_message=str(error),
            context=context.to_dict(),
            suggested_actions=[
                UserAction(
                    action_type=UserActionType.RETRY,
                    title="Retry Operation",
                    description="Try the operation again",
                    estimated_time="30 seconds"
                ),
                UserAction(
                    action_type=UserActionType.CONTACT_ADMIN,
                    title="Contact Administrator",
                    description="If the problem persists, contact your system administrator",
                    details=f"Error ID: {error_id}",
                    estimated_time="N/A"
                )
            ],
            is_retryable=True,
            retry_after=30
        )

    def _generate_user_friendly_message(self, error: BaseAmoException) -> str:
        """Generate user-friendly error message"""
        category_messages = {
            "authentication": "Authentication failed. Please check your credentials.",
            "authorization": "Access denied. Please check your permissions.",
            "validation": "Invalid data provided. Please check your input.",
            "network": "Network connection issue. Please check your internet connection.",
            "database": "Database error occurred. Please try again later.",
            "external_api": "External service error. Please try again later.",
            "configuration": "Configuration error. Please check your settings.",
            "processing": "Data processing error occurred.",
            "storage": "Storage error occurred.",
            "export": "Export operation failed.",
            "system": "System error occurred.",
            "business_logic": "Business logic error occurred."
        }

        return category_messages.get(error.category.value, "An error occurred during the operation.")

    def _generate_suggested_actions(self, error: BaseAmoException) -> List[UserAction]:
        """Generate suggested actions based on error type"""
        if error.is_retryable:
            actions = [
                UserAction(
                    action_type=UserActionType.RETRY,
                    title="Retry Operation",
                    description="The operation can be retried",
                    estimated_time=f"{error.retry_after or 30} seconds"
                )
            ]
        else:
            actions = []

        # Add category-specific actions
        if error.category.value == "configuration":
            actions.append(
                UserAction(
                    action_type=UserActionType.RECONFIGURE,
                    title="Check Configuration",
                    description="Review and update your configuration settings",
                    estimated_time="2-5 minutes"
                )
            )
        elif error.category.value == "network":
            actions.append(
                UserAction(
                    action_type=UserActionType.CHECK_NETWORK,
                    title="Check Network",
                    description="Verify your internet connection",
                    estimated_time="1-2 minutes"
                )
            )

        return actions

    def format_error_for_user(self, error_report: ErrorReport) -> str:
        """Format error report for user display"""
        lines = [
            f"Error ID: {error_report.error_id}",
            f"Time: {error_report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Severity: {error_report.severity.value.upper()}",
            "",
            f"What happened: {error_report.user_message}",
            ""
        ]

        if error_report.suggested_actions:
            lines.append("What you can do:")
            for i, action in enumerate(error_report.suggested_actions, 1):
                lines.append(f"{i}. {action.title}")
                lines.append(f"   {action.description}")
                if action.details:
                    lines.append(f"   Details: {action.details}")
                if action.estimated_time:
                    lines.append(f"   Estimated time: {action.estimated_time}")
                if action.url:
                    lines.append(f"   URL: {action.url}")
                lines.append("")

        if error_report.is_retryable:
            retry_msg = "This operation will be retried automatically"
            if error_report.retry_after:
                retry_msg += f" in {error_report.retry_after} seconds"
            lines.append(retry_msg)

        return "\n".join(lines)


# Global error handler instance
error_handler = GoogleSheetsErrorHandler()


def handle_google_sheets_error(
    error: Exception,
    context: Optional[ErrorContext] = None,
    operation: Optional[str] = None
) -> ErrorReport:
    """Convenience function for handling Google Sheets errors"""
    return error_handler.handle_error(error, context, operation)