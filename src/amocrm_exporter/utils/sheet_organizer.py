"""
Sheet Organization System for Google Sheets Export

This module provides sheet organization capabilities:
- Separate sheets for deals and contacts
- Combined reference sheet for companies, users, pipelines
- Clear sheet naming with date range indicators
- Consistent data formatting and structure
"""

from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum
import json

from ..core.logger import log_event
from ..utils.entity_types import EntityType
from .data_filter_engine import SheetData, DateFilter


class SheetType(str, Enum):
    """Types of sheets for organization"""
    DEALS = "deals"
    CONTACTS = "contacts"
    REFERENCE = "reference"


@dataclass
class SheetConfiguration:
    """Configuration for sheet organization"""
    separate_deals_contacts: bool = True
    combine_reference_entities: bool = True
    include_date_in_names: bool = True
    max_sheet_name_length: int = 31  # Google Sheets limit
    include_entity_type_column: bool = True  # For reference sheet


@dataclass
class OrganizedSheets:
    """Result of sheet organization"""
    sheets: Dict[str, SheetData]
    sheet_mapping: Dict[str, List[str]]  # sheet_name -> entity_types
    organization_stats: Dict[str, Any]


class SheetOrganizer:
    """Organizes export data into properly structured sheets"""

    def __init__(self, config: Optional[SheetConfiguration] = None):
        """Initialize the sheet organizer"""
        self.config = config or SheetConfiguration()

        # Define reference entities that should be combined
        self.reference_entities = {
            EntityType.COMPANIES.value,
            EntityType.USERS.value,
            EntityType.PIPELINES.value
        }

        log_event("sheet_organizer", "info", "SheetOrganizer initialized")

    def organize_data_into_sheets(self, filtered_data: Dict[str, List[Dict[str, Any]]],
                                 date_filter: Optional[DateFilter] = None) -> OrganizedSheets:
        """
        Organize filtered data into sheets according to configuration

        Args:
            filtered_data: Filtered entity data
            date_filter: Applied date filter for naming

        Returns:
            OrganizedSheets with all sheet data and metadata
        """
        log_event("sheet_organizer", "info", "Starting sheet organization")

        sheets = {}
        sheet_mapping = {}
        stats = {
            "total_sheets": 0,
            "total_records": 0,
            "sheets_by_type": {}
        }

        # Generate date suffix for sheet names
        date_suffix = self._generate_date_suffix(date_filter) if self.config.include_date_in_names else ""

        # Organize deals sheet
        deals_sheet = self._create_deals_sheet(filtered_data, date_suffix)
        if deals_sheet:
            sheets[deals_sheet.sheet_name] = deals_sheet
            sheet_mapping[deals_sheet.sheet_name] = deals_sheet.entity_types
            stats["sheets_by_type"]["deals"] = deals_sheet.record_count
            stats["total_records"] += deals_sheet.record_count

        # Organize contacts sheet
        contacts_sheet = self._create_contacts_sheet(filtered_data, date_suffix)
        if contacts_sheet:
            sheets[contacts_sheet.sheet_name] = contacts_sheet
            sheet_mapping[contacts_sheet.sheet_name] = contacts_sheet.entity_types
            stats["sheets_by_type"]["contacts"] = contacts_sheet.record_count
            stats["total_records"] += contacts_sheet.record_count

        # Organize reference sheet (companies, users, pipelines)
        reference_sheet = self._create_reference_sheet(filtered_data, date_suffix)
        if reference_sheet:
            sheets[reference_sheet.sheet_name] = reference_sheet
            sheet_mapping[reference_sheet.sheet_name] = reference_sheet.entity_types
            stats["sheets_by_type"]["reference"] = reference_sheet.record_count
            stats["total_records"] += reference_sheet.record_count

        stats["total_sheets"] = len(sheets)

        log_event("sheet_organizer", "info",
                 f"Sheet organization complete: {stats['total_sheets']} sheets, "
                 f"{stats['total_records']} total records")

        return OrganizedSheets(
            sheets=sheets,
            sheet_mapping=sheet_mapping,
            organization_stats=stats
        )

    def _create_deals_sheet(self, filtered_data: Dict[str, List[Dict[str, Any]]],
                           date_suffix: str) -> Optional[SheetData]:
        """Create deals sheet"""
        # Handle both 'deals' and 'leads' keys (AmoCRM uses 'leads' internally)
        deals_data = filtered_data.get(EntityType.DEALS.value, filtered_data.get("leads", []))

        if not deals_data:
            log_event("sheet_organizer", "info", "No deals data found, skipping deals sheet")
            return None

        sheet_name = self._generate_sheet_name("Deals", date_suffix)

        return self._create_sheet_data(
            sheet_name=sheet_name,
            data=deals_data,
            entity_types=[EntityType.DEALS.value],
            sheet_type=SheetType.DEALS
        )

    def _create_contacts_sheet(self, filtered_data: Dict[str, List[Dict[str, Any]]],
                              date_suffix: str) -> Optional[SheetData]:
        """Create contacts sheet"""
        contacts_data = filtered_data.get(EntityType.CONTACTS.value, [])

        if not contacts_data:
            log_event("sheet_organizer", "info", "No contacts data found, skipping contacts sheet")
            return None

        sheet_name = self._generate_sheet_name("Contacts", date_suffix)

        return self._create_sheet_data(
            sheet_name=sheet_name,
            data=contacts_data,
            entity_types=[EntityType.CONTACTS.value],
            sheet_type=SheetType.CONTACTS
        )

    def _create_reference_sheet(self, filtered_data: Dict[str, List[Dict[str, Any]]],
                               date_suffix: str) -> Optional[SheetData]:
        """Create combined reference sheet for companies, users, and pipelines"""
        reference_data = []
        included_entity_types = []

        # Collect data from all reference entities
        for entity_type in self.reference_entities:
            if entity_type in filtered_data and filtered_data[entity_type]:
                entity_data = filtered_data[entity_type]

                # Add entity type indicator to each record if configured
                if self.config.include_entity_type_column:
                    for record in entity_data:
                        if isinstance(record, dict):
                            # Create a copy to avoid modifying original data
                            record_copy = record.copy()
                            record_copy['_entity_type'] = entity_type
                            reference_data.append(record_copy)
                        else:
                            reference_data.append(record)
                else:
                    reference_data.extend(entity_data)

                included_entity_types.append(entity_type)

        if not reference_data:
            log_event("sheet_organizer", "info", "No reference data found, skipping reference sheet")
            return None

        sheet_name = self._generate_sheet_name("Reference", date_suffix)

        log_event("sheet_organizer", "info",
                 f"Creating reference sheet with {len(reference_data)} records from "
                 f"{len(included_entity_types)} entity types: {included_entity_types}")

        return self._create_sheet_data(
            sheet_name=sheet_name,
            data=reference_data,
            entity_types=included_entity_types,
            sheet_type=SheetType.REFERENCE
        )

    def _generate_sheet_name(self, base_name: str, date_suffix: str) -> str:
        """Generate sheet name with proper length limits"""
        full_name = f"{base_name}{date_suffix}"

        # Ensure name doesn't exceed Google Sheets limit
        if len(full_name) > self.config.max_sheet_name_length:
            # Truncate the date suffix if needed
            available_length = self.config.max_sheet_name_length - len(base_name)
            if available_length > 0:
                truncated_suffix = date_suffix[:available_length]
                full_name = f"{base_name}{truncated_suffix}"
            else:
                # If base name is too long, truncate it
                full_name = base_name[:self.config.max_sheet_name_length]

        return full_name

    def _generate_date_suffix(self, date_filter: Optional[DateFilter]) -> str:
        """Generate date range suffix for sheet names"""
        if not date_filter or (not date_filter.date_from and not date_filter.date_to):
            return ""

        try:
            parts = []

            if date_filter.date_from:
                from_dt = datetime.fromisoformat(date_filter.date_from.replace('Z', '+00:00'))
                parts.append(from_dt.strftime("%m%d"))  # Shorter format for sheet names

            if date_filter.date_to:
                to_dt = datetime.fromisoformat(date_filter.date_to.replace('Z', '+00:00'))
                if date_filter.date_from:
                    parts.append(to_dt.strftime("%m%d"))
                else:
                    parts.append(f"to{to_dt.strftime('%m%d')}")

            if parts:
                if len(parts) == 2:
                    return f"_{parts[0]}-{parts[1]}"
                else:
                    return f"_{parts[0]}"

        except ValueError as e:
            log_event("sheet_organizer", "warning", f"Could not generate date suffix: {e}")

        return ""

    def _create_sheet_data(self, sheet_name: str, data: List[Dict[str, Any]],
                          entity_types: List[str], sheet_type: SheetType) -> SheetData:
        """
        Create SheetData object with proper formatting and structure

        Args:
            sheet_name: Name of the sheet
            data: Entity data records
            entity_types: List of entity types included in this sheet
            sheet_type: Type of sheet being created

        Returns:
            SheetData object with formatted data
        """
        if not data:
            return SheetData(
                sheet_name=sheet_name,
                headers=[],
                rows=[],
                entity_types=entity_types,
                record_count=0
            )

        # Collect all unique headers from the data
        all_headers: Set[str] = set()
        for record in data:
            if isinstance(record, dict):
                all_headers.update(record.keys())

        # Sort headers with special ordering for better readability
        headers = self._sort_headers(list(all_headers), sheet_type)

        # Create rows with proper formatting
        rows = []
        for record in data:
            if not isinstance(record, dict):
                continue

            row = []
            for header in headers:
                value = record.get(header, '')
                formatted_value = self._format_cell_value(value, header)
                row.append(formatted_value)

            rows.append(row)

        log_event("sheet_organizer", "info",
                 f"Created {sheet_type.value} sheet '{sheet_name}' with {len(headers)} columns "
                 f"and {len(rows)} rows")

        return SheetData(
            sheet_name=sheet_name,
            headers=headers,
            rows=rows,
            entity_types=entity_types,
            record_count=len(rows)
        )

    def _sort_headers(self, headers: List[str], sheet_type: SheetType) -> List[str]:
        """Sort headers for better readability with important fields first"""
        # Define priority fields for different sheet types
        priority_fields = {
            SheetType.DEALS: ['id', 'name', 'status_id', 'pipeline_id', 'price', 'created_at', 'updated_at'],
            SheetType.CONTACTS: ['id', 'name', 'first_name', 'last_name', 'created_at', 'updated_at'],
            SheetType.REFERENCE: ['_entity_type', 'id', 'name', 'title', 'created_at', 'updated_at']
        }

        priority = priority_fields.get(sheet_type, ['id', 'name', 'created_at', 'updated_at'])

        # Separate priority fields from others
        priority_headers = []
        other_headers = []

        for field in priority:
            if field in headers:
                priority_headers.append(field)

        for header in sorted(headers):
            if header not in priority_headers:
                other_headers.append(header)

        return priority_headers + other_headers

    def _format_cell_value(self, value: Any, header: str) -> Any:
        """Format cell value for Google Sheets compatibility"""
        if value is None:
            return ""

        # Handle complex data types
        if isinstance(value, (list, dict)):
            try:
                return json.dumps(value, ensure_ascii=False)
            except (TypeError, ValueError):
                return str(value)

        # Handle strings that might cause issues in Google Sheets
        if isinstance(value, str):
            # Protect values that start with special characters
            if value.startswith(('+', '=', '-', '@')):
                return f"'{value}"
            return value

        # Handle timestamps for date fields
        if header in ('created_at', 'updated_at', 'closed_at', 'closest_task_at'):
            return self._format_timestamp(value)

        return value

    def _format_timestamp(self, timestamp: Any) -> Any:
        """Format timestamp for Google Sheets"""
        if timestamp is None:
            return ""

        try:
            # Convert to integer if it's a string
            if isinstance(timestamp, str):
                timestamp = timestamp.replace("'", "")
                timestamp = int(timestamp)

            # Validate timestamp range (2015-2050) - business data should be recent
            # January 1, 2015 00:00:00 UTC = 1420070400
            # January 1, 2050 00:00:00 UTC = 2524608000
            if not (1420070400 <= timestamp <= 2524608000):
                # Don't log here to avoid spam, just return original value
                return timestamp

            # Convert timestamp to datetime
            dt = datetime.fromtimestamp(int(timestamp))

            # Return as Excel/Google Sheets compatible date formula
            return f'=DATE({dt.year};{dt.month};{dt.day})+TIME({dt.hour};{dt.minute};{dt.second})'

        except (ValueError, TypeError, OSError):
            # If conversion fails, return the original value
            return timestamp

    def get_sheet_summary(self, organized_sheets: OrganizedSheets) -> Dict[str, Any]:
        """Get summary information about organized sheets"""
        summary = {
            "total_sheets": len(organized_sheets.sheets),
            "total_records": organized_sheets.organization_stats.get("total_records", 0),
            "sheets": []
        }

        for sheet_name, sheet_data in organized_sheets.sheets.items():
            sheet_info = {
                "name": sheet_name,
                "entity_types": sheet_data.entity_types,
                "record_count": sheet_data.record_count,
                "column_count": len(sheet_data.headers),
                "columns": sheet_data.headers[:10]  # First 10 columns for preview
            }
            summary["sheets"].append(sheet_info)

        return summary

    def validate_sheet_organization(self, organized_sheets: OrganizedSheets) -> Dict[str, Any]:
        """Validate the organized sheets for common issues"""
        validation_results = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }

        for sheet_name, sheet_data in organized_sheets.sheets.items():
            # Check sheet name length
            if len(sheet_name) > self.config.max_sheet_name_length:
                validation_results["errors"].append(
                    f"Sheet name '{sheet_name}' exceeds maximum length of {self.config.max_sheet_name_length}"
                )
                validation_results["is_valid"] = False

            # Check for empty sheets
            if sheet_data.record_count == 0:
                validation_results["warnings"].append(f"Sheet '{sheet_name}' is empty")

            # Check for very large sheets
            if sheet_data.record_count > 100000:
                validation_results["warnings"].append(
                    f"Sheet '{sheet_name}' has {sheet_data.record_count} records, "
                    "which may cause performance issues"
                )

            # Check for too many columns
            if len(sheet_data.headers) > 1000:
                validation_results["warnings"].append(
                    f"Sheet '{sheet_name}' has {len(sheet_data.headers)} columns, "
                    "which may cause performance issues"
                )

        return validation_results