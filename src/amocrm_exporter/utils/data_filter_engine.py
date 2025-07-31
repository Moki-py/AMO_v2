"""
Data Filter Engine for AmoCRM Google Sheets Export

This module provides filtering and organization capabilities for export data:
- Date range filtering for deals
- Contact filtering based on deal relationships
- Entity type filtering with reference data handling
- Sheet organization and naming
"""

from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum

from ..core.logger import log_event
from ..utils.entity_types import EntityType


class SheetType(str, Enum):
    """Types of sheets for organization"""
    DEALS = "deals"
    CONTACTS = "contacts"
    REFERENCE = "reference"  # Combined companies, users, pipelines


@dataclass
class DateFilter:
    """Date filter configuration"""
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    filter_field: str = "updated_at"  # Field to filter on


@dataclass
class SheetData:
    """Data for a specific sheet"""
    sheet_name: str
    headers: List[str]
    rows: List[List[Any]]
    entity_types: List[str]  # Entity types included in this sheet
    record_count: int


@dataclass
class FilterResult:
    """Result of filtering operation"""
    filtered_data: Dict[str, List[Dict[str, Any]]]
    filter_stats: Dict[str, int]
    date_range_applied: Optional[DateFilter]


class DataFilterEngine:
    """Engine for filtering and organizing export data"""

    def __init__(self):
        """Initialize the data filter engine"""
        # Entity types that should always be exported (reference data)
        self.reference_entities = {
            EntityType.COMPANIES.value,
            EntityType.USERS.value,
            EntityType.PIPELINES.value
        }

        # Entity types that should be excluded from export
        self.excluded_entities = {
            EntityType.EVENTS.value
        }

        log_event("data_filter", "info", "DataFilterEngine initialized")

    def apply_date_filter(self, deals: List[Dict[str, Any]], date_filter: Optional[DateFilter]) -> List[Dict[str, Any]]:
        """
        Apply optional date range filtering to deals

        Args:
            deals: List of deal records
            date_filter: Date filter configuration

        Returns:
            Filtered list of deals
        """
        if not date_filter or (not date_filter.date_from and not date_filter.date_to):
            log_event("data_filter", "info", f"No date filter applied, returning {len(deals)} deals")
            return deals

        if not deals:
            return deals

        filtered_deals = []
        filter_field = date_filter.filter_field

        # Convert date strings to timestamps for comparison
        from_timestamp = None
        to_timestamp = None

        try:
            if date_filter.date_from:
                from_dt = datetime.fromisoformat(date_filter.date_from.replace('Z', '+00:00'))
                from_timestamp = int(from_dt.timestamp())

            if date_filter.date_to:
                to_dt = datetime.fromisoformat(date_filter.date_to.replace('Z', '+00:00'))
                to_timestamp = int(to_dt.timestamp())

        except ValueError as e:
            log_event("data_filter", "error", f"Invalid date format in filter: {e}")
            return deals

        for deal in deals:
            if not isinstance(deal, dict):
                continue

            # Get the field value to filter on
            field_value = deal.get(filter_field)
            if field_value is None:
                continue

            # Convert field value to timestamp if needed
            try:
                if isinstance(field_value, str):
                    # Try to parse as ISO format first
                    try:
                        dt = datetime.fromisoformat(field_value.replace('Z', '+00:00'))
                        record_timestamp = int(dt.timestamp())
                    except ValueError:
                        # If that fails, assume it's already a timestamp string
                        record_timestamp = int(field_value)
                elif isinstance(field_value, (int, float)):
                    record_timestamp = int(field_value)
                else:
                    continue

            except (ValueError, TypeError):
                log_event("data_filter", "warning", f"Could not parse timestamp from field {filter_field}: {field_value}")
                continue

            # Apply date range filter
            include_record = True

            if from_timestamp is not None and record_timestamp < from_timestamp:
                include_record = False

            if to_timestamp is not None and record_timestamp > to_timestamp:
                include_record = False

            if include_record:
                filtered_deals.append(deal)

        log_event("data_filter", "info",
                 f"Date filter applied: {len(deals)} -> {len(filtered_deals)} deals "
                 f"(from: {date_filter.date_from}, to: {date_filter.date_to})")

        return filtered_deals

    def filter_related_contacts(self, deals: List[Dict[str, Any]], all_contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter contacts to include only those mentioned in the filtered deals

        Args:
            deals: Filtered list of deals
            all_contacts: All available contacts

        Returns:
            Filtered list of contacts that are related to the deals
        """
        if not deals or not all_contacts:
            return all_contacts if not deals else []

        # Extract contact IDs from deals
        contact_ids: Set[int] = set()

        for deal in deals:
            if not isinstance(deal, dict):
                continue

            # Check for contact references in various fields
            # Direct contact ID
            if 'contact_id' in deal and deal['contact_id']:
                try:
                    contact_ids.add(int(deal['contact_id']))
                except (ValueError, TypeError):
                    pass

            # Contacts array
            contacts_field = deal.get('contacts', [])
            if isinstance(contacts_field, list):
                for contact_ref in contacts_field:
                    if isinstance(contact_ref, dict) and 'id' in contact_ref:
                        try:
                            contact_ids.add(int(contact_ref['id']))
                        except (ValueError, TypeError):
                            pass
                    elif isinstance(contact_ref, (int, str)):
                        try:
                            contact_ids.add(int(contact_ref))
                        except (ValueError, TypeError):
                            pass

            # Check embedded contact data
            if 'contact' in deal and isinstance(deal['contact'], dict):
                contact_data = deal['contact']
                if 'id' in contact_data:
                    try:
                        contact_ids.add(int(contact_data['id']))
                    except (ValueError, TypeError):
                        pass

        # Filter contacts based on collected IDs
        filtered_contacts = []
        for contact in all_contacts:
            if not isinstance(contact, dict) or 'id' not in contact:
                continue

            try:
                contact_id = int(contact['id'])
                if contact_id in contact_ids:
                    filtered_contacts.append(contact)
            except (ValueError, TypeError):
                continue

        log_event("data_filter", "info",
                 f"Contact filter applied: {len(all_contacts)} -> {len(filtered_contacts)} contacts "
                 f"(based on {len(deals)} deals with {len(contact_ids)} unique contact references)")

        return filtered_contacts

    def filter_entities_by_type(self, entities_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Filter entities by type, ensuring reference entities are always included and excluded entities are removed

        Args:
            entities_data: Dictionary mapping entity types to their data

        Returns:
            Filtered entities data
        """
        filtered_data = {}

        for entity_type, data in entities_data.items():
            # Skip excluded entity types (like events)
            if entity_type in self.excluded_entities:
                log_event("data_filter", "info", f"Excluding {entity_type} from export (excluded entity type)")
                continue

            # Always include reference entities
            if entity_type in self.reference_entities:
                filtered_data[entity_type] = data
                log_event("data_filter", "info", f"Including {len(data)} {entity_type} records (reference data)")
                continue

            # Include other entity types
            filtered_data[entity_type] = data
            log_event("data_filter", "info", f"Including {len(data)} {entity_type} records")

        return filtered_data

    def organize_sheets(self, filtered_data: Dict[str, List[Dict[str, Any]]],
                       date_filter: Optional[DateFilter] = None) -> Dict[str, SheetData]:
        """
        Organize filtered data into sheets according to requirements:
        - Deals and contacts in separate sheets
        - Companies, users, and pipelines in single combined reference sheet
        - Clear sheet naming with date range indicators

        Args:
            filtered_data: Filtered entity data
            date_filter: Applied date filter for naming

        Returns:
            Dictionary mapping sheet names to SheetData objects
        """
        sheets = {}

        # Generate date range suffix for sheet names
        date_suffix = self._generate_date_suffix(date_filter)

        # Process deals sheet
        if EntityType.DEALS.value in filtered_data or "leads" in filtered_data:
            deals_data = filtered_data.get(EntityType.DEALS.value, filtered_data.get("leads", []))
            if deals_data:
                sheet_name = f"Deals{date_suffix}"
                sheets[sheet_name] = self._create_sheet_data(
                    sheet_name=sheet_name,
                    data=deals_data,
                    entity_types=[EntityType.DEALS.value]
                )

        # Process contacts sheet
        if EntityType.CONTACTS.value in filtered_data:
            contacts_data = filtered_data[EntityType.CONTACTS.value]
            if contacts_data:
                sheet_name = f"Contacts{date_suffix}"
                sheets[sheet_name] = self._create_sheet_data(
                    sheet_name=sheet_name,
                    data=contacts_data,
                    entity_types=[EntityType.CONTACTS.value]
                )

        # Process reference sheet (companies, users, pipelines combined)
        reference_data = []
        reference_entity_types = []

        for entity_type in self.reference_entities:
            if entity_type in filtered_data and filtered_data[entity_type]:
                entity_data = filtered_data[entity_type]
                # Add entity type indicator to each record
                for record in entity_data:
                    if isinstance(record, dict):
                        record['_entity_type'] = entity_type
                reference_data.extend(entity_data)
                reference_entity_types.append(entity_type)

        if reference_data:
            sheet_name = f"Reference{date_suffix}"
            sheets[sheet_name] = self._create_sheet_data(
                sheet_name=sheet_name,
                data=reference_data,
                entity_types=reference_entity_types
            )

        log_event("data_filter", "info", f"Organized data into {len(sheets)} sheets: {list(sheets.keys())}")

        return sheets

    def _generate_date_suffix(self, date_filter: Optional[DateFilter]) -> str:
        """Generate date range suffix for sheet names"""
        if not date_filter or (not date_filter.date_from and not date_filter.date_to):
            return ""

        try:
            parts = []

            if date_filter.date_from:
                from_dt = datetime.fromisoformat(date_filter.date_from.replace('Z', '+00:00'))
                parts.append(from_dt.strftime("%Y%m%d"))

            if date_filter.date_to:
                to_dt = datetime.fromisoformat(date_filter.date_to.replace('Z', '+00:00'))
                if date_filter.date_from:
                    parts.append(to_dt.strftime("%Y%m%d"))
                else:
                    parts.append(f"to_{to_dt.strftime('%Y%m%d')}")

            if parts:
                if len(parts) == 2:
                    return f"_{parts[0]}-{parts[1]}"
                else:
                    return f"_{parts[0]}"

        except ValueError as e:
            log_event("data_filter", "warning", f"Could not generate date suffix: {e}")

        return ""

    def _create_sheet_data(self, sheet_name: str, data: List[Dict[str, Any]],
                          entity_types: List[str]) -> SheetData:
        """
        Create SheetData object from entity data

        Args:
            sheet_name: Name of the sheet
            data: Entity data records
            entity_types: List of entity types included in this sheet

        Returns:
            SheetData object
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

        # Sort headers for consistent ordering
        headers = sorted(list(all_headers))

        # Create rows
        rows = []
        for record in data:
            if not isinstance(record, dict):
                continue

            row = []
            for header in headers:
                value = record.get(header, '')

                # Handle complex data types
                if isinstance(value, (list, dict)):
                    try:
                        import json
                        row.append(json.dumps(value))
                    except (TypeError, ValueError):
                        row.append(str(value))
                elif value is None:
                    row.append("")
                else:
                    row.append(value)

            rows.append(row)

        return SheetData(
            sheet_name=sheet_name,
            headers=headers,
            rows=rows,
            entity_types=entity_types,
            record_count=len(rows)
        )

    def apply_comprehensive_filter(self, entities_data: Dict[str, List[Dict[str, Any]]],
                                  date_filter: Optional[DateFilter] = None) -> FilterResult:
        """
        Apply comprehensive filtering to all entity data

        Args:
            entities_data: Raw entity data
            date_filter: Optional date filter for deals

        Returns:
            FilterResult with filtered data and statistics
        """
        log_event("data_filter", "info", "Starting comprehensive data filtering")

        # Step 1: Filter entities by type (exclude events, etc.)
        type_filtered_data = self.filter_entities_by_type(entities_data)

        # Step 2: Apply date filter to deals if specified
        filtered_deals = []
        if EntityType.DEALS.value in type_filtered_data:
            deals_data = type_filtered_data[EntityType.DEALS.value]
            filtered_deals = self.apply_date_filter(deals_data, date_filter)
            type_filtered_data[EntityType.DEALS.value] = filtered_deals
        elif "leads" in type_filtered_data:
            deals_data = type_filtered_data["leads"]
            filtered_deals = self.apply_date_filter(deals_data, date_filter)
            type_filtered_data["leads"] = filtered_deals

        # Step 3: Filter contacts based on deal relationships
        if EntityType.CONTACTS.value in type_filtered_data and filtered_deals:
            all_contacts = type_filtered_data[EntityType.CONTACTS.value]
            filtered_contacts = self.filter_related_contacts(filtered_deals, all_contacts)
            type_filtered_data[EntityType.CONTACTS.value] = filtered_contacts

        # Step 4: Always include reference entities (companies, users, pipelines) without filtering
        # This is already handled in filter_entities_by_type

        # Generate statistics
        filter_stats = {}
        for entity_type, data in type_filtered_data.items():
            filter_stats[entity_type] = len(data) if data else 0

        log_event("data_filter", "info",
                 f"Comprehensive filtering complete. Results: {filter_stats}")

        return FilterResult(
            filtered_data=type_filtered_data,
            filter_stats=filter_stats,
            date_range_applied=date_filter
        )