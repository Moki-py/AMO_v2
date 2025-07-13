"""
AmoCRM API interaction module
"""

import time
import requests
from typing import Any
from datetime import datetime

from . import config
from .auth import Auth
from .logger import log_event


class AmoCRMAPI:
    """AmoCRM API client"""

    def __init__(self):
        """Initialize the API client"""
        self.auth = Auth()
        self.last_request_time = 0
        self.session = requests.Session()

    async def close_session(self):
        """Close the requests session"""
        if hasattr(self, 'session'):
            self.session.close()

    def _rate_limit(self):
        """Implement rate limiting for API requests"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        # If we need to wait to respect rate limit
        if elapsed < 1.0 / config.settings.max_requests_per_second:
            sleep_time = (1.0 / config.settings.max_requests_per_second) - elapsed
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _get_headers(self) -> dict[str, str]:
        """Get the headers for API requests"""
        return {
            "Authorization": f"Bearer {self.auth.get_token()}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self, method: str, endpoint: str, params=None, data=None
    ) -> dict[str, Any]:
        """Make an API request with rate limiting and authorization"""
        self._rate_limit()

        url = f"{config.settings.api_url}/{endpoint}"
        headers = self._get_headers()

        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data,
            )

            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_msg = f"API request error: {e}"
            log_event("api", "error", error_msg)
            raise
        except Exception as e:
            error_msg = f"API request error: {e}"
            log_event("api", "error", error_msg)
            raise

    def get_deals_page(self, page: int, date_from: str | None = None, date_to: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of deals, optionally filtered by updated_at"""
        return self._get_entity_page("leads", page, date_from, date_to)

    def get_contacts_page(self, page: int, date_from: str | None = None, date_to: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of contacts, optionally filtered by updated_at"""
        return self._get_entity_page("contacts", page, date_from, date_to)

    def get_companies_page(self, page: int, date_from: str | None = None, date_to: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of companies, optionally filtered by updated_at"""
        return self._get_entity_page("companies", page, date_from, date_to)

    def get_events_page(self, page: int, date_from: str | None = None, date_to: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of events, optionally filtered by updated_at"""
        return self._get_entity_page("events", page, date_from, date_to)

    def _get_entity_page(
        self, entity_type: str, page: int, date_from: str | None = None, date_to: str | None = None
    ) -> tuple[list[dict[str, Any]], bool]:
        """
        Get a specific page of entities, optionally filtered by updated_at

        Returns:
            tuple containing the list of entities and a boolean indicating if there are more pages
        """
        params: dict[str, Any] = {
            "page": page,
            "limit": config.settings.page_size,
        }

        # Add with parameter for specific entity types
        if entity_type in ["leads", "contacts", "companies"]:
            params["with"] = (
                "catalog_elements,leads,customers"
            )

        # Add updated_at filter if provided
        if date_from:
            # Convert to unix timestamp if needed
            try:
                from_ts = int(
                    datetime.fromisoformat(date_from).timestamp()
                )
            except Exception:
                from_ts = int(date_from) if isinstance(date_from, str) and date_from.isdigit() else date_from
            params["updated_at[from]"] = from_ts
        if date_to:
            try:
                to_ts = int(
                    datetime.fromisoformat(date_to).timestamp()
                )
            except Exception:
                to_ts = int(date_to) if isinstance(date_to, str) and date_to.isdigit() else date_to
            params["updated_at[to]"] = to_ts

        try:
            response = self._make_request("GET", entity_type, params=params)

            # Extract entities from response
            if (
                "_embedded" in response
                and entity_type in response["_embedded"]
            ):
                entities = response["_embedded"][entity_type]

                # Log fetch success
                log_event(
                    "api",
                    "info",
                    f"Fetched {len(entities)} {entity_type} from page {page}",
                )

                # Determine if there are more pages
                has_more = len(entities) == config.settings.page_size

                return entities, has_more
            else:
                log_event(
                    "api",
                    "warning",
                    f"No {entity_type} found in response for page {page}",
                )
                return [], False

        except Exception as e:
            log_event(
                "api",
                "error",
                f"Error fetching {entity_type} page {page}: {e}",
            )
            raise

    def get_all_deals(self) -> list[dict[str, Any]]:
        """Get all deals with pagination handling"""
        return self._get_all_entities("leads")

    def get_all_contacts(self) -> list[dict[str, Any]]:
        """Get all contacts with pagination handling"""
        return self._get_all_entities("contacts")

    def get_all_companies(self) -> list[dict[str, Any]]:
        """Get all companies with pagination handling"""
        return self._get_all_entities("companies")

    def get_all_events(self) -> list[dict[str, Any]]:
        """Get all events with pagination handling"""
        return self._get_all_entities("events")

    def _get_all_entities(self, entity_type: str) -> list[dict[str, Any]]:
        """
        Get all entities of a specific type with pagination handling
        """
        log_event("api", "info", f"Starting to fetch all {entity_type}")

        all_entities = []
        page = 1
        has_more = True

        while has_more:
            entities, has_more = self._get_entity_page(entity_type, page)
            all_entities.extend(entities)
            page += 1

        log_event(
            "api",
            "info",
            f"Completed fetching all {entity_type}. Total: {len(all_entities)}",
        )
        return all_entities

    def get_entity_by_id(
        self, entity_type: str, entity_id: int
    ) -> dict[str, Any] | None:
        """Get a specific entity by ID"""
        try:
            response = self._make_request("GET", f"{entity_type}/{entity_id}")
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_users_page(self, page: int, date_from: str | None = None, date_to: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of users, optionally filtered by updated_at"""
        params: dict[str, Any] = {
            "page": page,
            "limit": config.settings.page_size,
        }

        # Add updated_at filter if provided
        if date_from:
            # Convert to unix timestamp if needed
            try:
                from_ts = int(
                    datetime.fromisoformat(date_from).timestamp()
                )
            except Exception:
                from_ts = date_from
            params["updated_at[from]"] = from_ts
        if date_to:
            try:
                to_ts = int(
                    datetime.fromisoformat(date_to).timestamp()
                )
            except Exception:
                to_ts = date_to
            params["updated_at[to]"] = to_ts

        try:
            response = self._make_request("GET", "users", params=params)

            # Extract users from response
            if (
                "_embedded" in response
                and "users" in response["_embedded"]
            ):
                users = response["_embedded"]["users"]

                # Log fetch success
                log_event(
                    "api",
                    "info",
                    f"Fetched {len(users)} users from page {page}",
                )

                # Determine if there are more pages
                has_more = len(users) == config.settings.page_size

                return users, has_more
            else:
                log_event(
                    "api",
                    "warning",
                    f"No users found in response for page {page}",
                )
                return [], False

        except Exception as e:
            log_event(
                "api",
                "error",
                f"Error fetching users page {page}: {e}",
            )
            raise

    def get_all_users(self) -> list[dict[str, Any]]:
        """Get all users with pagination handling"""
        log_event("api", "info", "Starting to fetch all users")

        all_users = []
        page = 1
        has_more = True

        while has_more:
            users, has_more = self.get_users_page(page)
            all_users.extend(users)
            page += 1

        log_event("api", "info", f"Fetched {len(all_users)} total users")
        return all_users

    def get_pipelines_page(self, page: int, date_from: str | None = None, date_to: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of pipelines, optionally filtered by updated_at"""
        params: dict[str, Any] = {
            "page": page,
            "limit": config.settings.page_size,
        }

        # Add updated_at filter if provided
        if date_from:
            # Convert to unix timestamp if needed
            try:
                from_ts = int(
                    datetime.fromisoformat(date_from).timestamp()
                )
            except Exception:
                from_ts = date_from
            params["updated_at[from]"] = from_ts
        if date_to:
            try:
                to_ts = int(
                    datetime.fromisoformat(date_to).timestamp()
                )
            except Exception:
                to_ts = date_to
            params["updated_at[to]"] = to_ts

        try:
            response = self._make_request("GET", "leads/pipelines", params=params)

            # Extract pipelines from response
            if (
                "_embedded" in response
                and "pipelines" in response["_embedded"]
            ):
                pipelines = response["_embedded"]["pipelines"]

                # Log fetch success
                log_event(
                    "api",
                    "info",
                    f"Fetched {len(pipelines)} pipelines from page {page}",
                )

                # Determine if there are more pages
                has_more = len(pipelines) == config.settings.page_size

                return pipelines, has_more
            else:
                log_event(
                    "api",
                    "warning",
                    f"No pipelines found in response for page {page}",
                )
                return [], False

        except Exception as e:
            log_event(
                "api",
                "error",
                f"Error fetching pipelines page {page}: {e}",
            )
            raise

    def get_all_pipelines(self) -> list[dict[str, Any]]:
        """Get all pipelines with detailed status information"""
        log_event("api", "info", "Starting to fetch all pipelines")

        all_pipelines = []
        page = 1
        has_more = True

        while has_more:
            pipelines, has_more = self.get_pipelines_page(page)
            all_pipelines.extend(pipelines)
            page += 1

        log_event(
            "api", "info", f"Fetched {len(all_pipelines)} pipelines total"
        )
        return all_pipelines

    def get_custom_fields(self, entity_type: str) -> list[dict[str, Any]]:
        """Get custom fields for a specific entity type"""
        try:
            endpoint = f"{entity_type}/custom_fields"
            response = self._make_request("GET", endpoint)

            if "_embedded" in response and "custom_fields" in response["_embedded"]:
                custom_fields = response["_embedded"]["custom_fields"]
                log_event("api", "info", f"Fetched {len(custom_fields)} custom fields for {entity_type}")
                return custom_fields
            else:
                log_event("api", "warning", f"No custom fields found for {entity_type}")
                return []

        except Exception as e:
            log_event("api", "error", f"Error fetching custom fields for {entity_type}: {e}")
            return []

    def get_all_custom_fields(self) -> dict[str, list[dict[str, Any]]]:
        """Get custom fields for all supported entity types"""
        entity_types = ["leads", "contacts", "companies"]
        all_custom_fields = {}

        for entity_type in entity_types:
            try:
                custom_fields = self.get_custom_fields(entity_type)
                all_custom_fields[entity_type] = custom_fields
            except Exception as e:
                log_event("api", "error", f"Error fetching custom fields for {entity_type}: {e}")
                all_custom_fields[entity_type] = []

        total_fields = sum(len(fields) for fields in all_custom_fields.values())
        log_event("api", "info", f"Fetched {total_fields} custom fields total across all entity types")

        return all_custom_fields

    def get_custom_fields_page(self, page: int, date_from: str | None = None, date_to: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """Get custom fields page - custom fields don't support pagination so we return all on first page"""
        try:
            if page > 1:
                # Custom fields don't support pagination, return empty for pages > 1
                return [], False

            # Get all custom fields and format them as flat list
            all_custom_fields = self.get_all_custom_fields()
            flat_custom_fields = []

            for entity_type, fields in all_custom_fields.items():
                for field in fields:
                    # Add entity_type to each field for context
                    field_with_type = field.copy()
                    field_with_type["entity_type"] = entity_type
                    field_with_type["fetched_at"] = datetime.now().isoformat()
                    flat_custom_fields.append(field_with_type)

            log_event("api", "info", f"Fetched {len(flat_custom_fields)} custom fields on page {page}")
            return flat_custom_fields, False  # No more pages after first

        except Exception as e:
            log_event("api", "error", f"Error fetching custom fields page {page}: {e}")
            return [], False
