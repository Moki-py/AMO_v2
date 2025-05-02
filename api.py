"""
AmoCRM API interaction module
"""

import time
import requests
from typing import Any

import config
from auth import Auth
from logger import log_event


class AmoCRMAPI:
    """AmoCRM API client"""

    def __init__(self):
        """Initialize the API client"""
        self.auth = Auth()
        self.last_request_time = 0

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
            response = requests.request(
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

    def get_deals_page(self, page: int) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of deals"""
        return self._get_entity_page("leads", page)

    def get_contacts_page(
        self, page: int
    ) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of contacts"""
        return self._get_entity_page("contacts", page)

    def get_companies_page(
        self, page: int
    ) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of companies"""
        return self._get_entity_page("companies", page)

    def get_events_page(self, page: int) -> tuple[list[dict[str, Any]], bool]:
        """Get a specific page of events"""
        return self._get_entity_page("events", page)

    def _get_entity_page(
        self, entity_type: str, page: int
    ) -> tuple[list[dict[str, Any]], bool]:
        """
        Get a specific page of entities

        Returns:
            tuple containing the list of entities and a boolean indicating if there are more pages
        """
        params = {
            "page": page,
            "limit": config.settings.page_size,
        }

        # Add with parameter for specific entity types
        if entity_type in ["leads", "contacts", "companies"]:
            params["with"] = (
                "contacts,catalog_elements,leads,customers,segments"
            )

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
