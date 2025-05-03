"""
Authentication module for AmoCRM API using long-term token
"""

import json
import os
import requests
from datetime import datetime, timedelta

import config
from logger import log_event


class Auth:
    """Handle AmoCRM authentication with long-term token"""

    def __init__(self):
        """Initialize the authentication module"""
        self.token_data = None
        self._load_token()

    def _load_token(self):
        """Load the token from the token file or create from long-term token"""
        try:
            # Try to load from token file first
            if os.path.exists(config.settings.token_file):
                with open(config.settings.token_file, "r", encoding="utf-8") as f:
                    self.token_data = json.load(f)
                log_event("auth", "info", "Loaded token from file")
            else:
                # If no token file exists, create one from the long-term token
                self._create_token_from_longterm()
        except Exception as e:
            print(f"Error loading token: {e}")
            log_event("auth", "error", f"Error loading token: {e}")
            # Try to create from long-term token if loading failed
            self._create_token_from_longterm()

    def _create_token_from_longterm(self):
        """Create token data from the long-term token"""
        longterm_token = config.settings.longterm_token

        if longterm_token:
            # Create a token data structure
            self.token_data = {
                "access_token": longterm_token,
                "token_type": "Bearer",
                "expires_in": 157680000,  # 5 years in seconds
                "expires_at": (
                    datetime.now() + timedelta(days=5 * 365)
                ).isoformat(),
            }

            # Save the token data
            self._save_token()
            log_event(
                "auth", "info", "Created token data from long-term token"
            )
        else:
            log_event("auth", "warning", "No long-term token available")

    def _save_token(self):
        """Save the token to the token file"""
        try:
            # Create the data directory if it doesn't exist
            os.makedirs(os.path.dirname(config.settings.token_file), exist_ok=True)

            with open(config.settings.token_file, "w", encoding="utf-8") as f:
                json.dump(self.token_data, f, indent=2)

            log_event("auth", "info", "Token saved to file")
        except Exception as e:
            print(f"Error saving token: {e}")
            log_event("auth", "error", f"Error saving token: {e}")

    def get_token(self):
        """Get the current access token"""
        if not self.token_data or "access_token" not in self.token_data:
            self._create_token_from_longterm()

            if not self.token_data or "access_token" not in self.token_data:
                raise Exception(
                    "No access token available. Please set LONGTERM_TOKEN in the .env file."
                )

        token = self.token_data["access_token"]
        log_event("auth", "info", f"Token: {token}")
        return token

    def validate_token(self):
        """Validate that the token works by making a test API call"""
        try:
            headers = {"Authorization": f"Bearer {self.get_token()}"}

            # Make a test request to the account info endpoint
            response = requests.get(
                f"{config.settings.api_url}/account", headers=headers
            )
            response.raise_for_status()

            log_event("auth", "info", "Token validated successfully")
            return True
        except Exception as e:
            log_event("auth", "error", f"Token validation failed: {e}")
            return False
