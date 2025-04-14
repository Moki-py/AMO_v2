"""
Authentication module for AmoCRM API using OAuth2
"""
import json
import os
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import config
from logger import log_event

class Auth:
    """Handle AmoCRM authentication with OAuth2"""

    def __init__(self):
        """Initialize the authentication module"""
        self.token_data = None
        self._load_token()

    def _load_token(self):
        """Load the token from the token file or create using auth code"""
        try:
            # Try to load from token file first
            if os.path.exists(config.TOKEN_FILE):
                with open(config.TOKEN_FILE, 'r', encoding='utf-8') as f:
                    self.token_data = json.load(f)
                log_event('auth', 'info', 'Loaded token from file')

                # Check if token needs refresh
                if self._token_needs_refresh():
                    log_event('auth', 'info', 'Token needs refresh, refreshing...')
                    self._refresh_token()
            else:
                # If no token file exists, create one using authorization code
                self._exchange_auth_code_for_tokens()
        except Exception as e:
            print(f"Error loading token: {e}")
            log_event('auth', 'error', f'Error loading token: {e}')
            # Try to create from authorization code if loading failed
            self._exchange_auth_code_for_tokens()

    def _token_needs_refresh(self) -> bool:
        """Check if the current token needs refresh"""
        if not self.token_data or 'expires_at' not in self.token_data:
            return True

        try:
            # Parse the expiration timestamp
            expires_at = datetime.fromisoformat(self.token_data['expires_at'])
            current_time = datetime.now()

            # Add buffer time to refresh token before it actually expires
            buffer_time = timedelta(seconds=config.TOKEN_REFRESH_BUFFER)

            # Return True if token expires soon or has expired
            return current_time + buffer_time >= expires_at
        except (ValueError, TypeError):
            # If there's any issue parsing the timestamp, refresh to be safe
            return True

    def _exchange_auth_code_for_tokens(self):
        """Exchange authorization code for access and refresh tokens"""
        auth_code = config.AUTHORIZATION_CODE
        client_id = config.CLIENT_ID
        client_secret = config.CLIENT_SECRET
        redirect_uri = config.REDIRECT_URI

        if not (auth_code and client_id and client_secret and redirect_uri):
            log_event('auth', 'error', 'Missing OAuth2 credentials in config')
            raise Exception("Missing OAuth2 credentials. Please set CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, and AUTHORIZATION_CODE in the .env file.")

        try:
            payload = {
                'grant_type': 'authorization_code',  # Используем authorization_code вместо long_term
                'client_id': client_id,
                'client_secret': client_secret,
                'redirect_uri': redirect_uri,
                'code': auth_code  # Используем code вместо long_term_token
            }

            response = requests.post(config.AUTH_URL, json=payload)
            response.raise_for_status()

            # Get token data from response
            token_data = response.json()

            # Add expires_at field for easy checking
            if 'expires_in' in token_data:
                expires_at = datetime.now() + timedelta(seconds=token_data['expires_in'])
                token_data['expires_at'] = expires_at.isoformat()

            self.token_data = token_data

            # Save the token data
            self._save_token()
            log_event('auth', 'info', 'Successfully exchanged authorization code for access and refresh tokens')

        except Exception as e:
            log_event('auth', 'error', f'Error exchanging authorization code: {e}')
            raise Exception(f"Failed to exchange authorization code: {e}")

    def _refresh_token(self):
        """Refresh the access token using the refresh token"""
        if not self.token_data or 'refresh_token' not in self.token_data:
            log_event('auth', 'error', 'No refresh token available, trying to get new tokens with authorization code')
            return self._exchange_auth_code_for_tokens()

        client_id = config.CLIENT_ID
        client_secret = config.CLIENT_SECRET
        redirect_uri = config.REDIRECT_URI
        refresh_token = self.token_data['refresh_token']

        if not (client_id and client_secret and redirect_uri and refresh_token):
            log_event('auth', 'error', 'Missing OAuth2 credentials for token refresh')
            return self._exchange_auth_code_for_tokens()

        try:
            payload = {
                'grant_type': 'refresh_token',
                'client_id': client_id,
                'client_secret': client_secret,
                'redirect_uri': redirect_uri,
                'refresh_token': refresh_token
            }

            response = requests.post(config.AUTH_URL, json=payload)
            response.raise_for_status()

            # Get token data from response
            token_data = response.json()

            # Add expires_at field for easy checking
            if 'expires_in' in token_data:
                expires_at = datetime.now() + timedelta(seconds=token_data['expires_in'])
                token_data['expires_at'] = expires_at.isoformat()

            self.token_data = token_data

            # Save the token data
            self._save_token()
            log_event('auth', 'info', 'Successfully refreshed access token')

        except Exception as e:
            log_event('auth', 'error', f'Error refreshing token: {e}. Trying to get new tokens with authorization code.')
            # If refresh fails, try to get new tokens from authorization code
            self._exchange_auth_code_for_tokens()

    def _save_token(self):
        """Save the token to the token file"""
        try:
            # Create the data directory if it doesn't exist
            os.makedirs(os.path.dirname(config.TOKEN_FILE), exist_ok=True)

            with open(config.TOKEN_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.token_data, f, indent=2)

            log_event('auth', 'info', 'Token saved to file')
        except Exception as e:
            print(f"Error saving token: {e}")
            log_event('auth', 'error', f'Error saving token: {e}')

    def get_token(self):
        """Get the current access token, refreshing if necessary"""
        if not self.token_data or 'access_token' not in self.token_data:
            self._exchange_auth_code_for_tokens()

        if self._token_needs_refresh():
            self._refresh_token()

        if not self.token_data or 'access_token' not in self.token_data:
            raise Exception("No access token available. Please check your OAuth2 credentials in the .env file.")

        return self.token_data['access_token']

    def validate_token(self):
        """Validate that the token works by making a test API call"""
        try:
            # Get a fresh token if needed
            token = self.get_token()

            headers = {
                'Authorization': f'Bearer {token}'
            }

            # Make a test request to the account info endpoint
            response = requests.get(f"{config.API_URL}/account", headers=headers)
            response.raise_for_status()

            log_event('auth', 'info', 'Token validated successfully')
            return True
        except Exception as e:
            log_event('auth', 'error', f'Token validation failed: {e}')

            # If validation fails, try to refresh token and validate again
            try:
                self._refresh_token()
                token = self.get_token()

                headers = {
                    'Authorization': f'Bearer {token}'
                }

                response = requests.get(f"{config.API_URL}/account", headers=headers)
                response.raise_for_status()

                log_event('auth', 'info', 'Token refreshed and validated successfully')
                return True
            except Exception as refresh_error:
                log_event('auth', 'error', f'Token refresh and validation failed: {refresh_error}')
                return False