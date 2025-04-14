"""
Simple server for running the API and web interface
"""
import os
import threading
from typing import Dict, Any
import webbrowser

from ui_server import run_ui_server
from logger import log_event

def run_server(host='0.0.0.0', port=8000):
    """Run the server"""
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)

    # Log server start
    log_event('server', 'info', f'Starting server on {host}:{port}')

    # Run UI server
    try:
        run_ui_server(host, port)
    except Exception as e:
        log_event('server', 'error', f'Error running server: {e}')
        print(f"Error running server: {e}")

if __name__ == "__main__":
    run_server()