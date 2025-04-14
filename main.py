"""
Main entry point for AmoCRM exporter
"""
import os
import time
import argparse
import threading
from datetime import datetime

import config
from auth import Auth
from api import AmoCRMAPI
from storage import Storage
from logger import log_event, init_storage, flush_log_buffer
from parallel_exporter import ParallelExporter
from state_manager import StateManager

def main():
    """Main entry point"""
    # Create data directory if it doesn't exist
    os.makedirs(config.DATA_DIR, exist_ok=True)

    # Initialize storage and logger
    storage = Storage()
    init_storage(storage)
    flush_log_buffer()

    # Create state manager and exporter
    state_manager = StateManager()
    exporter = ParallelExporter()

    # Validate the token
    auth = Auth()
    if not auth.validate_token():
        log_event('main', 'error', 'Token validation failed. Please check your LONGTERM_TOKEN in .env')
        print("Error: Token validation failed. Please check your LONGTERM_TOKEN in .env")
        return

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='AmoCRM data exporter')
    parser.add_argument('--fetch-all', action='store_true', help='Fetch all data')
    parser.add_argument('--fetch-deals', action='store_true', help='Fetch only deals')
    parser.add_argument('--fetch-contacts', action='store_true', help='Fetch only contacts')
    parser.add_argument('--fetch-companies', action='store_true', help='Fetch only companies')
    parser.add_argument('--fetch-events', action='store_true', help='Fetch only events')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of pages to fetch before saving (default: 10)')
    parser.add_argument('--force-restart', action='store_true', help='Restart exports from beginning')
    parser.add_argument('--server', action='store_true', help='Start the web server')
    parser.add_argument('--port', type=int, default=8000, help='Port for the web server (default: 8000)')
    args = parser.parse_args()

    # Execute requested actions
    if args.fetch_all:
        exporter.export_all(force_restart=args.force_restart, batch_size=args.batch_size)
    else:
        if args.fetch_deals:
            exporter.export_deals(force_restart=args.force_restart, batch_size=args.batch_size)
        if args.fetch_contacts:
            exporter.export_contacts(force_restart=args.force_restart, batch_size=args.batch_size)
        if args.fetch_companies:
            exporter.export_companies(force_restart=args.force_restart, batch_size=args.batch_size)
        if args.fetch_events:
            exporter.export_events(force_restart=args.force_restart, batch_size=args.batch_size)

    # Start web server if requested
    server_thread = None
    if args.server:
        from simple_server import run_server
        server_thread = threading.Thread(target=run_server, args=('0.0.0.0', args.port))
        server_thread.daemon = True
        server_thread.start()

    # If nothing was requested, show help
    if not any([args.fetch_all, args.fetch_deals, args.fetch_contacts,
                args.fetch_companies, args.fetch_events, args.server]):
        parser.print_help()
        return

    # Keep the program running if exports are active or server is running
    try:
        while True:
            # Check if any exports are running
            running_exports = exporter.get_running_exports()
            if not running_exports and not args.server:
                log_event('main', 'info', 'All exports completed')
                break

            # Sleep for a bit to prevent high CPU usage
            time.sleep(1)
    except KeyboardInterrupt:
        log_event('main', 'info', 'Stopping all exports...')
        exporter.stop_all_exports()

    log_event('main', 'info', 'Program finished')

if __name__ == "__main__":
    main()