"""
Web interface for AmoCRM exporter
"""
import os
import json
import threading
from datetime import datetime
import webbrowser
import http.server
import socketserver
import urllib.parse
from typing import Dict, Any

import config_longterm as config
from logger import log_event
from storage_improved import Storage
from parallel_exporter import ParallelExporter
from state_manager import StateManager

# Create instances
storage = Storage()
exporter = ParallelExporter()
state_manager = StateManager()

# HTML templates
HTML_HEADER = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AmoCRM Exporter</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1, h2, h3 {
            color: #2c3e50;
        }
        h1 {
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }
        .card {
            background-color: #fff;
            border-radius: 6px;
            box-shadow: 0 1px 5px rgba(0,0,0,0.08);
            padding: 15px;
            margin-bottom: 20px;
        }
        .dashboard {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-box {
            background-color: #f8f9fa;
            border-left: 4px solid #007bff;
            padding: 15px;
            border-radius: 4px;
        }
        .stat-box h3 {
            margin-top: 0;
            margin-bottom: 10px;
            font-size: 18px;
        }
        .stat-box .value {
            font-size: 24px;
            font-weight: bold;
        }
        .btn {
            display: inline-block;
            background-color: #007bff;
            color: white;
            padding: 8px 15px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            text-decoration: none;
            font-size: 14px;
            margin-right: 10px;
            margin-bottom: 10px;
        }
        .btn:hover {
            background-color: #0069d9;
        }
        .btn-danger {
            background-color: #dc3545;
        }
        .btn-danger:hover {
            background-color: #c82333;
        }
        .btn-warning {
            background-color: #ffc107;
            color: #212529;
        }
        .btn-warning:hover {
            background-color: #e0a800;
        }
        .btn-success {
            background-color: #28a745;
        }
        .btn-success:hover {
            background-color: #218838;
        }
        .status-running {
            color: #28a745;
        }
        .status-stopped {
            color: #dc3545;
        }
        .status-completed {
            color: #6c757d;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        table, th, td {
            border: 1px solid #dee2e6;
        }
        th, td {
            padding: 12px 15px;
            text-align: left;
        }
        th {
            background-color: #f8f9fa;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .section {
            margin-bottom: 30px;
        }
        .progress-container {
            width: 100%;
            background-color: #e9ecef;
            border-radius: 4px;
            margin-top: 10px;
        }
        .progress-bar {
            height: 20px;
            background-color: #007bff;
            border-radius: 4px;
            text-align: center;
            color: white;
            font-size: 12px;
            line-height: 20px;
        }
        .auto-refresh {
            margin-bottom: 20px;
        }
        .alert {
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 4px;
        }
        .alert-info {
            background-color: #d1ecf1;
            color: #0c5460;
            border: 1px solid #bee5eb;
        }
        .alert-warning {
            background-color: #fff3cd;
            color: #856404;
            border: 1px solid #ffeeba;
        }
        #logs {
            height: 300px;
            overflow-y: auto;
            font-family: monospace;
            background-color: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            border: 1px solid #dee2e6;
        }
        .log-entry {
            margin-bottom: 5px;
            word-wrap: break-word;
        }
        .log-info {
            color: #0c5460;
        }
        .log-warning {
            color: #856404;
        }
        .log-error {
            color: #721c24;
        }
    </style>
    <script>
        // Auto-refresh function
        function setupAutoRefresh() {
            const checkbox = document.getElementById('auto-refresh');
            if (checkbox.checked) {
                window.autoRefreshInterval = setInterval(() => {
                    location.reload();
                }, 10000); // Refresh every 10 seconds
            } else {
                if (window.autoRefreshInterval) {
                    clearInterval(window.autoRefreshInterval);
                }
            }
        }

        // Initialize on page load
        window.onload = function() {
            const checkbox = document.getElementById('auto-refresh');
            checkbox.addEventListener('change', setupAutoRefresh);
            if (checkbox.checked) {
                setupAutoRefresh();
            }

            // Scroll logs to bottom
            const logsContainer = document.getElementById('logs');
            if (logsContainer) {
                logsContainer.scrollTop = logsContainer.scrollHeight;
            }
        };
    </script>
</head>
<body>
    <div class="container">
        <h1>AmoCRM Data Exporter</h1>
        <div class="auto-refresh">
            <input type="checkbox" id="auto-refresh" checked>
            <label for="auto-refresh">Auto-refresh (every 10 seconds)</label>
        </div>
"""

HTML_FOOTER = """
    </div>
</body>
</html>
"""

class UIHandler(http.server.BaseHTTPRequestHandler):
    """HTTP request handler for the UI server"""

    def do_GET(self):
        """Handle GET request"""
        parsed_url = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # Handle API endpoints
        if parsed_url.path.startswith('/api/'):
            self._handle_api_request(parsed_url.path, query_params)
            return

        # Handle static files
        if parsed_url.path.startswith('/static/'):
            self._serve_static_file(parsed_url.path[8:])
            return

        # Handle main UI
        self._serve_ui()

    def _serve_ui(self):
        """Serve the main UI HTML"""
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        # Get stats
        stats = storage.get_statistics()
        export_status = exporter.get_export_status()

        # Start of HTML
        html = HTML_HEADER

        # Dashboard section
        html += """
        <div class="section">
            <h2>Dashboard</h2>
            <div class="dashboard">
        """

        # Stats boxes
        for entity, count in stats.items():
            if entity != 'logs':
                icon = "📄"
                if entity == "deals":
                    icon = "💼"
                elif entity == "contacts":
                    icon = "👤"
                elif entity == "companies":
                    icon = "🏢"
                elif entity == "events":
                    icon = "📅"

                html += f"""
                <div class="stat-box">
                    <h3>{icon} {entity.capitalize()}</h3>
                    <div class="value">{count:,}</div>
                </div>
                """

        html += """
            </div>
        </div>
        """

        # Export status section
        html += """
        <div class="section">
            <h2>Export Status</h2>
            <table>
                <tr>
                    <th>Entity Type</th>
                    <th>Status</th>
                    <th>Progress</th>
                    <th>Actions</th>
                </tr>
        """

        # Add rows for each entity type
        for entity_type, status in export_status.items():
            display_type = entity_type.capitalize()
            if entity_type == 'leads':
                display_type = 'Deals'

            # Determine status text and class
            status_text = "Not started"
            status_class = "status-stopped"

            if status['running']:
                status_text = "Running"
                status_class = "status-running"
            elif status['completed']:
                status_text = "Completed"
                status_class = "status-completed"

            # Create progress info
            progress_info = f"Page: {status['last_page']}"

            # Determine available actions
            actions = ""
            if status['running']:
                actions += f'<a href="/api/stop_export?type={entity_type}" class="btn btn-danger">Stop</a>'
            else:
                actions += f'<a href="/api/start_export?type={entity_type}" class="btn btn-success">Start</a>'
                actions += f'<a href="/api/start_export?type={entity_type}&force=true" class="btn btn-warning">Restart</a>'

            html += f"""
            <tr>
                <td>{display_type}</td>
                <td><span class="{status_class}">{status_text}</span></td>
                <td>{progress_info}</td>
                <td>{actions}</td>
            </tr>
            """

        html += """
            </table>

            <div class="card">
                <h3>Bulk Actions</h3>
                <a href="/api/start_all_exports" class="btn btn-success">Start All Exports</a>
                <a href="/api/start_all_exports?force=true" class="btn btn-warning">Restart All Exports</a>
                <a href="/api/stop_all_exports" class="btn btn-danger">Stop All Exports</a>
            </div>
        </div>
        """

        # Recent logs section
        html += """
        <div class="section">
            <h2>Recent Logs</h2>
            <div id="logs">
        """

        # Get recent logs (last 100 entries)
        log_file = os.path.join(config.DATA_DIR, 'log.json')
        logs = []
        if os.path.exists(log_file):
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
                    # Get the last 100 logs
                    logs = logs[-100:] if len(logs) > 100 else logs
            except Exception as e:
                logs = [{"timestamp": datetime.now().isoformat(), "level": "error", "message": f"Error loading logs: {e}"}]

        # Generate log entries
        for log in logs:
            timestamp = log.get('timestamp', '')
            level = log.get('level', 'info')
            component = log.get('component', '')
            message = log.get('message', '')

            log_class = f"log-{level}"

            html += f"""
            <div class="log-entry {log_class}">
                [{timestamp}] [{component}] [{level.upper()}] {message}
            </div>
            """

        html += """
            </div>
        </div>
        """

        # End of HTML
        html += HTML_FOOTER

        self.wfile.write(html.encode('utf-8'))

    def _handle_api_request(self, path: str, query_params: Dict[str, Any]):
        """Handle API requests"""
        # Extract the endpoint from the path
        endpoint = path.split('/api/')[1]

        if endpoint == 'start_export':
            entity_type = query_params.get('type', [''])[0]
            force = query_params.get('force', ['false'])[0].lower() == 'true'

            if entity_type == 'leads' or entity_type == 'deals':
                exporter.export_deals(force_restart=force)
            elif entity_type == 'contacts':
                exporter.export_contacts(force_restart=force)
            elif entity_type == 'companies':
                exporter.export_companies(force_restart=force)
            elif entity_type == 'events':
                exporter.export_events(force_restart=force)

            # Redirect back to the main UI
            self._redirect_to_ui()

        elif endpoint == 'stop_export':
            entity_type = query_params.get('type', [''])[0]
            exporter.stop_export(entity_type)

            # Redirect back to the main UI
            self._redirect_to_ui()

        elif endpoint == 'start_all_exports':
            force = query_params.get('force', ['false'])[0].lower() == 'true'
            exporter.export_all(force_restart=force)

            # Redirect back to the main UI
            self._redirect_to_ui()

        elif endpoint == 'stop_all_exports':
            exporter.stop_all_exports()

            # Redirect back to the main UI
            self._redirect_to_ui()

        else:
            # Unknown API endpoint
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"Unknown API endpoint: {endpoint}".encode('utf-8'))

    def _redirect_to_ui(self):
        """Redirect to the main UI page"""
        self.send_response(302)  # Found/Redirect
        self.send_header('Location', '/')
        self.end_headers()

    def _serve_static_file(self, file_path: str):
        """Serve a static file"""
        # This is a simple implementation; in production you'd want more security
        try:
            # Determine the file type
            if file_path.endswith('.css'):
                content_type = 'text/css'
            elif file_path.endswith('.js'):
                content_type = 'application/javascript'
            elif file_path.endswith('.png'):
                content_type = 'image/png'
            elif file_path.endswith('.jpg') or file_path.endswith('.jpeg'):
                content_type = 'image/jpeg'
            else:
                content_type = 'text/plain'

            # Serve the file
            with open(f"static/{file_path}", 'rb') as f:
                self.send_response(200)
                self.send_header('Content-type', content_type)
                self.end_headers()
                self.wfile.write(f.read())
        except FileNotFoundError:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"File not found: {file_path}".encode('utf-8'))
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"Error serving file: {e}".encode('utf-8'))

    def log_message(self, format, *args):
        """Override to prevent printing logs to console"""
        pass

class UIServer:
    """Web UI server for AmoCRM exporter"""

    def __init__(self, host='0.0.0.0', port=8000):
        """Initialize the UI server"""
        self.host = host
        self.port = port
        self.server = None
        self.thread = None

    def start(self):
        """Start the UI server"""
        try:
            # Create the server
            self.server = socketserver.TCPServer((self.host, self.port), UIHandler)

            # Log startup
            log_event('ui', 'info', f'Starting UI server on http://{self.host}:{self.port}')
            print(f"UI server started on http://{self.host}:{self.port}")