"""
Modern web interface for AmoCRM exporter
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
import subprocess

import config
from logger import log_event
from storage import Storage

# Create global storage instance
storage = Storage()

def get_recent_logs(count=100):
    """Get the most recent logs from the log file"""
    log_file = config.LOG_FILE
    logs = []

    if os.path.exists(log_file):
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                logs = json.load(f)
                # Get the last N logs
                logs = logs[-count:] if len(logs) > count else logs
        except Exception as e:
            logs = [{"timestamp": datetime.now().isoformat(),
                     "level": "error",
                     "message": f"Error loading logs: {e}"}]

    return logs

class ModernHandler(http.server.BaseHTTPRequestHandler):
    """Modern HTTP request handler"""

    def do_GET(self):
        """Handle GET request"""
        if self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            # Generate HTML
            html = self._generate_html()
            self.wfile.write(html.encode('utf-8'))
            return

        # Handle actions
        elif self.path.startswith('/action'):
            self._handle_action()
            return

        # Handle other paths
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b'Not Found')

    def _generate_html(self):
        """Generate the HTML content"""
        # Load statistics
        stats = self._get_stats()

        # Get recent logs
        logs = get_recent_logs(30)  # Get last 30 logs

        # Generate HTML with modern design
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>AmoCRM Data Exporter</title>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
            <style>
                :root {{
                    --primary: #4361ee;
                    --primary-hover: #3a56d4;
                    --primary-light: #eef2ff;
                    --success: #10b981;
                    --success-hover: #059669;
                    --warning: #f59e0b;
                    --warning-hover: #d97706;
                    --danger: #ef4444;
                    --danger-hover: #dc2626;
                    --dark: #111827;
                    --gray: #6b7280;
                    --light-gray: #f3f4f6;
                    --white: #ffffff;
                    --shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                    --border-radius: 8px;
                    --card-border-radius: 12px;
                    --animation-speed: 0.3s;
                }}

                * {{
                    box-sizing: border-box;
                    margin: 0;
                    padding: 0;
                }}

                body {{
                    font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, Oxygen, Ubuntu, sans-serif;
                    line-height: 1.6;
                    background-color: #f9fafb;
                    color: #374151;
                    padding: 0;
                    margin: 0;
                }}

                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                }}

                header {{
                    background-color: var(--white);
                    box-shadow: var(--shadow);
                    padding: 16px 0;
                    position: sticky;
                    top: 0;
                    z-index: 100;
                    margin-bottom: 30px;
                }}

                header .header-content {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 0 20px;
                }}

                h1, h2, h3 {{
                    color: var(--dark);
                    font-weight: 600;
                }}

                h1 {{
                    font-size: 1.6rem;
                    margin: 0;
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }}

                h1 i {{
                    color: var(--primary);
                }}

                h2 {{
                    font-size: 1.4rem;
                    margin-bottom: 16px;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }}

                h2 i {{
                    color: var(--primary);
                }}

                h3 {{
                    font-size: 1.2rem;
                    margin-bottom: 12px;
                    color: var(--gray);
                }}

                .auto-refresh {{
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }}

                .toggle {{
                    position: relative;
                    display: inline-block;
                    width: 48px;
                    height: 24px;
                }}

                .toggle input {{
                    opacity: 0;
                    width: 0;
                    height: 0;
                }}

                .slider {{
                    position: absolute;
                    cursor: pointer;
                    top: 0;
                    left: 0;
                    right: 0;
                    bottom: 0;
                    background-color: #e5e7eb;
                    transition: .4s;
                    border-radius: 24px;
                }}

                .slider:before {{
                    position: absolute;
                    content: "";
                    height: 18px;
                    width: 18px;
                    left: 3px;
                    bottom: 3px;
                    background-color: white;
                    transition: .4s;
                    border-radius: 50%;
                }}

                input:checked + .slider {{
                    background-color: var(--primary);
                }}

                input:focus + .slider {{
                    box-shadow: 0 0 1px var(--primary);
                }}

                input:checked + .slider:before {{
                    transform: translateX(24px);
                }}

                .dashboard {{
                    display: grid;
                    grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}

                .card {{
                    background-color: var(--white);
                    border-radius: var(--card-border-radius);
                    box-shadow: var(--shadow);
                    overflow: hidden;
                    transition: transform var(--animation-speed), box-shadow var(--animation-speed);
                }}

                .card:hover {{
                    transform: translateY(-3px);
                    box-shadow: 0 10px 20px rgba(0, 0, 0, 0.1);
                }}

                .card-header {{
                    padding: 16px 20px;
                    border-bottom: 1px solid var(--light-gray);
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                }}

                .card-body {{
                    padding: 20px;
                }}

                .stat-card {{
                    text-align: center;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    padding: 25px 20px;
                }}

                .stat-card .icon {{
                    width: 64px;
                    height: 64px;
                    border-radius: 16px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    margin-bottom: 16px;
                    font-size: 1.8rem;
                    color: var(--white);
                }}

                .stat-card h3 {{
                    font-size: 1.1rem;
                    color: var(--gray);
                    margin-bottom: 8px;
                }}

                .stat-card .value {{
                    font-size: 2.5rem;
                    font-weight: 700;
                    color: var(--dark);
                }}

                .btn {{
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    gap: 8px;
                    padding: 9px 16px;
                    background-color: var(--primary);
                    color: var(--white);
                    border: none;
                    border-radius: var(--border-radius);
                    font-weight: 500;
                    text-decoration: none;
                    cursor: pointer;
                    transition: all var(--animation-speed);
                    font-size: 0.9rem;
                    min-width: 110px;
                }}

                .btn:hover {{
                    background-color: var(--primary-hover);
                    transform: translateY(-2px);
                    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                }}

                .btn-success {{
                    background-color: var(--success);
                }}

                .btn-success:hover {{
                    background-color: var(--success-hover);
                }}

                .btn-warning {{
                    background-color: var(--warning);
                }}

                .btn-warning:hover {{
                    background-color: var(--warning-hover);
                }}

                .btn-danger {{
                    background-color: var(--danger);
                }}

                .btn-danger:hover {{
                    background-color: var(--danger-hover);
                }}

                .btn-group {{
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                }}

                .logs-container {{
                    margin-top: 30px;
                }}

                .logs-content {{
                    background-color: #1e293b;
                    border-radius: var(--border-radius);
                    padding: 15px;
                    font-family: 'Consolas', 'Monaco', monospace;
                    font-size: 0.9rem;
                    line-height: 1.5;
                    color: #e2e8f0;
                    height: 300px;
                    overflow-y: auto;
                }}

                .log-entry {{
                    margin-bottom: 6px;
                    word-wrap: break-word;
                }}

                .log-info {{
                    color: #60a5fa;
                }}

                .log-warning {{
                    color: #fbbf24;
                }}

                .log-error {{
                    color: #f87171;
                }}

                .section {{
                    margin-bottom: 30px;
                }}

                .footer {{
                    text-align: center;
                    padding: 20px;
                    margin-top: 40px;
                    color: var(--gray);
                    font-size: 0.9rem;
                    border-top: 1px solid var(--light-gray);
                }}

                /* Animations */
                @keyframes pulse {{
                    0% {{ transform: scale(1); }}
                    50% {{ transform: scale(1.05); }}
                    100% {{ transform: scale(1); }}
                }}

                .pulse {{
                    animation: pulse 2s infinite;
                }}

                /* Responsiveness */
                @media (max-width: 768px) {{
                    .dashboard {{
                        grid-template-columns: 1fr;
                    }}

                    .btn-group {{
                        justify-content: center;
                    }}
                }}
            </style>
            <script>
                // Auto-refresh function
                function setupAutoRefresh() {{
                    const checkbox = document.getElementById('auto-refresh');
                    if (checkbox.checked) {{
                        window.autoRefreshInterval = setInterval(() => {{
                            location.reload();
                        }}, 10000); // Refresh every 10 seconds
                    }} else {{
                        if (window.autoRefreshInterval) {{
                            clearInterval(window.autoRefreshInterval);
                        }}
                    }}
                }}

                // Initialize on page load
                window.onload = function() {{
                    const checkbox = document.getElementById('auto-refresh');
                    checkbox.addEventListener('change', setupAutoRefresh);
                    if (checkbox.checked) {{
                        setupAutoRefresh();
                    }}

                    // Scroll logs to bottom
                    const logsContainer = document.getElementById('logs-content');
                    if (logsContainer) {{
                        logsContainer.scrollTop = logsContainer.scrollHeight;
                    }}

                    // Update last refresh time
                    document.getElementById('last-refresh-time').textContent = new Date().toLocaleTimeString();
                }};
            </script>
        </head>
        <body>
            <header>
                <div class="header-content">
                    <h1><i class="fas fa-sync-alt"></i> AmoCRM Data Exporter</h1>
                    <div class="auto-refresh">
                        <label class="toggle">
                            <input type="checkbox" id="auto-refresh" checked>
                            <span class="slider"></span>
                        </label>
                        <div>
                            <div style="font-weight: 500;">Auto-refresh</div>
                            <div style="font-size: 0.8rem; color: var(--gray);">Every 10 seconds</div>
                        </div>
                    </div>
                </div>
            </header>

            <div class="container">
                <section class="section">
                    <h2><i class="fas fa-chart-pie"></i> Dashboard</h2>
                    <div class="dashboard">
        """

        # Add stat cards
        entity_colors = {
            "deals": "#4361ee",     # Primary blue
            "contacts": "#10b981",  # Success green
            "companies": "#f59e0b", # Warning yellow
            "events": "#8b5cf6"     # Purple
        }

        entity_icons = {
            "deals": "fa-briefcase",
            "contacts": "fa-user",
            "companies": "fa-building",
            "events": "fa-history"
        }

        for entity, count in stats.items():
            if entity != 'logs':
                icon = entity_icons.get(entity, "fa-file")
                color = entity_colors.get(entity, "#4361ee")

                html += f"""
                        <div class="card stat-card">
                            <div class="icon" style="background-color: {color};">
                                <i class="fas {icon}"></i>
                            </div>
                            <h3>{entity.capitalize()}</h3>
                            <div class="value">{count:,}</div>
                        </div>
                """

        html += """
                    </div>
                </section>

                <section class="section">
                    <h2><i class="fas fa-play-circle"></i> Export Controls</h2>
                    <div class="card">
                        <div class="card-header">
                            <h3>Available Actions</h3>
                        </div>
                        <div class="card-body">
                            <div class="btn-group">
                                <a href="/action?cmd=export_all" class="btn btn-success">
                                    <i class="fas fa-sync-alt"></i> Export All
                                </a>
                                <a href="/action?cmd=export_deals" class="btn">
                                    <i class="fas fa-briefcase"></i> Export Deals
                                </a>
                                <a href="/action?cmd=export_contacts" class="btn">
                                    <i class="fas fa-user"></i> Export Contacts
                                </a>
                                <a href="/action?cmd=export_companies" class="btn">
                                    <i class="fas fa-building"></i> Export Companies
                                </a>
                                <a href="/action?cmd=export_events" class="btn">
                                    <i class="fas fa-history"></i> Export Events
                                </a>
                                <a href="/action?cmd=stop_all" class="btn btn-danger">
                                    <i class="fas fa-stop"></i> Stop All
                                </a>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="logs-container">
                    <h2><i class="fas fa-terminal"></i> System Logs</h2>
                    <div class="card">
                        <div class="card-header">
                            <h3>Recent Activity</h3>
                        </div>
                        <div class="logs-content" id="logs-content">
        """

        # Add logs
        for log in reversed(logs):  # Show newest logs first
            timestamp = log.get('timestamp', '')
            level = log.get('level', 'info')
            component = log.get('component', '')
            message = log.get('message', '')

            # Format timestamp to be more readable
            try:
                dt = datetime.fromisoformat(timestamp)
                timestamp = dt.strftime('%H:%M:%S')
            except (ValueError, TypeError):
                pass

            log_class = f'log-{level}'

            # Add icon for log level
            if level == 'info':
                icon = "fa-info-circle"
            elif level == 'warning':
                icon = "fa-exclamation-triangle"
            elif level == 'error':
                icon = "fa-times-circle"
            else:
                icon = "fa-circle"

            html += f'<div class="log-entry {log_class}"><i class="fas {icon}"></i> [{timestamp}] [{component}] {message}</div>\n'

        html += """
                        </div>
                    </div>
                </section>

                <div class="footer">
                    <p>Last refresh: <span id="last-refresh-time"></span> | AmoCRM Data Exporter</p>
                </div>
            </div>
        </body>
        </html>
        """

        return html

    def _handle_action(self):
        """Handle action requests"""
        from urllib.parse import urlparse, parse_qs

        parsed_url = urlparse(self.path)
        params = parse_qs(parsed_url.query)

        command = params.get('cmd', [''])[0]

        if command == 'export_all':
            subprocess.Popen(['python', 'main.py', '--fetch-all'])
            log_event('server', 'info', 'Started export of all entities')
        elif command == 'export_deals':
            subprocess.Popen(['python', 'main.py', '--fetch-deals'])
            log_event('server', 'info', 'Started export of deals')
        elif command == 'export_contacts':
            subprocess.Popen(['python', 'main.py', '--fetch-contacts'])
            log_event('server', 'info', 'Started export of contacts')
        elif command == 'export_companies':
            subprocess.Popen(['python', 'main.py', '--fetch-companies'])
            log_event('server', 'info', 'Started export of companies')
        elif command == 'export_events':
            subprocess.Popen(['python', 'main.py', '--fetch-events'])
            log_event('server', 'info', 'Started export of events')
        elif command == 'stop_all':
            # Find and kill all Python processes with 'main.py' in the command line
            try:
                import psutil
                import os
                import signal

                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        cmdline = proc.info['cmdline']
                        if cmdline and 'python' in cmdline[0] and 'main.py' in ' '.join(cmdline):
                            os.kill(proc.info['pid'], signal.SIGTERM)
                            log_event('server', 'info', f'Stopped process {proc.info["pid"]}')
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass

                log_event('server', 'info', 'Stopped all export processes')
            except ImportError:
                log_event('server', 'error', 'Failed to import psutil for stopping processes')
                # Fallback method
                if os.name == 'nt':  # Windows
                    os.system('taskkill /f /im python.exe')
                else:  # Unix/Linux/Mac
                    os.system("pkill -f 'python.*main.py'")

        # Redirect back to the main page
        self.send_response(302)
        self.send_header('Location', '/')
        self.end_headers()

    def _get_stats(self):
        """Get data statistics"""
        try:
            return storage.get_statistics()
        except:
            # Fallback if storage.get_statistics() is not available
            stats = {}

            # Check deals
            deals_file = os.path.join(config.DATA_DIR, 'deals.json')
            if os.path.exists(deals_file):
                try:
                    with open(deals_file, 'r', encoding='utf-8') as f:
                        deals = json.load(f)
                        stats['deals'] = len(deals)
                except:
                    stats['deals'] = 0
            else:
                stats['deals'] = 0

            # Check contacts
            contacts_file = os.path.join(config.DATA_DIR, 'contacts.json')
            if os.path.exists(contacts_file):
                try:
                    with open(contacts_file, 'r', encoding='utf-8') as f:
                        contacts = json.load(f)
                        stats['contacts'] = len(contacts)
                except:
                    stats['contacts'] = 0
            else:
                stats['contacts'] = 0

            # Check companies
            companies_file = os.path.join(config.DATA_DIR, 'companies.json')
            if os.path.exists(companies_file):
                try:
                    with open(companies_file, 'r', encoding='utf-8') as f:
                        companies = json.load(f)
                        stats['companies'] = len(companies)
                except:
                    stats['companies'] = 0
            else:
                stats['companies'] = 0

            # Check events
            events_file = os.path.join(config.DATA_DIR, 'events.json')
            if os.path.exists(events_file):
                try:
                    with open(events_file, 'r', encoding='utf-8') as f:
                        events = json.load(f)
                        stats['events'] = len(events)
                except:
                    stats['events'] = 0
            else:
                stats['events'] = 0

            return stats

    def log_message(self, format, *args):
        """Override to prevent printing logs to console"""
        pass

def run_server(host='0.0.0.0', port=8000):
    """Run the modern UI server"""
    try:
        # Try to find an available port
        server = None

        for test_port in range(port, port + 10):
            try:
                server = socketserver.TCPServer((host, test_port), ModernHandler)
                port = test_port
                break
            except OSError:
                continue

        if server is None:
            log_event('server', 'error', 'Could not find an available port')
            print("Error: Could not find an available port")
            return False

        server_url = f"http://localhost:{port}" if host in ['0.0.0.0', '127.0.0.1'] else f"http://{host}:{port}"
        log_event('server', 'info', f'Starting modern UI server on {server_url}')
        print(f"Modern UI server started on {server_url}")

        # Open browser
        if host in ['0.0.0.0', 'localhost', '127.0.0.1']:
            threading.Timer(1.0, lambda: webbrowser.open(f"http://localhost:{port}")).start()

        # Run server until interrupted
        server.serve_forever()

    except KeyboardInterrupt:
        if server:
            server.server_close()
        log_event('server', 'info', 'Server stopped')
        print("Server stopped")
        return True
    except Exception as e:
        log_event('server', 'error', f'Error running server: {e}')
        print(f"Error running server: {e}")
        return False

if __name__ == "__main__":
    # Make sure data directory exists
    os.makedirs(config.DATA_DIR, exist_ok=True)

    # Initialize storage for logs
    from logger import init_storage, flush_log_buffer
    init_storage(storage)
    flush_log_buffer()

    # Run the server
    run_server()