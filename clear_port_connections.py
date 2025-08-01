#!/usr/bin/env python3
"""
Utility script to help clear port connections and diagnose socket issues
"""

import subprocess
import socket
import time
import sys
import os
from typing import List, Dict


def get_port_connections(port: int) -> List[Dict]:
    """Get all connections on a specific port"""
    connections = []
    try:
        # Windows netstat command
        result = subprocess.run(
            ["netstat", "-ano"], 
            capture_output=True, 
            text=True, 
            timeout=10
        )
        
        lines = result.stdout.split('\n')
        for line in lines:
            if f":{port}" in line and "TCP" in line:
                parts = line.split()
                if len(parts) >= 4:
                    connections.append({
                        "protocol": parts[0],
                        "local_address": parts[1],
                        "foreign_address": parts[2],
                        "state": parts[3],
                        "pid": parts[4] if len(parts) > 4 else "N/A"
                    })
    except Exception as e:
        print(f"Error getting port connections: {e}")
    
    return connections


def check_port_availability(host: str, port: int) -> bool:
    """Check if a port is available for binding"""
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        test_socket.bind((host, port))
        test_socket.close()
        return True
    except OSError:
        return False


def kill_processes_on_port(port: int) -> int:
    """Kill processes using a specific port (Windows)"""
    killed_count = 0
    connections = get_port_connections(port)
    
    pids_to_kill = set()
    for conn in connections:
        if conn["state"] in ["LISTENING", "ESTABLISHED"] and conn["pid"] != "N/A":
            try:
                pid = int(conn["pid"])
                pids_to_kill.add(pid)
            except ValueError:
                continue
    
    for pid in pids_to_kill:
        try:
            # Check if it's a Python process first
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV"], 
                capture_output=True, 
                text=True
            )
            
            if "python" in result.stdout.lower():
                print(f"Killing Python process PID {pid}...")
                subprocess.run(["taskkill", "/F", "/PID", str(pid)], check=True)
                killed_count += 1
                time.sleep(1)  # Give it time to clean up
        except Exception as e:
            print(f"Could not kill process {pid}: {e}")
    
    return killed_count


def wait_for_time_wait_cleanup(port: int, max_wait: int = 30) -> bool:
    """Wait for TIME_WAIT connections to clear naturally"""
    print(f"Waiting for TIME_WAIT connections on port {port} to clear...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        connections = get_port_connections(port)
        time_wait_connections = [c for c in connections if c["state"] == "TIME_WAIT"]
        
        if not time_wait_connections:
            print("✅ All TIME_WAIT connections cleared")
            return True
        
        print(f"⏳ Still {len(time_wait_connections)} TIME_WAIT connections, waiting...")
        time.sleep(2)
    
    print(f"⚠️ Timeout: TIME_WAIT connections still present after {max_wait} seconds")
    return False


def diagnose_port_issue(port: int = 8001):
    """Diagnose port binding issues"""
    host = "127.0.0.1"
    
    print(f"🔍 Diagnosing port {port} on {host}")
    print("=" * 50)
    
    # Check current connections
    connections = get_port_connections(port)
    if connections:
        print(f"Found {len(connections)} connections on port {port}:")
        for conn in connections:
            print(f"  {conn['state']:15} {conn['local_address']:20} -> {conn['foreign_address']:20} (PID: {conn['pid']})")
    else:
        print(f"No connections found on port {port}")
    
    # Check port availability
    print(f"\n🔌 Testing port availability...")
    is_available = check_port_availability(host, port)
    print(f"Port {port} is {'✅ AVAILABLE' if is_available else '❌ NOT AVAILABLE'}")
    
    if not is_available:
        print(f"\n🛠️ Port {port} is not available. Attempting to resolve...")
        
        # Try to kill listening processes
        killed = kill_processes_on_port(port)
        if killed > 0:
            print(f"Killed {killed} processes")
            time.sleep(2)  # Give time for cleanup
            
            # Check again
            is_available = check_port_availability(host, port)
            print(f"Port {port} is now {'✅ AVAILABLE' if is_available else '❌ STILL NOT AVAILABLE'}")
        
        # If still not available, wait for TIME_WAIT to clear
        if not is_available:
            remaining_connections = get_port_connections(port)
            time_wait_count = len([c for c in remaining_connections if c["state"] == "TIME_WAIT"])
            
            if time_wait_count > 0:
                print(f"\n⏳ Found {time_wait_count} TIME_WAIT connections")
                wait_for_time_wait_cleanup(port, max_wait=30)
                
                # Final check
                is_available = check_port_availability(host, port)
                print(f"Final status: Port {port} is {'✅ AVAILABLE' if is_available else '❌ STILL NOT AVAILABLE'}")
    
    return is_available


def main():
    """Main function"""
    print("🔧 AmoCRM Data Exporter - Port Connection Diagnostic Tool")
    
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Error: Port must be a number")
            sys.exit(1)
    else:
        port = 8001  # Default port
    
    print(f"Checking port: {port}")
    
    try:
        is_available = diagnose_port_issue(port)
        
        if is_available:
            print(f"\n✅ Port {port} is ready for use!")
            print(f"You can now start the server with: python web_server.py")
        else:
            print(f"\n❌ Port {port} is still not available")
            print("Possible solutions:")
            print("1. Wait 1-2 minutes for TIME_WAIT connections to clear naturally")
            print("2. Restart your computer to clear all socket states")
            print("3. Use a different port (modify the port in web_server.py)")
            print("4. Check for other applications using this port")
            
    except KeyboardInterrupt:
        print("\n\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()