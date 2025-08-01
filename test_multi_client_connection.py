#!/usr/bin/env python3
"""
Test script to verify multiple clients can connect to the AmoCRM Data Exporter server
"""

import asyncio
import aiohttp
import websockets
import json
import time
from concurrent.futures import ThreadPoolExecutor
import sys

# Configuration
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8001
BASE_URL = f"http://{SERVER_HOST}:{SERVER_PORT}"
WS_URL = f"ws://{SERVER_HOST}:{SERVER_PORT}/ws/progress"


async def test_http_client(client_id: int, session: aiohttp.ClientSession) -> dict:
    """Test HTTP client connection"""
    try:
        # Test basic endpoints
        async with session.get(f"{BASE_URL}/stats") as response:
            if response.status == 200:
                data = await response.json()
                print(f"✅ Client {client_id}: HTTP connection successful, got stats")
                return {"client_id": client_id, "status": "success", "type": "http", "data": data}
            else:
                print(f"❌ Client {client_id}: HTTP connection failed with status {response.status}")
                return {"client_id": client_id, "status": "failed", "type": "http", "error": f"HTTP {response.status}"}
    except Exception as e:
        print(f"❌ Client {client_id}: HTTP connection error: {e}")
        return {"client_id": client_id, "status": "error", "type": "http", "error": str(e)}


async def test_websocket_client(client_id: int) -> dict:
    """Test WebSocket client connection"""
    try:
        async with websockets.connect(WS_URL) as websocket:
            # Send ping message
            ping_message = {"type": "ping", "client_id": client_id}
            await websocket.send(json.dumps(ping_message))

            # Wait for pong response
            response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            response_data = json.loads(response)

            if response_data.get("type") == "pong":
                print(f"✅ Client {client_id}: WebSocket connection successful, got pong")
                return {"client_id": client_id, "status": "success", "type": "websocket", "data": response_data}
            else:
                print(f"⚠️ Client {client_id}: WebSocket unexpected response: {response_data}")
                return {"client_id": client_id, "status": "unexpected", "type": "websocket", "data": response_data}

    except Exception as e:
        print(f"❌ Client {client_id}: WebSocket connection error: {e}")
        return {"client_id": client_id, "status": "error", "type": "websocket", "error": str(e)}


async def test_concurrent_clients(num_clients: int = 5) -> dict:
    """Test multiple concurrent client connections"""
    print(f"\n🔧 Testing {num_clients} concurrent client connections to {BASE_URL}")
    print("=" * 60)

    results = {"http": [], "websocket": [], "summary": {}}

    # Create HTTP session with connection pooling
    connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Test HTTP clients
        print("\n📡 Testing HTTP connections...")
        http_tasks = [test_http_client(i, session) for i in range(1, num_clients + 1)]
        http_results = await asyncio.gather(*http_tasks, return_exceptions=True)

        for result in http_results:
            if isinstance(result, Exception):
                results["http"].append({"status": "exception", "error": str(result)})
            else:
                results["http"].append(result)

    # Test WebSocket clients
    print("\n🔌 Testing WebSocket connections...")
    ws_tasks = [test_websocket_client(i) for i in range(1, num_clients + 1)]
    ws_results = await asyncio.gather(*ws_tasks, return_exceptions=True)

    for result in ws_results:
        if isinstance(result, Exception):
            results["websocket"].append({"status": "exception", "error": str(result)})
        else:
            results["websocket"].append(result)

    # Calculate summary
    http_success = sum(1 for r in results["http"] if r.get("status") == "success")
    ws_success = sum(1 for r in results["websocket"] if r.get("status") == "success")

    results["summary"] = {
        "total_clients": num_clients,
        "http_success": http_success,
        "http_success_rate": f"{(http_success/num_clients)*100:.1f}%",
        "websocket_success": ws_success,
        "websocket_success_rate": f"{(ws_success/num_clients)*100:.1f}%",
        "overall_success": http_success == num_clients and ws_success == num_clients
    }

    return results


def print_test_results(results: dict):
    """Print formatted test results"""
    summary = results["summary"]

    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    print(f"Total Clients Tested: {summary['total_clients']}")
    print(f"HTTP Connections: {summary['http_success']}/{summary['total_clients']} ({summary['http_success_rate']})")
    print(f"WebSocket Connections: {summary['websocket_success']}/{summary['total_clients']} ({summary['websocket_success_rate']})")

    if summary["overall_success"]:
        print("\n✅ ALL TESTS PASSED - Multiple client support is working correctly!")
    else:
        print("\n❌ SOME TESTS FAILED - Check the details above for issues")

        # Print failed connections
        failed_http = [r for r in results["http"] if r.get("status") != "success"]
        failed_ws = [r for r in results["websocket"] if r.get("status") != "success"]

        if failed_http:
            print(f"\n❌ Failed HTTP connections: {len(failed_http)}")
            for failure in failed_http:
                print(f"   Client {failure.get('client_id', 'Unknown')}: {failure.get('error', 'Unknown error')}")

        if failed_ws:
            print(f"\n❌ Failed WebSocket connections: {len(failed_ws)}")
            for failure in failed_ws:
                print(f"   Client {failure.get('client_id', 'Unknown')}: {failure.get('error', 'Unknown error')}")

    print("=" * 60)


async def check_server_availability() -> bool:
    """Check if the server is running and accessible"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BASE_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status == 200:
                    print(f"✅ Server is running at {BASE_URL}")
                    return True
                else:
                    print(f"⚠️ Server responded with status {response.status}")
                    return False
    except Exception as e:
        print(f"❌ Server is not accessible: {e}")
        print(f"   Make sure the server is running on {BASE_URL}")
        return False


async def main():
    """Main test function"""
    print("🚀 AmoCRM Data Exporter - Multi-Client Connection Test")
    print(f"Testing server at: {BASE_URL}")

    # Check server availability first
    if not await check_server_availability():
        print("\n💡 To start the server, run: python web_server.py")
        return False

    # Run the multi-client test
    try:
        results = await test_concurrent_clients(num_clients=5)
        print_test_results(results)
        return results["summary"]["overall_success"]
    except Exception as e:
        print(f"\n💥 Test failed with exception: {e}")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️ Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)