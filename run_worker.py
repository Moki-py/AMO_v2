"""
Script to run the AMO export worker using FastStream
"""

import os
import argparse
from faststream.cli import run as faststream_run


def main():
    """Main entry point for the worker"""
    parser = argparse.ArgumentParser(description="AMO Export Worker")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (default: 1)"
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable hot reload for development"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="RabbitMQ host (default: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5672,
        help="RabbitMQ port (default: 5672)"
    )
    parser.add_argument(
        "--user",
        type=str,
        default="guest",
        help="RabbitMQ username (default: guest)"
    )
    parser.add_argument(
        "--password",
        type=str,
        default="guest",
        help="RabbitMQ password (default: guest)"
    )

    args = parser.parse_args()

    # Update RabbitMQ connection from environment or .env if available
    if os.environ.get("RABBITMQ_HOST"):
        args.host = os.environ.get("RABBITMQ_HOST", args.host)
    if os.environ.get("RABBITMQ_PORT"):
        port_str = os.environ.get("RABBITMQ_PORT", "")
        if port_str and port_str.isdigit():
            args.port = int(port_str)
    if os.environ.get("RABBITMQ_USER"):
        args.user = os.environ.get("RABBITMQ_USER", args.user)
    if os.environ.get("RABBITMQ_PASSWORD"):
        args.password = os.environ.get("RABBITMQ_PASSWORD", args.password)

    # Update broker URL in message_broker.py
    os.environ["RABBITMQ_URL"] = f"amqp://{args.user}:{args.password}@{args.host}:{args.port}/"

    # Prepare arguments for FastStream CLI
    faststream_args = [
        "message_broker:app",
        "--workers", str(args.workers)
    ]

    if args.reload:
        faststream_args.append("--reload")

    print(f"Starting AMO Export Worker with {args.workers} worker processes")
    print(f"RabbitMQ URL: amqp://{args.user}:***@{args.host}:{args.port}/")

    # Run FastStream worker
    faststream_run(faststream_args)


if __name__ == "__main__":
    main()