"""
Script to run the AMO export worker using FastStream
"""

import os
import sys
import argparse
import subprocess


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
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retries for failed operations (default: 3)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch buffer size for processing (default: 10)"
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=5,
        help="Delay between retries in seconds (default: 5)"
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

    # Set resource optimization environment variables
    if os.environ.get("MAX_RETRIES"):
        args.max_retries = int(os.environ.get("MAX_RETRIES", args.max_retries))
    if os.environ.get("BATCH_BUFFER_SIZE"):
        args.batch_size = int(os.environ.get("BATCH_BUFFER_SIZE", args.batch_size))
    if os.environ.get("RETRY_DELAY"):
        args.retry_delay = int(os.environ.get("RETRY_DELAY", args.retry_delay))

    # Update broker URL in message_broker.py
    os.environ["RABBITMQ_URL"] = f"amqp://{args.user}:{args.password}@{args.host}:{args.port}/"
    os.environ["MAX_RETRIES"] = str(args.max_retries)
    os.environ["BATCH_BUFFER_SIZE"] = str(args.batch_size)
    os.environ["RETRY_DELAY"] = str(args.retry_delay)

    # Prepare command for running FastStream CLI
    cmd = ["faststream", "run", "message_broker:app"]

    if args.workers > 1:
        cmd.extend(["--workers", str(args.workers)])

    if args.reload:
        cmd.append("--reload")

    print(f"Starting AMO Export Worker with {args.workers} worker processes")
    print(f"RabbitMQ URL: amqp://{args.user}:***@{args.host}:{args.port}/")
    print(f"Resource settings: Max retries={args.max_retries}, Batch size={args.batch_size}, Retry delay={args.retry_delay}s")
    print(f"Running command: {' '.join(cmd)}")

    # Run FastStream CLI as a subprocess
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running FastStream: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("ERROR: FastStream CLI not found. Make sure you have installed FastStream with the CLI:")
        print("pip install 'faststream[cli]'")
        # Try to install it automatically and retry
        try:
            print("Attempting to install FastStream CLI...")
            subprocess.run([sys.executable, "-m", "pip", "install", "faststream[cli]"], check=True)
            print("Installation successful, retrying...")
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"Failed to install or run FastStream: {e}")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nWorker interrupted, shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()