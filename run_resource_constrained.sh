#!/bin/bash

# Script to run AMO exporter in resource-constrained environment

# Set environment variables for resource optimization
export MAX_RETRIES=2
export BATCH_BUFFER_SIZE=5
export RETRY_DELAY=10

# Default configuration
CONFIG="rabbitmq.docker-compose.yml"
ACTION="up -d"
MINIMAL=0

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --minimal)
      MINIMAL=1
      shift
      ;;
    --down)
      ACTION="down"
      shift
      ;;
    --logs)
      ACTION="logs -f"
      shift
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --minimal   Use minimal configuration (for very limited resources)"
      echo "  --down      Stop all containers"
      echo "  --logs      Show logs"
      echo "  --help      Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Select configuration based on resources
if [ $MINIMAL -eq 1 ]; then
  CONFIG="minimal.docker-compose.yml"
  echo "Using minimal resource configuration"
else
  echo "Using standard resource-limited configuration"
fi

# Execute Docker Compose command
echo "Running: docker-compose -f $CONFIG $ACTION"
docker-compose -f $CONFIG $ACTION

# If starting up, show some helpful info
if [[ "$ACTION" == "up -d" ]]; then
  echo ""
  echo "-----------------------------------"
  echo "Services started in resource-constrained mode"
  echo ""

  if [ $MINIMAL -eq 1 ]; then
    echo "Access the API at: http://localhost:8000"
    echo "RabbitMQ running on port 5672 (no management UI)"
    echo "MongoDB running on port 27017"
  else
    echo "Access the API at: http://localhost:8000"
    echo "Access RabbitMQ management at: http://localhost:15672"
    echo "Access MongoDB Express at: http://localhost:8081"
  fi

  echo ""
  echo "To view logs: $0 --logs"
  echo "To stop services: $0 --down"
  echo "-----------------------------------"
fi