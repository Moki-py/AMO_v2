#!/bin/bash

# Script to upgrade FastStream and its components to compatible versions
echo "Upgrading FastStream and related packages..."

# Check if virtual environment exists and activate if it does
if [ -d "venv" ]; then
    echo "Using existing virtual environment"
    source venv/bin/activate
else
    echo "No virtual environment found"
fi

# Ensure pip is up to date
python -m pip install --upgrade pip

# First, uninstall faststream if it exists to avoid version conflicts
pip uninstall -y faststream

# Install or upgrade required packages
pip install 'faststream[rabbit,cli]>=0.4.0'

# Record the version in a local file
FASTSTREAM_VERSION=$(python -c "import faststream; print(faststream.__version__)")
echo "Upgraded FastStream to version: $FASTSTREAM_VERSION"
echo "$FASTSTREAM_VERSION" > .faststream-version

echo "Ensuring RabbitMQ dependencies are installed..."
pip install 'aio-pika>=9.0.0'

echo "Testing FastStream CLI availability..."
python -m faststream --version

if [ $? -eq 0 ]; then
    echo "FastStream upgrade completed successfully!"
    echo "You can now run the application with './run_resource_constrained.sh'"
else
    echo "FastStream CLI installation may have issues. Please check the error above."
    exit 1
fi