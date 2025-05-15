# FastStream Compatibility Fixes

This document explains the changes made to fix compatibility issues with newer versions of FastStream.

## Background

FastStream has undergone significant changes between versions, which have affected our implementation:

- Changes to the CLI command structure
- Updates to the RabbitMQ broker connection mechanisms
- Changes in the Pydantic model serialization methods

## Fixed Issues for FastStream 0.5.40

1. **RabbitBroker Initialization**:
   - Fixed error: `TypeError: RabbitBroker.__init__() got an unexpected keyword argument 'prefetch_count'`
   - Updated initialization process to match new API:
     ```python
     # Old approach
     broker = RabbitBroker(
         rabbitmq_url,
         prefetch_count=5,
         heartbeat=60
     )

     # New approach (0.5.40)
     broker = RabbitBroker(url=rabbitmq_url)
     await broker.connect(connection_kwargs={"heartbeat": 60})
     await broker.set_prefetch_count(5)
     ```

2. **Exchange and Queue Declarations**:
   - Updated parameter names for RabbitExchange and RabbitQueue
   - Fixed binding syntax for queue declarations:
     ```python
     # Old syntax
     await broker.declare_queue(
         queue,
         exchange=exchange,
         routing_key="key"
     )

     # New syntax (0.5.40)
     await broker.declare_queue(
         queue,
         exchange,
         routing_key="key"
     )
     ```

3. **Import Path Changes**:
   - Updated import paths for Exchange and Queue schemas:
     ```python
     # Old import
     from faststream.rabbit import RabbitExchange, RabbitQueue, ExchangeType

     # New import (0.5.40)
     from faststream.rabbit.schemas import ExchangeType, RabbitExchange, RabbitQueue
     ```

4. **Health Check Connection Checking**:
   - Updated how we check if the broker is connected:
     ```python
     # Old approach
     if broker.is_connected:
         # ...

     # New approach (0.5.40)
     if hasattr(broker, "_connection") and broker._connection:
         # ...
     ```

5. **CLI Command Syntax**:
   - Updated from `faststream run` to `python -m faststream run`
   - Added proper Python module invocation for better compatibility

6. **Lifecycle Hooks**:
   - Changed from broker-level to app-level lifecycle hooks:
     ```python
     # Old approach
     @broker.on_startup
     async def startup():
         # ...

     # New approach (0.5.40)
     @app.on_startup
     async def startup():
         # ...
     ```

## Upgrading

We've created an upgrade script `upgrade_faststream.sh` that:

1. Uninstalls any existing FastStream installation
2. Installs FastStream 0.5.40+ with RabbitMQ and CLI support
3. Verifies that the CLI tools are working

Run the script with:
```bash
./upgrade_faststream.sh
```

## Fixes Implemented

1. **Updated CLI Command Syntax:**
   - Changed from `faststream run` to `python -m faststream run`
   - This ensures compatibility with FastStream 0.4.0 and above

2. **RabbitMQ Connection Handling:**
   - Removed deprecated connection pool settings
   - Updated broker initialization to use the latest format

3. **Health Check Updates:**
   - Changed from `broker.is_connected` to `broker._broker` for connection checks
   - Added more robust error handling

4. **Pydantic Compatibility:**
   - Added support for both Pydantic v1 (`dict()`) and v2 (`model_dump()`) methods
   - Implemented fallbacks for version differences

5. **Worker Process Management:**
   - Updated worker startup code to handle newer FastStream versions
   - Added more robust error recovery and retry mechanisms

## How to Apply These Fixes

1. Run the `upgrade_faststream.sh` script to ensure your FastStream installation is up-to-date:
   ```bash
   ./upgrade_faststream.sh
   ```

2. Start the system with the updated resource-constrained script:
   ```bash
   ./run_resource_constrained.sh
   ```

## Troubleshooting

If you experience issues with FastStream:

1. **Check FastStream Version:**
   ```bash
   python -c "import faststream; print(faststream.__version__)"
   ```

2. **Verify CLI Installation:**
   ```bash
   python -m faststream --version
   ```

3. **Reinstall with CLI Support:**
   ```bash
   pip install 'faststream[rabbit,cli]>=0.4.0'
   ```

4. **Check RabbitMQ Connection:**
   ```bash
   python -m faststream ping --url amqp://guest:guest@localhost:5672/
   ```

## Notes on FastStream 0.5.0

FastStream 0.5.0 introduced significant changes to the middleware system. If you plan to use middlewares, be aware that they now need to be implemented as async context managers rather than simple functions.

Example:
```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def subscriber_middleware(msg_body):
    yield msg_body

@broker.subscriber("in", middlewares=(subscriber_middleware,))
async def handler():
    ...
```