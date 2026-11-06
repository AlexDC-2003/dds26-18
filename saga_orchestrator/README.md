# Saga with an orchestrator

The orchestrator coordinates checkout across the order, stock, and payment
services. Redis Sentinel provides database failover.

## Project structure

- `order/`, `payment/`, `stock/`, `orchestrator/`: services and Docker build files.
- `env/`: Redis environment files.
- `compose/`: default, medium, and large Docker Compose configurations.
- `compose/nginx/`: gateway configuration for each deployment size.
- `compose/kafka/`: Kafka topic initialization scripts.
- `redis/`: Redis Sentinel startup and failover scripts.
- `watchdog.sh`: container recovery and Sentinel checks.
- `tests/`: integration, fault-tolerance, and manual kill tests.
- `tests/smoke/`: checkout idempotency, insufficient stock, and insufficient credit checks.

## Deployment

Docker and Docker Compose are required. Run these commands from
`saga_orchestrator/`:

```bash
# Default deployment
docker compose -p order-processing -f compose/docker-compose.yml up -d --build

# Medium deployment
docker compose -p order-processing -f compose/docker-compose-medium.yml up -d --build

# Large deployment
docker compose -p order-processing -f compose/docker-compose-large.yml up -d --build

# Follow logs for the default deployment
docker compose -p order-processing -f compose/docker-compose.yml logs -f

# Stop the default deployment
docker compose -p order-processing -f compose/docker-compose.yml down
```

Choose one deployment size at a time. Use the same `-p order-processing` and `-f`
options for all commands targeting it. Stop the current implementation before
switching to another; the shared project name also shares named Redis volumes.
Add `-v` to `down` only when you want to delete named volumes.

The gateway is available at `http://localhost:8000`. Configuration paths are
relative to the Compose file in `compose/`; service build contexts, Redis
environment files, `redis/`, and `watchdog.sh` are one directory above it.

## Testing

Run tests from `saga_orchestrator/` against its running deployment:

```bash
pip install requests aiohttp
python tests/test_microservices.py

bash tests/smoke/checkout_idempotency.sh
bash tests/smoke/insufficient_stock.sh
bash tests/smoke/insufficient_credit.sh

python tests/fault_tolerance_test.py
python tests/kill_test.py
```

Fault-tolerance tests intentionally kill containers and expect the
`order-processing-` container name prefix. The manual kill test provides a load
window during which you can kill containers yourself.
