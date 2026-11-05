# 2PC with an orchestrator

The order service forwards checkout requests to a dedicated orchestrator.
The orchestrator coordinates two-phase commit with stock and payment over Kafka.

## Project structure

- `order/`, `payment/`, `stock/`, `orchestrator/`: services and Docker build files.
- `env/`: Redis environment files, including the orchestrator database settings.
- `compose/`: default, medium, and large Docker Compose configurations.
- `compose/nginx/`: gateway configurations.
- `compose/kafka/`: Kafka topic initialization scripts.
- `test/`: integration, concurrency, and fault-tolerance tests.
- `test/smoke/`: checkout idempotency, insufficient stock, and insufficient credit checks.

## Deployment

Docker and Docker Compose are required. Run these commands from `2pc_orchestrator/`:

```bash
# Default deployment
docker compose -p order-processing -f compose/docker-compose.yml up -d --build

# Medium deployment
docker compose -p order-processing -f compose/docker-compose-medium.yml up -d --build

# Large deployment
docker compose -p order-processing -f compose/docker-compose-large.yml up -d --build

# Stop the default deployment
docker compose -p order-processing -f compose/docker-compose.yml down
```

Choose one deployment at a time. Use the same `-p order-processing` and `-f`
options for all commands targeting it. Stop the current implementation before
switching to another; the shared project name also shares named Redis volumes.
Add `-v` to `down` only when you want to delete named volumes.

The gateway is available at `http://localhost:8000`. Each Compose file embeds
its watchdog command, which restarts containers killed during fault tests.

## Testing

Run tests from `2pc_orchestrator/` against its running deployment:

```bash
pip install requests aiohttp
python tests/test_microservices.py

bash tests/smoke/check_idempotency.sh
bash tests/smoke/insufficient_stock.sh
bash tests/smoke/insufficient_credit.sh

python tests/concurrent_checkout_test.py
python tests/concurrent_checkout_3users_test.py
python tests/concurrent_checkout_5users_test.py
python tests/fault_tolerance_test.py
```

Fault-tolerance tests intentionally kill containers and expect the
`order-processing-` container name prefix.
