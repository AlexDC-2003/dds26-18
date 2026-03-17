#!/bin/bash
set -e

docker compose down -v
git checkout saga_replicas
docker compose up -d --build
# locust --master -u 20000 -r 100  -f locustfile.py --host http://localhost:8000
# locust --worker --master-host=localhost -f locustfile.py --host http://localhost:8000