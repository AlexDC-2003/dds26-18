#!/bin/bash
set -e

docker compose down -v
git checkout saga_dev
docker compose up -d --build
