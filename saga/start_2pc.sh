#!/bin/bash
set -e

docker compose down -v
git checkout 2pc_dev
docker compose up -d --build
