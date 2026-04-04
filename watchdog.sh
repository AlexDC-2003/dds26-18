#!/bin/sh

restart_db() {
  db=$1
  echo "$db is down, restarting..."
  docker start $db
  sleep 3
  ROLE=$(docker exec $db redis-cli -a redis ROLE 2>/dev/null | head -1)
  if [ "$ROLE" = "master" ]; then
    docker exec $db redis-cli -a redis CONFIG SET appendfsync everysec 2>/dev/null || true
  else
    docker exec $db redis-cli -a redis CONFIG SET appendfsync always 2>/dev/null || true
  fi
}

while true; do
  # Single docker ps -a call — get all container names and statuses at once
  docker ps -a --format "{{.Names}}\t{{.State}}" | while IFS='	' read -r name state; do
    [ "$state" != "exited" ] && [ "$state" != "dead" ] && continue

    case "$name" in
      *-db-*)
        restart_db "$name" &
        ;;
      *service* | *orchestrator* | *sentinel*)
        echo "$name is down, restarting..."
        docker start "$name" &
        ;;
    esac
  done

  # Wait for any parallel restarts to finish before sentinel check
  wait

  # Sentinel correctness check
  for master_name in order-master stock-master payment-master; do
    SENTINEL=$(docker ps --format "{{.Names}}" | grep sentinel | head -1)
    [ -z "$SENTINEL" ] && continue
    MASTER_HOST=$(docker exec $SENTINEL redis-cli -p 26379 SENTINEL get-master-addr-by-name $master_name 2>/dev/null | head -1)
    [ -z "$MASTER_HOST" ] && continue
    MASTER_CONTAINER=$(docker ps --format "{{.Names}}" | grep "$MASTER_HOST" | head -1)
    [ -z "$MASTER_CONTAINER" ] && continue
    ACTUAL_ROLE=$(docker exec $MASTER_CONTAINER redis-cli -a redis ROLE 2>/dev/null | head -1)
    if [ "$ACTUAL_ROLE" != "master" ]; then
      echo "[watchdog] sentinel $master_name points to $MASTER_HOST which is not master (role=$ACTUAL_ROLE) — resetting"
      for s in $(docker ps --format "{{.Names}}" | grep sentinel); do
        docker exec $s redis-cli -p 26379 SENTINEL RESET $master_name 2>/dev/null || true
      done
    fi
  done

  sleep 2
done
