#!/bin/sh
# Called by sentinel after a failover completes.
# Args: <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
#   role  = leader | observer
#   state = start | end-of-failover
MASTER_NAME="$1"
ROLE="$2"
STATE="$3"
FROM_IP="$4"
FROM_PORT="$5"
TO_IP="$6"
TO_PORT="$7"

# Only act when the failover is complete
[ "$STATE" = "end-of-failover" ] || exit 0

echo "[reconfig] $MASTER_NAME: promoting $TO_IP:$TO_PORT, demoting $FROM_IP:$FROM_PORT"

# New master: switch to everysec (throughput optimised)
redis-cli -h "$TO_IP" -p "$TO_PORT" -a redis CONFIG SET appendfsync everysec

# Old master (now replica): switch to always (durable).
# May be down; ignore errors.
redis-cli -h "$FROM_IP" -p "$FROM_PORT" -a redis CONFIG SET appendfsync always 2>/dev/null || true
