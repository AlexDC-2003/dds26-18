#!/bin/sh
# Generates sentinel.conf at runtime (sentinel rewrites its conf file, so we
# cannot use a read-only bind-mount) and starts redis-sentinel.
cat > /tmp/sentinel.conf << 'EOF'
port 26379
sentinel resolve-hostnames yes
sentinel announce-hostnames yes

# ── order cluster ────────────────────────────────────────────────────────────
sentinel monitor order-master order-db-primary 6379 2
sentinel auth-pass order-master redis
sentinel down-after-milliseconds order-master 3000
sentinel failover-timeout order-master 10000
sentinel parallel-syncs order-master 1
sentinel client-reconfig-script order-master /tmp/reconfig.sh

# ── stock cluster ─────────────────────────────────────────────────────────────
sentinel monitor stock-master stock-db-primary 6379 2
sentinel auth-pass stock-master redis
sentinel down-after-milliseconds stock-master 3000
sentinel failover-timeout stock-master 10000
sentinel parallel-syncs stock-master 1
sentinel client-reconfig-script stock-master /tmp/reconfig.sh

# ── payment cluster ───────────────────────────────────────────────────────────
sentinel monitor payment-master payment-db-primary 6379 2
sentinel auth-pass payment-master redis
sentinel down-after-milliseconds payment-master 3000
sentinel failover-timeout payment-master 10000
sentinel parallel-syncs payment-master 1
sentinel client-reconfig-script payment-master /tmp/reconfig.sh
EOF

cp /scripts/reconfig.sh /tmp/reconfig.sh
chmod +x /tmp/reconfig.sh
exec redis-sentinel /tmp/sentinel.conf
