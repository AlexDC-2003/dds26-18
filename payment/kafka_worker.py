import asyncio
import json
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple

import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from msgspec import Struct, msgpack

from lock_manager import LockManager, Transaction, WaitDieAbort, LockTimeout


class UserValue(Struct):
    credit: int


class Payment2PCTxValue(Struct):
    user_id: str
    amount: int
    state: str
    credit_after_prepare: int
    ts: float


class PaymentKafkaWorker:
    """Kafka command handler for the Payment service.

    Implements request/reply over Kafka:
      - Consumes commands from KAFKA_PAYMENT_COMMANDS_TOPIC
      - Produces replies to KAFKA_PAYMENT_REPLIES_TOPIC

    Message contract (expected by order-service KafkaBus):
      Command:
        {
          "msg_id": "uuid",
          "tx_id": "uuid",
          "type": "charge_user" | "refund_user" | "health",
          "payload": {...}
        }

      Reply:
        {
          "msg_id": "uuid",
          "tx_id": "uuid",
          "status_code": 200 | 400,
          "payload": {...},
          "error": "..." | null,
          "ts": <float unix seconds>
        }

    Idempotency:
      - Successful charges are recorded at key pay_tx:<tx_id>
      - Successful refunds are recorded at key refund_tx:<tx_id>
      - Failures (insufficient funds) are NOT recorded so a retry after adding funds can succeed.

    Atomicity & Concurrency:
      - Uses 2-Phase Locking (2PL) with Wait-Die deadlock prevention.
      - The lock manager stores lock state in Redis, so it works across replicas.
      - Growing phase: acquire all needed locks before any read/write.
      - Shrinking phase: release all locks after the operation completes.
      - Wait-Die rule: an older transaction waits; a younger one aborts.

    #TODO(order-service): In kafka mode, order/app.py currently assumes `reply.status_code` like a
    `requests.Response`. Kafka replies are dicts, so order-service must use `reply["status_code"]`
    (and similarly for stock replies).

    #TODO(infra): Ensure Kafka topics exist and match env:
      - KAFKA_PAYMENT_COMMANDS_TOPIC (default: payment.commands)
      - KAFKA_PAYMENT_REPLIES_TOPIC (default: payment.replies)
    """

    def __init__(self, *, db: redis.Redis) -> None:
        self._db = db
        self._lock_manager = LockManager(db=db)
        self._tx_contexts: Dict[str, Transaction] = {}

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()

        self._bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
        self._commands_topic = os.environ.get("KAFKA_PAYMENT_COMMANDS_TOPIC", "payment.commands")
        self._replies_topic = os.environ.get("KAFKA_PAYMENT_REPLIES_TOPIC", "payment.replies")

        # Use a consumer group so multiple payment instances share work.
        self._group_id = os.environ.get("KAFKA_PAYMENT_GROUP_ID", "payment-service")

        self._producer: Optional[AIOKafkaProducer] = None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._consume_task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        if not self._bootstrap:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS not set")

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        while self._loop is None:
            time.sleep(0.01)

        fut = asyncio.run_coroutine_threadsafe(self._async_start_with_retry(), self._loop)
        try:
            fut.result(timeout=10)
        except Exception:
            pass

    def stop(self) -> None:
        self._stop_evt.set()
        if self._loop is None:
            return
        fut = asyncio.run_coroutine_threadsafe(self._async_stop(), self._loop)
        try:
            fut.result(timeout=10)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        loop.run_forever()

    async def _async_start_with_retry(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._async_start()
                return
            except Exception as e:
                print(f"Kafka start error: {e}. Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _async_start(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
        await self._producer.start()

        self._consumer = AIOKafkaConsumer(
            self._commands_topic,
            bootstrap_servers=self._bootstrap,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()

        self._consume_task = asyncio.create_task(self._consume())

    async def _async_stop(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
            self._consume_task = None

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def _consume(self) -> None:
        assert self._consumer is not None
        assert self._producer is not None

        async for msg in self._consumer:
            if self._stop_evt.is_set():
                break

            try:
                cmd = json.loads(msg.value.decode("utf-8"))
            except Exception:
                await self._consumer.commit()
                continue

            reply = self._handle_command(cmd)

            try:
                payload = json.dumps(reply).encode("utf-8")
                await self._producer.send_and_wait(self._replies_topic, payload)
                await self._consumer.commit()
            except Exception:
                # If we fail to publish the reply, client may timeout and retry.
                # Our operations are idempotent on success.
                continue

    def _handle_command(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        msg_id = cmd.get("msg_id")
        tx_id = cmd.get("tx_id")
        typ = cmd.get("type")
        payload = cmd.get("payload") or {}

        # tx_ts allows the order service to propagate the transaction birth timestamp
        # so that 2PL age comparisons are meaningful across services.
        tx_ts = cmd.get("tx_ts")  # optional float; defaults to now inside Transaction

        base = {
            "msg_id": msg_id,
            "tx_id": tx_id,
            "status_code": 400,
            "payload": {},
            "error": None,
            "ts": time.time(),
        }

        if not msg_id or not tx_id or not typ:
            base["error"] = "missing msg_id/tx_id/type"
            return base

        try:
            if typ == "prepare_payment":
                user_id = str(payload.get("user_id", ""))
                amount = int(payload.get("amount", 0))
                ok, err, new_credit = self._prepare_payment(tx_id, user_id, amount, tx_ts=tx_ts)
                if ok:
                    base["status_code"] = 200
                    base["payload"] = {"user_id": user_id, "amount": amount, "credit": new_credit, "state": "PREPARED"}
                else:
                    base["error"] = err
                return base

            if typ == "commit_payment":
                ok, err = self._commit_payment(tx_id, tx_ts=tx_ts)
                if ok:
                    base["status_code"] = 200
                    base["payload"] = {"state": "COMMITTED"}
                else:
                    base["error"] = err
                return base

            if typ == "abort_payment":
                ok, err = self._abort_payment(tx_id, tx_ts=tx_ts)
                if ok:
                    base["status_code"] = 200
                    base["payload"] = {"state": "ABORTED"}
                else:
                    base["error"] = err
                return base

            if typ == "health":
                base["status_code"] = 200
                base["payload"] = {"ok": True}
                return base

            base["error"] = f"unknown command type: {typ}"
            return base

        except Exception as e:
            base["error"] = f"exception: {type(e).__name__}: {e}"
            return base

    # ------------------------------------------------------------------
    # Business logic — protected by 2PL
    # ------------------------------------------------------------------

    def _get_or_create_tx_context(self, tx_id: str, *, tx_ts: Optional[float]) -> Transaction:
        txn = self._tx_contexts.get(tx_id)
        if txn is None:
            txn = Transaction(tx_id=tx_id, ts=tx_ts)
            self._tx_contexts[tx_id] = txn
        return txn

    def _acquire_user_lock(self, tx_id: str, user_id: str, *, tx_ts: Optional[float]) -> Tuple[bool, Optional[str]]:
        try:
            txn = self._get_or_create_tx_context(tx_id, tx_ts=tx_ts)
            self._lock_manager.acquire(txn, f"user:{user_id}")
            return True, None
        except WaitDieAbort as e:
            return False, f"wait-die abort: {e}"
        except LockTimeout as e:
            return False, f"lock timeout: {e}"
        except redis.exceptions.RedisError as e:
            return False, f"DB error: {e}"

    def _release_tx_locks(self, tx_id: str) -> None:
        txn = self._tx_contexts.pop(tx_id, None)
        if txn is not None:
            self._lock_manager.release_all(txn)

    def _prepare_payment(
        self,
        tx_id: str,
        user_id: str,
        amount: int,
        *,
        tx_ts: Optional[float] = None,
    ) -> Tuple[bool, Optional[str], Optional[int]]:
        print(f"Handling prepare_payment for user_id: {user_id}, amount: {amount}, tx_id: {tx_id}")
        if not user_id:
            return False, "missing user_id", None
        if amount <= 0:
            return False, "amount must be > 0", None

        tx_key = f"pay_2pc_tx:{tx_id}"

        # Fast idempotency check.
        existing = self._db.get(tx_key)
        if existing:
            tx = msgpack.decode(existing, type=Payment2PCTxValue)
            if tx.state == "ABORTED":
                return False, "transaction already aborted", None
            print(f"Idempotent prepare_payment for user_id: {user_id}, already prepared in tx_id: {tx_id}")
            return True, None, tx.credit_after_prepare

        lock_ok, lock_err = self._acquire_user_lock(tx_id, user_id, tx_ts=tx_ts)
        if not lock_ok:
            print(f"[2PL] Failed to acquire lock for user_id: {user_id} in tx_id: {tx_id}: {lock_err}")
            return False, lock_err, None
        print(f"Acquired lock for user_id: {user_id} in tx_id: {tx_id}")

        try:
            existing2 = self._db.get(tx_key)
            if existing2:
                tx = msgpack.decode(existing2, type=Payment2PCTxValue)
                if tx.state == "ABORTED":
                    return False, "transaction already aborted", None
                print(f"Idempotent prepare_payment for user_id: {user_id}, already prepared in tx_id: {tx_id}")
                return True, None, tx.credit_after_prepare

            raw_user = self._db.get(user_id)
            if not raw_user:
                print(f"User not found for user_id: {user_id} in tx_id: {tx_id}")
                self._release_tx_locks(tx_id)
                return False, f"User: {user_id} not found!", None

            user = msgpack.decode(raw_user, type=UserValue)
            if user.credit < amount:
                print(f"Insufficient credit for user_id: {user_id}, requested: {amount}, available: {user.credit} in tx_id: {tx_id}")
                self._release_tx_locks(tx_id)
                return False, "User out of credit", None

            tx_record = Payment2PCTxValue(
                user_id=user_id,
                amount=amount,
                state="PREPARED",
                credit_after_prepare=user.credit,
                ts=time.time(),
            )
            self._db.set(tx_key, msgpack.encode(tx_record))
            print(f"Prepared payment for user_id: {user_id}, amount: {amount} in tx_id: {tx_id}")
            return True, None, user.credit
        except redis.exceptions.RedisError as e:
            self._release_tx_locks(tx_id)
            return False, f"DB error: {e}", None

    def _commit_payment(
        self,
        tx_id: str,
        *,
        tx_ts: Optional[float] = None,
    ) -> Tuple[bool, Optional[str]]:
        tx_key = f"pay_2pc_tx:{tx_id}"
        raw = self._db.get(tx_key)
        if not raw:
            return True, None
        tx = msgpack.decode(raw, type=Payment2PCTxValue)
        if tx.state == "COMMITTED":
            return True, None
        if tx.state == "ABORTED":
            return False, "transaction already aborted"

        try:
            raw_user = self._db.get(tx.user_id)
            if not raw_user:
                return False, f"User: {tx.user_id} not found"

            user = msgpack.decode(raw_user, type=UserValue)

            user.credit -= tx.amount
            tx.state = "COMMITTED"
            tx.credit_after_prepare = user.credit
            pipe = self._db.pipeline(transaction=True)
            pipe.set(tx.user_id, msgpack.encode(user))
            pipe.set(tx_key, msgpack.encode(tx))
            pipe.execute()
            print(f"Committed payment for user_id: {tx.user_id}, amount: {tx.amount} in tx_id: {tx_id}")
            return True, None

        except redis.exceptions.RedisError as e:
            return False, f"DB error: {e}"
        finally:
            self._release_tx_locks(tx_id)

    def _abort_payment(
        self,
        tx_id: str,
        *,
        tx_ts: Optional[float] = None,
    ) -> Tuple[bool, Optional[str]]:
        tx_key = f"pay_2pc_tx:{tx_id}"
        raw = self._db.get(tx_key)
        if not raw:
            return True, None
        tx = msgpack.decode(raw, type=Payment2PCTxValue)
        if tx.state == "ABORTED":
            return True, None
        if tx.state == "COMMITTED":
            return False, "transaction already committed"

        try:
            tx.state = "ABORTED"
            self._db.set(tx_key, msgpack.encode(tx))
            self._release_tx_locks(tx_id)
            return True, None
        except redis.exceptions.RedisError as e:
            return False, f"DB error: {e}"
