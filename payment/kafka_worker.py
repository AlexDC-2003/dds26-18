import asyncio
import json
import logging
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from msgspec import Struct, msgpack

from lock_manager import LockManager, WaitDieAbort, LockTimeout, transaction_context


class UserValue(Struct):
    credit: int


class ChargeTxValue(Struct):
    user_id: str
    amount: int
    credit_after: int
    ts: float


class RefundTxValue(Struct):
    user_id: str
    amount: int
    credit_after: int
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
                continue
            await self._process_one(cmd)
            try:
                await self._consumer.commit()
            except Exception as commit_err:
                print(f"Failed to commit Kafka offset: {commit_err}")

    async def _process_one(self, cmd: Dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        try:
            reply = await loop.run_in_executor(None, self._handle_command, cmd)
            payload = json.dumps(reply).encode("utf-8")
            await self._producer.send_and_wait(self._replies_topic, payload)
        except Exception as e:
            print(f"Failed to process/send payment reply: {e}")

    def _handle_command(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        msg_id = cmd.get("msg_id")
        tx_id = cmd.get("tx_id")
        typ = cmd.get("type")
        payload = cmd.get("payload") or {}

        # tx_ts allows the order service to propagate the saga's birth timestamp
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
            if typ == "charge_user":
                user_id = str(payload.get("user_id", ""))
                amount = int(payload.get("amount", 0))
                ok, err, new_credit = self._charge_user(tx_id, user_id, amount, msg_id=msg_id, tx_ts=tx_ts)
                if ok:
                    base["status_code"] = 200
                    base["payload"] = {"user_id": user_id, "amount": amount, "credit": new_credit}
                else:
                    base["error"] = err
                return base

            if typ == "refund_user":
                user_id = str(payload.get("user_id", ""))
                amount = int(payload.get("amount", 0))
                ok, err, new_credit = self._refund_user(tx_id, user_id, amount, msg_id=msg_id, tx_ts=tx_ts)
                if ok:
                    base["status_code"] = 200
                    base["payload"] = {"user_id": user_id, "amount": amount, "credit": new_credit}
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

    def _charge_user(
        self,
        tx_id: str,
        user_id: str,
        amount: int,
        *,
        msg_id: str,
        tx_ts: Optional[float] = None,
    ) -> Tuple[bool, Optional[str], Optional[int]]:
        if not user_id:
            return False, "missing user_id", None
        if amount <= 0:
            return False, "amount must be > 0", None

        tx_key = f"pay_tx:{msg_id}"
        # Refund key written by _refund_user for this same tx_id.
        # If it exists the charge was already reversed, so a retry must
        # re-charge rather than return the stale idempotency record.
        refund_key = f"refund_tx:refund:{tx_id}:{user_id}"
        logger.info("[CHARGE] msg_id=%s tx_id=%s user=%s amount=%s", msg_id, tx_id, user_id, amount)

        # Fast idempotency check (no lock needed — tx record is immutable once written).
        existing = self._db.get(tx_key)
        if existing and not self._db.get(refund_key):
            tx = msgpack.decode(existing, type=ChargeTxValue)
            logger.info("[CHARGE:IDEMPOTENT] msg_id=%s user=%s credit_after=%s", msg_id, user_id, tx.credit_after)
            return True, None, tx.credit_after

        # Resources we need to lock (sorted inside transaction_context too, but
        # explicit ordering here makes the intent clear):
        #   1. user record
        #   2. idempotency key for this message
        resources = [f"user:{user_id}", f"pay_tx:{msg_id}"]

        try:
            with transaction_context(
                self._lock_manager,
                resources,
                tx_id=tx_id,
                ts=tx_ts,
            ) as txn:
                # ---- Growing phase complete; now read and write ----

                # Re-check idempotency under lock.
                existing2 = self._db.get(tx_key)
                if existing2 and not self._db.get(refund_key):
                    tx = msgpack.decode(existing2, type=ChargeTxValue)
                    logger.info("[CHARGE:IDEMPOTENT-LOCKED] msg_id=%s user=%s credit_after=%s", msg_id, user_id, tx.credit_after)
                    return True, None, tx.credit_after

                raw_user = self._db.get(user_id)
                if not raw_user:
                    return False, f"User: {user_id} not found!", None

                user = msgpack.decode(raw_user, type=UserValue)
                if user.credit < amount:
                    return False, "User out of credit", None

                user.credit -= amount
                now = time.time()
                tx_record = ChargeTxValue(
                    user_id=user_id, amount=amount, credit_after=user.credit, ts=now
                )

                pipe = self._db.pipeline(transaction=True)
                pipe.set(user_id, msgpack.encode(user))
                pipe.set(tx_key, msgpack.encode(tx_record))
                # Clear the refund key so this re-charge is not repeated on
                # the next idempotency check (only bypass once per refund).
                pipe.delete(refund_key)
                pipe.execute()
                logger.info("[CHARGE:COMMITTED] msg_id=%s tx_id=%s user=%s amount=%s credit_after=%s", msg_id, tx_id, user_id, amount, user.credit)

                return True, None, user.credit

        except WaitDieAbort as e:
            logger.warning("[CHARGE:ABORTED] msg_id=%s tx_id=%s user=%s reason=wait-die: %s", msg_id, tx_id, user_id, e)
            return False, f"wait-die abort: {e}", None
        except LockTimeout as e:
            logger.warning("[CHARGE:ABORTED] msg_id=%s tx_id=%s user=%s reason=lock-timeout: %s", msg_id, tx_id, user_id, e)
            return False, f"lock timeout: {e}", None
        except redis.exceptions.RedisError as e:
            logger.error("[CHARGE:ERROR] msg_id=%s tx_id=%s user=%s error=%s", msg_id, tx_id, user_id, e)
            return False, f"DB error: {e}", None

    def _refund_user(
        self,
        tx_id: str,
        user_id: str,
        amount: int,
        *,
        msg_id: str,
        tx_ts: Optional[float] = None,
    ) -> Tuple[bool, Optional[str], Optional[int]]:
        if not user_id:
            return False, "missing user_id", None
        if amount <= 0:
            return False, "amount must be > 0", None

        # Reconstruct the charge's idempotency key: charge msg_id = f"charge:{tx_id}:{user_id}"
        charge_tx_key = f"pay_tx:charge:{tx_id}:{user_id}"
        refund_tx_key = f"refund_tx:{msg_id}"

        # Fast idempotency check — already refunded.
        existing_refund = self._db.get(refund_tx_key)
        if existing_refund:
            tx = msgpack.decode(existing_refund, type=RefundTxValue)
            return True, None, tx.credit_after

        # Safe no-op — never charged, so nothing to refund.
        existing_charge = self._db.get(charge_tx_key)
        if not existing_charge:
            return True, None, None

        resources = [f"user:{user_id}", f"refund_tx:{msg_id}"]

        try:
            with transaction_context(
                self._lock_manager,
                resources,
                tx_id=tx_id,
                ts=tx_ts,
            ) as txn:
                # ---- Growing phase complete ----

                # Re-check idempotency under lock.
                existing2 = self._db.get(refund_tx_key)
                if existing2:
                    tx = msgpack.decode(existing2, type=RefundTxValue)
                    return True, None, tx.credit_after

                raw_user = self._db.get(user_id)
                if not raw_user:
                    return False, f"User: {user_id} not found!", None

                user = msgpack.decode(raw_user, type=UserValue)
                user.credit += amount

                now = time.time()
                tx_record = RefundTxValue(
                    user_id=user_id, amount=amount, credit_after=user.credit, ts=now
                )

                pipe = self._db.pipeline(transaction=True)
                pipe.set(user_id, msgpack.encode(user))
                pipe.set(refund_tx_key, msgpack.encode(tx_record))
                pipe.execute()
                print(f"[IDEMPOTENCY] wrote key={refund_tx_key}")

                return True, None, user.credit

        except WaitDieAbort as e:
            return False, f"wait-die abort: {e}", None
        except LockTimeout as e:
            return False, f"lock timeout: {e}", None
        except redis.exceptions.RedisError as e:
            return False, f"DB error: {e}", None