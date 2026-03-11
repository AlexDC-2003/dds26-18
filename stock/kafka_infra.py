# stock/kafka_infra.py
import asyncio
import json
import os
import threading
from typing import Optional, Callable, Any, Dict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from lock_manager import WaitDieAbort, LockTimeout, MAX_RETRIES

class StockKafkaInfrastructure:

    def __init__(self, dispatcher: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        self.dispatcher = dispatcher

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

        self._producer: Optional[AIOKafkaProducer] = None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._consume_task: Optional[asyncio.Task] = None

        # env (match your docker-compose style; tolerate quotes)
        self._bootstrap = (os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092") or "").strip('"')
        self._commands_topic = (os.environ.get("KAFKA_STOCK_COMMANDS_TOPIC", "stock.commands") or "").strip('"')
        self._replies_topic = (os.environ.get("KAFKA_STOCK_REPLIES_TOPIC", "stock.replies") or "").strip('"')

        # consumer group for scaling (multiple replicas share work across partitions)
        self._group_id = os.environ.get("KAFKA_STOCK_GROUP_ID", "stock-service-group")

    def start(self) -> None:
        """Start background thread + loop and connect producer/consumer (with retries)."""
        if self._thread is not None:
            return

        if not self._bootstrap:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS not set")

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        # Wait until loop is ready (same pattern as Order)
        while self._loop is None:
            pass

        fut = asyncio.run_coroutine_threadsafe(self._async_start_with_retry(), self._loop)
        try:
            fut.result(timeout=10)
        except Exception:
            pass

    def stop(self) -> None:
        """Stop consumer/producer and shut down loop."""
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
            except KafkaConnectionError as e:
                print(f'Unable connect to "{self._bootstrap}": {e}. Retrying in {backoff:.1f}s...')
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            except Exception as e:
                # e.g. auth/serialization errors; retry slowly but keep service alive
                print(f"Kafka start error: {e}. Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _async_start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self._producer.start()

        self._consumer = AIOKafkaConsumer(
            self._commands_topic,
            bootstrap_servers=self._bootstrap,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        await self._consumer.start()

        self._consume_task = asyncio.create_task(self._consume_commands())
        print(f"Stock Kafka started: consume={self._commands_topic} reply={self._replies_topic} bootstrap={self._bootstrap}")

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

    async def _consume_commands(self) -> None:
        assert self._consumer is not None
        assert self._producer is not None

        async for msg in self._consumer:
            command = msg.value
            # Run blocking dispatcher in executor so the event loop stays unblocked
            #loop = asyncio.get_event_loop()
            reply = self.dispatcher(command)
            #await loop.run_in_executor(None, self._dispatch_with_retry, command)
            try:
                await self._producer.send_and_wait(self._replies_topic, reply)
                await self._consumer.commit()
            except Exception as send_err:
                print(f"Failed to send error reply: {send_err}")
                continue



    # def _dispatch_with_retry(self, command: Dict[str, Any]) -> Dict[str, Any]:
    #     # Retry on wait-die abort/timeout (dispatcher already sleeps a random back-off before re-raising).
    #     backoff = 1.0

    #     for attempt in range(1, MAX_RETRIES + 1):
    #         try:
    #             return self.dispatcher(command)
    #         except (WaitDieAbort, LockTimeout) as e:
    #             if attempt < MAX_RETRIES:
    #                 print(f"[2PL] Wait-Die retry {attempt}/{MAX_RETRIES} for msg_id={command.get('msg_id')}")
    #                 backoff = min(backoff * 2, 30.0)
    #                 # await asyncio.sleep(backoff)
    #             else:
    #                 print(f"[2PL] All {MAX_RETRIES} retries exhausted for msg_id={command.get('msg_id')}")
    #                 return {"msg_id": command.get("msg_id"), "tx_id": command.get("tx_id"), "ok": False, "error": str(e)}
    #         except Exception as e:
    #             print(f"Error processing command: {e}")
    #             try:
    #                 return {"msg_id": command.get("msg_id"), "tx_id": command.get("tx_id"), "ok": False, "error": str(e)}
    #             except Exception as send_err:
    #                 print(f"Failed to send error reply: {send_err}")
