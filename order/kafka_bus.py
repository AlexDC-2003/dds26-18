# order/kafka_bus.py
import asyncio
import json
import os
import threading
from typing import Any, Callable, Dict, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


class KafkaBus:
    """
    Request/reply bus for a sync Flask app:
    - Runs an asyncio event loop in a background thread
    - Has a producer for commands
    - Has a consumer subscribed to reply topics
    - Matches replies to requests via msg_id
    """

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

        self._producer: Optional[AIOKafkaProducer] = None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._consume_task: Optional[asyncio.Task] = None

        # msg_id -> Future that will be completed when reply arrives
        self._pending: Dict[str, asyncio.Future] = {}

        self._bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
        self._replies_topics = [
            t.strip()
            for t in os.environ.get("KAFKA_REPLIES_TOPICS", "").split(",")
            if t.strip()
        ]

        self._event_handlers: Dict[str, Callable[[dict], None]] = {}  # event_type -> handler
        self._events_topics = [
            t.strip()
            for t in os.environ.get("KAFKA_EVENTS_TOPICS", "").split(",")
            if t.strip()
        ]
        self._event_consumer: Optional[AIOKafkaConsumer] = None
        self._event_consume_task: Optional[asyncio.Task] = None

    def register_handler(self, event_type: str, handler: Callable[[dict], None]) -> None:
        self._event_handlers[event_type] = handler
    
    def start(self) -> None:
        """Start the background thread + event loop and connect producer/consumer."""
        if self._thread is not None:
            return

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        # Wait until loop is ready
        while self._loop is None:
            pass
        if not self._bootstrap:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS not set")

        # Initialize producer/consumer inside the loop
        fut = asyncio.run_coroutine_threadsafe(self._async_start(), self._loop)
        fut.result(timeout=10)

    def stop(self) -> None:
        """Stop consumer/producer and shut down loop."""
        if self._loop is None:
            return
        fut = asyncio.run_coroutine_threadsafe(self._async_stop(), self._loop)
        try:
            fut.result(timeout=10)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def request(self, topic: str, message: dict, timeout_sec: float) -> dict:
        """
        Sync API used by Flask code.
        Sends message to `topic` and waits for matching reply by msg_id.
        """
        if self._loop is None:
            raise RuntimeError("KafkaBus not started")

        fut = asyncio.run_coroutine_threadsafe(
            self._async_request(topic, message, timeout_sec), self._loop
        )
        return fut.result(timeout=timeout_sec + 1)

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        loop.run_forever()

    async def _async_start(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
        await self._producer.start()

        if not self._replies_topics:
            # You can still use producer-only mode (e.g., events)
            return

        if self._events_topics:
            self._event_consumer = AIOKafkaConsumer(
                *self._events_topics,
                bootstrap_servers=self._bootstrap,
                group_id=os.environ.get("KAFKA_ORDER_GROUP_ID", "order-service"),
                enable_auto_commit=True,
                auto_offset_reset="earliest",
            )
            await self._event_consumer.start()
            self._event_consume_task = asyncio.create_task(self._consume_events())

        # No group_id on purpose: in request/reply, safest is each instance sees all replies
        self._consumer = AIOKafkaConsumer(
            *self._replies_topics,
            bootstrap_servers=self._bootstrap,
            enable_auto_commit=True,
            auto_offset_reset="latest",
        )
        await self._consumer.start()

        self._consume_task = asyncio.create_task(self._consume_replies())

    async def _consume_events(self) -> None:
        assert self._event_consumer is not None
        async for msg in self._event_consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
            except Exception:
                continue
            event_type = data.get("type")
            handler = self._event_handlers.get(event_type)
            if handler:
                try:
                    handler(data)
                except Exception as e:
                    print(f"Error in event handler for {event_type}: {e}")

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
        
        if self._event_consume_task:
            self._event_consume_task.cancel()
            self._event_consume_task = None
            
        if self._event_consumer:
            await self._event_consumer.stop()
            self._event_consumer = None

        # Fail any pending requests
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(RuntimeError("KafkaBus stopped"))
        self._pending.clear()

    async def _async_request(self, topic: str, message: dict, timeout_sec: float) -> dict:
        if self._producer is None:
            raise RuntimeError("Producer not started")

        msg_id = message.get("msg_id")
        if not msg_id:
            raise ValueError("message must include msg_id")

        # Create Future tied to this loop
        reply_future = self._loop.create_future()  # type: ignore[union-attr]
        self._pending[msg_id] = reply_future

        try:
            payload = json.dumps(message).encode("utf-8")
            await self._producer.send_and_wait(topic, payload)

            reply = await asyncio.wait_for(reply_future, timeout=timeout_sec)
            return reply
        finally:
            self._pending.pop(msg_id, None)

    async def _consume_replies(self) -> None:
        assert self._consumer is not None

        async for msg in self._consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
            except Exception:
                continue

            msg_id = data.get("msg_id")
            if not msg_id:
                continue

            fut = self._pending.get(msg_id)
            if fut is not None and not fut.done():
                fut.set_result(data)

    def publish(self, topic: str, message: dict) -> None:
        """Fire-and-forget: send event without waiting for a reply."""
        if self._loop is None:
            raise RuntimeError("KafkaBus not started")
        fut = asyncio.run_coroutine_threadsafe(
            self._async_publish(topic, message), self._loop
        )
        fut.result(timeout=5)

    async def _async_publish(self, topic: str, message: dict) -> None:
        if self._producer is None:
            raise RuntimeError("Producer not started")
        payload = json.dumps(message).encode("utf-8")
        await self._producer.send_and_wait(topic, payload)