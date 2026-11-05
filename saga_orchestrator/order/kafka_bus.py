import asyncio
import json
import os
from typing import Dict, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


class KafkaBus:
    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._consume_task: Optional[asyncio.Task] = None

        self._pending: Dict[str, asyncio.Future] = {}

        self._bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
        self._replies_topics = [
            t.strip()
            for t in os.environ.get("KAFKA_REPLIES_TOPICS", "").split(",")
            if t.strip()
        ]

    async def start(self) -> None:
        if not self._bootstrap:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS not set")

        if self._producer is None:
            self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
            await self._producer.start()

        if self._replies_topics and self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                *self._replies_topics,
                bootstrap_servers=self._bootstrap,
                enable_auto_commit=True,
                auto_offset_reset="latest",
            )
            await self._consumer.start()
            self._consume_task = asyncio.create_task(self._consume_replies())

    async def stop(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
            self._consume_task = None

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._producer:
            await self._producer.stop()
            self._producer = None

        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(RuntimeError("KafkaBus stopped"))
        self._pending.clear()

    async def request(self, topic: str, message: dict, timeout_sec: float) -> dict:
        return await self._async_request(topic, message, timeout_sec)

    async def _async_request(self, topic: str, message: dict, timeout_sec: float) -> dict:
        if self._producer is None:
            raise RuntimeError("Producer not started")

        msg_id = message.get("msg_id")
        if not msg_id:
            raise ValueError("message must include msg_id")

        loop = asyncio.get_running_loop()
        reply_future = loop.create_future()
        self._pending[msg_id] = reply_future

        try:
            payload = json.dumps(message).encode("utf-8")
            await self._producer.send_and_wait(topic, payload,
                key=message.get("tx_id", "").encode("utf-8"))
            return await asyncio.wait_for(reply_future, timeout=timeout_sec)
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

    async def publish(self, topic: str, message: dict) -> None:
        if self._producer is None:
            raise RuntimeError("Producer not started")
        payload = json.dumps(message).encode("utf-8")
        await self._producer.send_and_wait(topic, payload)