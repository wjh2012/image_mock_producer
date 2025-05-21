import json
from dataclasses import asdict
from datetime import timezone, datetime

import aio_pika
from uuid_extensions import uuid7str

from app.config import get_settings
from app.custom_logger import time_logger
from app.publish_message import MessagePayload, MessageHeader

config = get_settings()


class AioPublisher:
    def __init__(self):
        self.amqp_url = f"amqp://{config.rabbitmq_username}:{config.rabbitmq_password}@{config.rabbitmq_host}:{config.rabbitmq_port}"
        self.exchange_name = config.rabbitmq_image_publish_exchange
        self.routing_keys = config.rabbitmq_image_publish_routing_keys
        self._connection = None
        self._channel = None
        self._exchange = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self.amqp_url, heartbeat=180)
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
        )
        print("✅ RabbitMQ 연결 성공")

    @time_logger
    async def send_message(self, trace_id: str, body: MessagePayload):
        if not self._channel:
            raise RuntimeError(
                "RabbitMQ 채널이 설정되지 않았습니다. connect()를 먼저 호출하세요."
            )

        for routing_key in self.routing_keys:
            event_id = uuid7str()
            event_type = routing_key

            headers = MessageHeader(
                event_id=event_id,
                event_type=event_type,
                trace_id=trace_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                source_service="image-mock-producer",
            )

            message = aio_pika.Message(
                body=json.dumps(asdict(body)).encode("utf-8"),
                headers=asdict(headers),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )

            await self._exchange.publish(message=message, routing_key=routing_key)

            print(f"📨 메시지 전송 완료 (Routing Key: {routing_key})")

    async def close(self):
        if self._connection:
            await self._connection.close()
            print("❌ RabbitMQ 연결 종료")
