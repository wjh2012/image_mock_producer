import aio_pika
from app.custom_logger import time_logger


class AioPublisher:
    def __init__(self, amqp_url: str, queues: list[str]):
        self.amqp_url = amqp_url
        self.queues = queues
        self._connection = None
        self._channel = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self.amqp_url, heartbeat=180)
        self._channel = await self._connection.channel()
        print("✅ RabbitMQ 연결 성공")

    @time_logger
    async def send_message(self, message: str):
        if not self._channel:
            raise RuntimeError(
                "RabbitMQ 채널이 설정되지 않았습니다. connect()를 먼저 호출하세요."
            )

        for queue in self.queues:
            await self._channel.default_exchange.publish(
                aio_pika.Message(body=message.encode()),
                routing_key=queue,
            )
            print(f"📨 메시지 전송 완료 (Queue: {queue})")

    async def close(self):
        if self._connection:
            await self._connection.close()
            print("❌ RabbitMQ 연결 종료")
