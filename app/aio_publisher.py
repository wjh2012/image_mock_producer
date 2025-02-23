import aio_pika


class AioPublisher:
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self._connection = None
        self._channel = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self.amqp_url)
        self._channel = await self._connection.channel()
        print("✅ RabbitMQ 연결 성공")

    async def send_message(self, routing_key: str, message: str):
        if not self._channel:
            raise RuntimeError(
                "RabbitMQ 채널이 설정되지 않았습니다. connect()를 먼저 호출하세요."
            )

        await self._channel.default_exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=routing_key,
        )
        print(f"📨 메시지 전송: {message}")

    async def close(self):
        if self._connection:
            await self._connection.close()
            print("❌ RabbitMQ 연결 종료")
