import pika

from app.config import get_settings

config = get_settings()


class RabbitMQPublisher:
    def __init__(self, host="localhost", queue_name="my_queue"):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        try:
            connection_params = pika.ConnectionParameters(host=self.host)
            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except Exception as e:
            raise Exception(f"RabbitMQ 연결 실패: {e}")

    def publish_message(self, message: str):
        try:
            self.channel.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except Exception as e:
            raise Exception(f"RabbitMQ 메시지 발행 실패: {e}")

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()


def get_rabbitmq_publisher():
    publisher = RabbitMQPublisher(host="localhost", queue_name="my_queue")
    try:
        yield publisher
    finally:
        publisher.close()
