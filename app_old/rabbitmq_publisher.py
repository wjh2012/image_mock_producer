import pika

from app_old.config import get_settings
from app_old.custom_logger import logger

config = get_settings()


class RabbitMQPublisher:
    def __init__(self, host, port, username, password, queue):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.queue = queue
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=600,  # heartbeat 설정
                blocked_connection_timeout=300,  # 연결 차단 타임아웃 설정
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue, durable=True)
        except Exception as e:
            raise Exception(f"❌ RabbitMQ 연결 실패: {e}")

    def publish_message(self, message: str):
        try:
            self.channel.basic_publish(
                exchange="", routing_key=self.queue, body=message
            )
            logger.info(f"📩 RabbitMQ 메시지 발행 완료")
        except Exception as e:
            raise Exception(f"❌ RabbitMQ 메시지 발행 실패: {e}")

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()


def get_rabbitmq_publisher():
    publisher = RabbitMQPublisher(
        host=config.rabbitmq_host,
        port=config.rabbitmq_port,
        username=config.rabbitmq_username,
        password=config.rabbitmq_password,
        queue=config.rabbitmq_queue,
    )
    return publisher
