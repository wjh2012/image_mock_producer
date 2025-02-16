import pika

from app.config import get_settings

config = get_settings()


class RabbitMQConsumer:
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

    def consume(self, callback):
        try:
            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=callback, auto_ack=True
            )
            print("RabbitMQ Consumer 시작. 메시지 대기 중...")
            self.channel.start_consuming()
        except Exception as e:
            raise Exception(f"RabbitMQ 소비 실패: {e}")

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()
