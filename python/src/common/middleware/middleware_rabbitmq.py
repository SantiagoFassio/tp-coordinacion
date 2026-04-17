import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host)
        )

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
    
    def send(self, message: bytes):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=message
        )
    
    def start_consuming(self, callback):
        def on_message(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            callback(body, ack, nack)
        
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )

        self.channel.start_consuming()

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()

    def stop_consuming(self):
        if self.channel:
            self.channel.stop_consuming()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
