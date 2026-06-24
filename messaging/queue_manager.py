import json
import logging
from typing import Callable, Optional

import pika

from config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    RABBITMQ_PASS,
    EXCHANGE_NAME,
    EXCHANGE_TYPE,
    QUEUE_TASKS,
    QUEUE_STATUS,
    QUEUE_MONITOR,
    ROUTING_TASK_ASSIGN,
    ROUTING_TASK_ACK,
    ROUTING_TASK_STATUS,
    ROUTING_TASK_COMPLETED,
    ROUTING_TASK_FAILED,
    ROUTING_MONITOR_HEARTBEAT,
    ROUTING_MONITOR_SUMMARY,
)
from messaging.message import Message

logger = logging.getLogger(__name__)

# Each agent subscribes to specific queues.
# Agent A (Planner)  <- monitor queue  (task.ack, monitor.summary, monitor.heartbeat)
# Agent B (Executor) <- tasks queue    (task.assign)
# Agent C (Monitor)  <- status queue   (task.status_update, task.completed, task.failed)

ROUTING_KEYS = {
    QUEUE_TASKS: [ROUTING_TASK_ASSIGN],
    QUEUE_STATUS: [
        ROUTING_TASK_STATUS,
        ROUTING_TASK_COMPLETED,
        ROUTING_TASK_FAILED,
    ],
    QUEUE_MONITOR: [
        ROUTING_TASK_ACK,
        ROUTING_MONITOR_HEARTBEAT,
        ROUTING_MONITOR_SUMMARY,
    ],
}


class QueueManager:
    def __init__(self):
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

    def connect(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type=EXCHANGE_TYPE,
            durable=True,
        )

        for queue_name, routing_keys in ROUTING_KEYS.items():
            self.channel.queue_declare(queue=queue_name, durable=True)
            for key in routing_keys:
                self.channel.queue_bind(
                    exchange=EXCHANGE_NAME,
                    queue=queue_name,
                    routing_key=key,
                )

        logger.info("Connected to RabbitMQ and declared exchange/queues.")

    def publish_message(self, message: Message, routing_key: Optional[str] = None):
        if not self.channel or self.channel.is_closed:
            logger.warning("Channel closed. Reconnecting...")
            self.connect()

        key = routing_key or message.message_type
        body = message.to_json()

        properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=message.message_id,
            timestamp=None,
            expiration=str(message.ttl_seconds * 1000) if message.ttl_seconds else None,
            priority={"low": 0, "medium": 1, "high": 2}.get(message.priority, 1),
        )

        self.channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=key,
            body=body,
            properties=properties,
        )
        logger.info(
            "Published: %s (%s) -> %s", message.message_type, message.message_id[:8], key
        )

    def subscribe(
        self,
        queue_name: str,
        callback: Callable[[Message], None],
        prefetch_count: int = 1,
    ):
        if not self.channel or self.channel.is_closed:
            self.connect()

        self.channel.basic_qos(prefetch_count=prefetch_count)

        def pika_callback(ch, method, properties, body):
            try:
                data = json.loads(body)
                message = Message.from_dict(data)
                logger.info(
                    "Received: %s (%s) from %s",
                    message.message_type,
                    message.message_id[:8],
                    message.sender_id,
                )
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error("Failed to process message: %s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=pika_callback,
            auto_ack=False,
        )
        logger.info("Subscribed to queue: %s", queue_name)

    def start_consuming(self):
        logger.info("Start consuming messages...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        finally:
            self.close()

    def close(self):
        if self.channel and not self.channel.is_closed:
            self.channel.close()
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        logger.info("Connection closed.")
