from multiprocessing import Queue, Process
from typing import Tuple, Callable

import pika
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel

from . import conf


class BaseConsumer:
    @classmethod
    def _create_blocking_connection_and_channel(cls) -> Tuple[BlockingConnection, BlockingChannel]:
        credentials = pika.PlainCredentials(conf.RABBITMQ_USER, conf.RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=conf.RABBITMQ_HOST,
            port=conf.RABBITMQ_PORT,
            credentials=credentials,
        )
        conn = BlockingConnection(parameters)
        return conn, conn.channel()


class WorkerCommandConsumer(BaseConsumer, Process):
    def __init__(self, ipc_queue: Queue, on_message_callback: Callable) -> None:
        self._ipc_queue = ipc_queue
        self._on_message_callback = on_message_callback
        self._connection, self._channel = self._create_blocking_connection_and_channel()
        super().__init__()

    def run(self) -> None:
        self._setup()
        self._channel.start_consuming()

    def _setup(self) -> None:
        self._channel.exchange_declare(
            exchange=conf.WORKER_COMMANDS_EXCHANGE,
            exchange_type='fanout',
        )
        tmp_queue = self._channel.queue_declare(queue='', exclusive=True)
        self._channel.queue_bind(
            exchange=conf.WORKER_COMMANDS_EXCHANGE,
            queue=tmp_queue.method.queue,
        )
        self._channel.basic_consume(
            queue=tmp_queue.method.queue,
            on_message_callback=self._on_message_callback,
            auto_ack=True,
        )


class TaskConsumer(BaseConsumer):
    def __init__(self, ipc_queue: Queue, worker_name: str, on_message_callback: Callable) -> None:
        self._ipc_queue = ipc_queue
        self._worker_name = worker_name
        self._on_message_callback = on_message_callback
        self._connection, self._channel = self._create_blocking_connection_and_channel()
        super().__init__()

    def start_consuming(self) -> None:
        self._setup()

        try:
            self._channel.start_consuming()
        except Exception as error:
            self._channel.stop_consuming()
            self._connection.close()
            raise error

    def _setup(self) -> None:
        self._channel.basic_qos(prefetch_count=1)

        queue = self._channel.queue_declare(
            queue=conf.TASKS_QUEUE,
            arguments={'x-max-priority': conf.TASKS_MAX_PRIORITY},
            durable=True,
            auto_delete=False,
        )

        self._channel.basic_consume(
            queue=queue.method.queue,
            consumer_tag=self._worker_name,
            on_message_callback=self._on_message_callback,
        )
