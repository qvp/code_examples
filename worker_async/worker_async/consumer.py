import json
import asyncio
from typing import Callable, Any
from asyncio.queues import Queue
from abc import ABC, abstractmethod
from functools import partial

import aiormq

from . import schema, utils
from .logger import logger


class BaseConsumer(ABC):
    @abstractmethod
    async def start_consume(self, connection: aiormq.abc.AbstractConnection, command_queue: Queue) -> None:
        pass


class CommandConsumer(BaseConsumer):
    def __init__(self, command_exchange_name: str) -> None:
        self._command_exchange_name = command_exchange_name

    async def start_consume(self, connection: aiormq.abc.AbstractConnection, command_queue: Queue) -> None:
        channel = await connection.channel()
        await channel.exchange_declare(exchange=self._command_exchange_name, exchange_type='fanout')
        commands_queue = await channel.queue_declare(exclusive=True)
        await channel.queue_bind(commands_queue.queue, self._command_exchange_name)

        await channel.basic_consume(commands_queue.queue, partial(self._on_message, command_queue), no_ack=True)

    @staticmethod
    async def _on_message(command_queue: Queue, message: aiormq.abc.DeliveredMessage) -> None:
        logger.debug(f'Поступила сообщение в очередь команд: {message.body}')
        message_data = json.loads(message.body)
        command_message = schema.CommandMessage(**message_data)
        await command_queue.put(command_message)


class TaskConsumer(BaseConsumer):
    def __init__(self, tasks_queue_name: str, task_handler: Callable[[schema.TaskMessage], Any]) -> None:
        self._tasks_queue_name = tasks_queue_name
        self._task_handler = task_handler

    async def start_consume(self, connection: aiormq.abc.AbstractConnection, command_queue: Queue) -> None:
        channel = await connection.channel()
        await channel.basic_qos(prefetch_count=1)  # получаем только по 1 задаче за раз
        await channel.queue_declare(self._tasks_queue_name)

        await channel.basic_consume(self._tasks_queue_name, partial(self._on_message, command_queue))

    async def _on_message(self, command_queue: Queue, message: aiormq.abc.DeliveredMessage) -> None:
        logger.debug(f'Поступило сообщение в очередь задач: {message.body}')
        message_data = json.loads(message.body)
        task_message = schema.TaskMessage(**message_data)

        # loop.run_in_executor не позволяет отменить задачу если она уже начала выполняться
        # поэтому создаем и управляем процессом самостоятельно
        task_process = utils.TaskRunner(target=self._task_handler, args=(task_message,))
        task_process.start()

        while True:
            await asyncio.sleep(1)

            if result := task_process.result():
                if type(result) == schema.TaskError:
                    logger.error(result)
                else:
                    logger.info(f'Подтверждаю завершение задачи #{task_message.id}')
                await message.channel.basic_ack(message.delivery.delivery_tag)
                break

            if not command_queue.empty():
                command_message: schema.CommandMessage = await command_queue.get()

                if command_message.type == schema.CommandMessage.Types.session_canceled:
                    if task_message.session_id == command_message.payload['session_id']:
                        task_process.terminate()
                        await message.channel.basic_ack(message.delivery.delivery_tag)
                        logger.info(f'Задача #{task_message.id} отменена')
                        break

                if command_message.type == schema.CommandMessage.Types.say_hello:
                    logger.info('Hello!')
