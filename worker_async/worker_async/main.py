import time

import aiormq

from .worker import Worker
from .consumer import CommandConsumer, TaskConsumer
from .schema import TaskMessage
from .logger import logger


def blocking_task(task_message: TaskMessage) -> str:
    logger.debug(f'blocking_task получила данные для расчета:', task_message.payload)

    for i in range(task_message.payload['iteration_count']):
        time.sleep(1)
        logger.debug('blocking_task в процессе вычисления...')

    return '{TASK_RESPONSE}'


def start_worker():
    command_consumer = CommandConsumer(
        command_exchange_name='wa_command_exchange',
    )
    task_consumer = TaskConsumer(
        tasks_queue_name='wa_task_queue',
        task_handler=blocking_task,
    )

    worker = Worker(
        connection_url='amqp://admin:admin@rabbit/',
        consumers=[
            command_consumer,
            task_consumer,
        ],
    )

    worker.run()


def main():
    while True:
        try:
            start_worker()
        except aiormq.exceptions.AMQPConnectionError:
            logger.info('Connection error, wait 5 seconds and retry...')
            time.sleep(5)
        except Exception as critical_error:
            logger.error('Critical error:', critical_error)
            break


if __name__ == '__main__':
    main()
