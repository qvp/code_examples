import time
import argparse
from typing import Generator
from multiprocessing import Queue

from pika.exceptions import AMQPConnectionError
from pika.adapters.utils.connection_workflow import AMQPConnectionWorkflowFailed, AMQPConnectorSocketConnectError

from . import schema, utils
from .logger import logger
from .task import Task
from .worker import WorkerCommand, WorkerTask
from .consumer import WorkerCommandConsumer, TaskConsumer


class TaskExample(Task):
    def run(self, task_message: schema.TaskMessage) -> Generator[schema.TaskProgress | schema.TaskDone, None, None]:
        for stage in range(1, 6):
            time.sleep(5)
            yield schema.TaskProgress(
                message=f'Complete stage {stage}',
                in_percentages=stage * 20,
            )

        if task_message.payload.get('raise_error'):
            raise Exception('Error in task')

        yield schema.TaskDone(task_id=task_message.id)

    def on_progress_changed(self, task_message: schema.TaskMessage, ipc_message: schema.QueueMessage) -> None:
        # [записываем изменившийся прогресc в бд, отправляем уведомление и.т.д]
        pass

    def on_progress_timeout(self, task_message: schema.TaskMessage) -> None:
        # [уведомляем, что задача "зависла" и.т.д]
        pass

    def on_session_canceled(self, task_message: schema.TaskMessage, ipc_message: schema.QueueMessage) -> None:
        # [уведомляем, что задача отменена и.т.д]
        pass

    def on_done(self, task_message: schema.TaskMessage, ipc_message: schema.QueueMessage) -> None:
        # [уведомляем, что задача завершилась и.т.д]
        pass

    def on_error(self, task_message: schema.TaskMessage, ipc_message: schema.QueueMessage) -> None:
        # [уведомляем, что задача завершилась с ошибкой, перенаправляем на повторный запуск и.т.д]
        pass


def start_worker():
    parser = argparse.ArgumentParser(description='Runs calculation of tasks from the RabbitMQ queue.')
    parser.add_argument('--name', type=str, help='Name of the worker (showing in admin interface)')
    args_dict = vars(parser.parse_args())

    ipc_queue = Queue()  # для взаимодействия между процессами
    worker_name = args_dict.get('name', utils.create_worker_name())

    worker_command = WorkerCommand(ipc_queue, worker_name)
    worker_command_consumer = WorkerCommandConsumer(ipc_queue, worker_command)
    worker_command_consumer.start()

    task_example = TaskExample(progress_timeout_sec=10)
    worker_task = WorkerTask(ipc_queue, worker_name, task_example)
    task_consumer = TaskConsumer(ipc_queue, worker_name, worker_task)
    task_consumer.start_consuming()


def main():
    while True:
        try:
            start_worker()
        except (AMQPConnectionError, AMQPConnectionWorkflowFailed, AMQPConnectorSocketConnectError):
            logger.info('Unable to connect RabbitMQ, wait 5 second and retry')
            time.sleep(5)
        except Exception as error:
            logger.error(f'Uncaught exception received, stop worker. Message: {error}')
            break


if __name__ == '__main__':
    main()
