import time
import json
from datetime import datetime
from typing import Any
from multiprocessing import Queue
from abc import ABC, abstractmethod

from . import schema
from .logger import logger
from .task import TaskRunner, Task


class BaseWorker(ABC):
    def __init__(self, ipc_queue: Queue, worker_name: str) -> None:
        self._ipc_queue = ipc_queue
        self._worker_name = worker_name

    def __call__(self, ch, method_frame, header_frame, body) -> None:
        self.on_message(ch, method_frame, header_frame, body)

    @abstractmethod
    def on_message(self, ch, method_frame, header_frame, body) -> None:
        raise NotImplementedError('Method "on_message" not implemented')


class WorkerCommand(BaseWorker):
    def on_message(self, ch, method_frame, header_frame, body) -> None:
        logger.debug('New worker command: %s' % body)
        payload = json.loads(body)
        message_type = payload.get('message_type')

        match message_type:
            case schema.QueueMessageType.session_canceled:
                self._session_canceled(payload)
            case _:
                raise Exception(f'Message of type "{message_type}" not supported')

    def _session_canceled(self, payload: Any) -> None:
        message = schema.QueueMessage(
            type=schema.QueueMessageType.session_canceled,
            payload={'session_id': payload['session_id']}
        )
        self._ipc_queue.put(message)


class WorkerTask(BaseWorker):
    def __init__(self, ipc_queue: Queue, worker_name: str, task_obj: Task) -> None:
        super().__init__(ipc_queue, worker_name)
        self._task_obj = task_obj

    def on_message(self, ch, method_frame, header_frame, body) -> None:
        task_data = json.loads(body)
        task_message = schema.TaskMessage(
            id=task_data['id'],
            session_id=task_data['session_id'],
            payload=task_data['payload'],
        )

        task_runner = TaskRunner(self._ipc_queue, task_message, self._task_obj.run)
        task_runner.start()

        last_progress_at_dt = datetime.now()

        while True:
            time.sleep(1)
            last_progress_sec = (datetime.now() - last_progress_at_dt).total_seconds()

            if not self._ipc_queue.empty():
                ipc_message: schema.QueueMessage = self._ipc_queue.get()

                if ipc_message.type == schema.QueueMessageType.progress_changed:
                    logger.info(f'Task({task_message.id}) progress updated ({ipc_message.payload.in_percentages} %)')
                    last_progress_at_dt = datetime.now()
                    self._task_obj.on_progress_changed(task_message, ipc_message)

                if ipc_message.type == schema.QueueMessageType.session_canceled:
                    if ipc_message.payload['session_id'] == task_message.session_id:
                        logger.info(f'Task({task_message.id}) canceled')
                        task_runner.terminate()
                        self._task_obj.on_session_canceled(task_message, ipc_message)
                        ch.basic_ack(delivery_tag=method_frame.delivery_tag)
                        break

                if ipc_message.type == schema.QueueMessageType.done:
                    logger.info(f'Task({task_message.id}) done')
                    self._task_obj.on_done(task_message, ipc_message)
                    ch.basic_ack(delivery_tag=method_frame.delivery_tag)
                    break

                if ipc_message.type == schema.QueueMessageType.error:
                    logger.info(f'Task({task_message.id}) error: {ipc_message.payload["message"]}')
                    self._task_obj.on_error(task_message, ipc_message)
                    ch.basic_ack(delivery_tag=method_frame.delivery_tag)
                    break

            if self._task_obj.progress_timeout_sec and last_progress_sec > self._task_obj.progress_timeout_sec:
                logger.info(f'Task({task_message.id}) progress timeout')
                task_runner.terminate()
                self._task_obj.on_progress_timeout(task_message)
                ch.basic_nack(delivery_tag=method_frame.delivery_tag)
                break
