import traceback
from multiprocessing import Process, Queue
from typing import Callable, Iterator
from abc import ABC, abstractmethod

from . import schema


class TaskRunner(Process):
    def __init__(self, ipc_queue: Queue, task: schema.TaskMessage,
                 task_handler: Callable[[schema.TaskMessage], Iterator[schema.TaskProgress]]) -> None:
        self._ipc_queue = ipc_queue
        self._task = task
        self._task_handler = task_handler
        super().__init__()

    def run(self) -> None:
        try:
            for progress in self._task_handler(self._task):
                match type(progress):
                    case schema.TaskProgress:
                        message_type = schema.QueueMessageType.progress_changed
                    case schema.TaskDone:
                        message_type = schema.QueueMessageType.done
                    case _:
                        raise Exception('Unknown progress type')

                message = schema.QueueMessage(
                    type=message_type,
                    payload=progress,
                )
                self._ipc_queue.put(message)
        except Exception as error:
            message = schema.QueueMessage(
                type=schema.QueueMessageType.error,
                payload={
                    'message': str(error),
                    'traceback': traceback.format_exc(),
                },
            )
            self._ipc_queue.put(message)


class Task(ABC):
    def __init__(self, progress_timeout_sec=None):
        self.progress_timeout_sec = progress_timeout_sec

    @abstractmethod
    def run(self, task_message: schema.TaskMessage) -> Iterator[schema.TaskProgress | schema.TaskDone]:
        """
        Запускает выполнение задачи.

        Задача может возвращать результаты по частям или целиком.
        Опционально можно оправлять прогресс schema.TaskProgress (функция должна быть генератором).
        Но в конце обязательно нужно вернуть schema.TaskDone.
        """
        raise NotImplementedError

    def on_progress_changed(self, task_message: schema.TaskMessage, ipc_message: schema.QueueMessage) -> None:
        """
        Вызывается каждый раз когда метод run возвращает schema.TaskProgress
        """
        pass

    def on_progress_timeout(self, task_message: schema.TaskMessage) -> None:
        """
        Вызывается когда время ожидания прогресса истекло.

        В таком случае задача завершается и воркер возвращает сообщение обратно в очередь задач.
        """
        pass

    def on_session_canceled(self, task_message: schema.TaskMessage, ipc_message: schema.QueueMessage) -> None:
        """
        Вызывается когда отменяется сессия к которой пренадлежит задача.

        В таком случае задача завершается и воркер подтверждает обработку сообщения в очереди задач.
        """
        pass

    def on_done(self, task_message: schema.TaskMessage, ipc_message: schema.QueueMessage) -> None:
        """
        Вызывается когда задача успешно завершена.

        Воркер подтверждает обработку сообщения в очереди задач.
        """
        pass

    def on_error(self, task_message: schema.TaskMessage, ipc_message: schema.QueueMessage) -> None:
        """
        Вызывается когда во время выполнения задачи произошло исключение.

        В таком случае задача завершается и воркер подтверждает обработку сообщения в очереди задач.
        """
        pass
