"""
Данный код не относится к ТЗ, а является попыткой разделить метод main.unpack_obj_archives_to_csv
на отдельные логические части чтобы не нарушать принцип единой ответственности.
Для демонстрации выбрал упрощенный вариант реализации похожей задачи.
"""

from typing import List, Any
from queue import Queue
from multiprocessing import Pool, Manager
from abc import ABC, abstractmethod


class TaskPoolRunner(ABC):
    def __init__(self, tasks: List):
        self._tasks = tasks
        self._queue = Manager().Queue()
        self._tasks_done = 0
        self._result = None

    def run(self) -> Any:
        with Pool() as pool:

            for task in self._tasks:
                pool.apply_async(self.process_task, (self._queue, task))

            while self._tasks_done < len(self._tasks):
                message = self._queue.get()
                self.on_message(message)

        return self._result

    @staticmethod
    @abstractmethod
    def process_task(queue: Queue, task: Any):
        pass

    @abstractmethod
    def on_message(self, message: Any):
        pass


class SquareSumTask(TaskPoolRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._result: int = 0

    @staticmethod
    def process_task(queue: Queue, task: int):
        message = task ** 2
        queue.put(message)

    def on_message(self, message: int):
        self._result += message
        self._tasks_done += 1


if __name__ == '__main__':
    ss_task = SquareSumTask(tasks=[1, 2, 3])
    result = ss_task.run()
    print(result)
