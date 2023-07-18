"""
Данный код не относится к ТЗ, а является попыткой разделить метод main.unpack_obj_archives_to_csv
на отдельные логические части чтобы не нарушать принцип единой ответственности.
Для демонстрации выбрал упрощенный вариант реализации похожей задачи.
"""

import os
import traceback
from dataclasses import dataclass
from typing import List, Any
from queue import Queue
from multiprocessing import Pool, Manager
from abc import ABC, abstractmethod


@dataclass
class TaskResult:
    task: Any
    result: Any = None
    error: str = None
    traceback_str: str = None


class TaskExecutor(ABC):
    @classmethod
    def process_task(cls, queue: Queue, task: Any):
        task_result = TaskResult(task=task)
        try:
            task_result.result = cls.run(queue, task)
        except Exception as e:
            task_result.error = str(e)
            task_result.traceback_str = traceback.format_exc()
        queue.put(task_result)

    @classmethod
    @abstractmethod
    def run(cls, queue: Queue, task: Any):
        """Parallel execution in separate process"""
        pass

    @abstractmethod
    def on_progress(self, message: Any):
        """Sequentially executes in main process"""
        pass


class TaskRunner:
    def __init__(self, task_executor: TaskExecutor, tasks: List, skip_errors=False):
        self._task_executor = task_executor
        self._tasks = tasks
        self._skip_errors = skip_errors
        self._completed_tasks_count = 0
        self._results: List[TaskResult] = []
        self._num_processes = os.cpu_count() if len(self._tasks) > os.cpu_count() else len(self._tasks)

    def run(self) -> Any:
        with Pool(processes=self._num_processes) as pool, Manager() as manager:
            queue = manager.Queue()

            for task in self._tasks:
                pool.apply_async(self._task_executor.process_task, (queue, task))

            while self._completed_tasks_count < len(self._tasks):
                message = queue.get()

                if type(message) == TaskResult:
                    self._completed_tasks_count += 1
                    self._results.append(message)
                    if message.error and not self._skip_errors:
                        break
                else:
                    self._task_executor.on_progress(message)

        return self._results
