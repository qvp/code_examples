"""
Данный код не относится к ТЗ, а является попыткой разделить метод main.unpack_obj_archives_to_csv
на отдельные логические части чтобы не нарушать принцип единой ответственности.
Для демонстрации выбрал упрощенный вариант реализации похожей задачи.
"""

import io
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
        self._queue = Manager().Queue()
        self._skip_errors = skip_errors
        self._completed_tasks_count = 0
        self._results: List[TaskResult] = []
        self._num_processes = os.cpu_count() if len(self._tasks) > os.cpu_count() else len(self._tasks)

    def run(self) -> Any:
        with Pool(processes=self._num_processes) as pool:

            for task in self._tasks:
                pool.apply_async(self._task_executor.process_task, (self._queue, task))

            while self._completed_tasks_count < len(self._tasks):
                message = self._queue.get()

                if type(message) == TaskResult:
                    self._completed_tasks_count += 1
                    self._results.append(message)
                    if message.error and not self._skip_errors:
                        break
                    continue

                self._task_executor.on_progress(message)
        return self._results


class CreateFileParallelTask(TaskExecutor):
    def __init__(self, file):
        self._file = file

    @classmethod
    def run(cls, queue: Queue, task: int):
        if task == 2:
            raise Exception('Just not 2!')
        pid = os.getpid()
        queue.put(f'Task: {task}, process id: {pid}\r\n')

    def on_progress(self, message: str):
        self._file.write(message)


if __name__ == '__main__':
    with io.StringIO() as result_file:
        ss_task = CreateFileParallelTask(result_file)
        runner = TaskRunner(task_executor=ss_task, tasks=[1, 2, 3, 4, 5], skip_errors=True)
        results = runner.run()
        print('TaskResult objects:', results)
        print('File result:')
        print(result_file.getvalue())
