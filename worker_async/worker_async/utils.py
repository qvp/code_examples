import traceback
from multiprocessing import Process, Manager

from . import schema


class TaskRunner(Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        mem_manager = Manager()
        self._calc_result = mem_manager.list()

    def run(self) -> None:
        try:
            self._calc_result.append(self._target(*self._args, **self._kwargs))  # noqa
        except Exception as error:
            task_error = schema.TaskError(
                message=str(error),
                traceback=traceback.format_exc(),
            )
            self._calc_result.append(task_error)

    def result(self):
        if len(self._calc_result):
            return self._calc_result[0]
