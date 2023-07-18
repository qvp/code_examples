import csv
from queue import Queue
from typing import TextIO
from pathlib import Path

from utils.tasks import TaskExecutor, TaskRunner
from schemas import Obj
from serializers import ObjXmlSerializer
from utils.zip import read_zip_archive, dir_zip_files
from generate_data import create_archives


class UnzipXmlToCsvTask(TaskExecutor):
    def __init__(self, id_level_file: TextIO, id_object_name_file: TextIO, csv_delimiter=','):
        self._id_level_writer = csv.writer(id_level_file, delimiter=csv_delimiter)
        self._id_object_name_writer = csv.writer(id_object_name_file, delimiter=csv_delimiter)

    @classmethod
    def run(cls, queue: Queue, zip_file_path: Path | str):
        """Читает файлы из zip-архива и отправляет их в очередь"""
        serializer = ObjXmlSerializer()
        for _, file_content in read_zip_archive(zip_file_path):
            obj: Obj = serializer.deserialize(file_content)
            queue.put(obj)

    def on_progress(self, obj: Obj):
        """"""
        self._id_level_writer.writerow([obj.vars['id'], obj.vars['level']])
        self._id_object_name_writer.writerows([(obj.vars['id'], o) for o in obj.objects])


def unzip_archives(archives_dir_path: Path, id_level_file_path: Path, id_object_name_file_path: Path):
    with (open(id_level_file_path, 'a', newline='') as id_level_file,
          open(id_object_name_file_path, 'a', newline='') as id_object_name_file):

        tasks = list(dir_zip_files(archives_dir_path))
        task_executor = UnzipXmlToCsvTask(id_level_file, id_object_name_file)
        task_runner = TaskRunner(task_executor, tasks)
        task_runner.run()


if __name__ == '__main__':
    archives_dir = Path().absolute() / 'archives'
    results_dir = Path().absolute() / 'results'

    create_archives(archives_dir, archives_count=50, files_per_archive=100)

    unzip_archives(
        archives_dir_path=archives_dir,
        id_level_file_path=results_dir / 'id_level.csv',
        id_object_name_file_path=results_dir / 'id_object_name.csv',
    )
