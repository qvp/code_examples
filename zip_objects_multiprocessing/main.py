import os
import csv
import traceback
from pathlib import Path
from multiprocessing import Pool, Manager
from timeit import default_timer as timer

from schemas import Obj, QueueMessage, QueueMessageType
from serializers import ObjXmlSerializer
from utils.zip import read_zip_archive, dir_zip_files
from generate_data import create_archives


CSV_DELIMITER = ','
archives_dir = Path().absolute() / 'archives'
results_dir = Path().absolute() / 'results'


def read_archive_objects(q, archive_path: Path | str) -> None:
    """Читает файлы из zip-архива и отправляет в очередь на обработку"""
    try:
        serializer = ObjXmlSerializer()
        for _, file_content in read_zip_archive(archive_path):
            obj: Obj = serializer.deserialize(file_content)
            new_obj = QueueMessage(
                type=QueueMessageType.NEW_OBJ,
                payload=obj,
            )
            q.put(new_obj)

        archive_processed = QueueMessage(
            type=QueueMessageType.ARCHIVE_PROCESSED,
            payload={'archive_path': archive_path},
        )
        q.put(archive_processed)

    except Exception as e:
        error = QueueMessage(
            type=QueueMessageType.ERROR,
            payload={
                'message': str(e),
                'traceback': traceback.format_exc(),
            }
        )
        q.put(error)


def unpack_obj_archives_to_csv(id_level_file_path: Path | str, id_object_name_file_path: Path | str,
                               processes_count: int = os.cpu_count(), skip_errors: bool = False):
    """Параллельно распаковывает zip-архивы и записывает содержимое в csv-файлы"""
    start_time = timer()

    with (open(id_level_file_path, 'a', newline='') as csv_id_level,
          open(id_object_name_file_path, 'a', newline='') as id_object_name):

        csv_id_level_writer = csv.writer(csv_id_level, delimiter=CSV_DELIMITER)
        csv_id_object_name_writer = csv.writer(id_object_name, delimiter=CSV_DELIMITER)

        with Pool(processes=processes_count) as pool:
            obj_queue = Manager().Queue()

            archives = list(dir_zip_files(archives_dir))
            archives_count = len(archives)
            for archive_patch in archives:
                pool.apply_async(read_archive_objects, (obj_queue, archive_patch))

            archives_processed = 0
            while archives_processed < archives_count:
                queue_msg: QueueMessage = obj_queue.get()

                # Если получен новый объект - обрабатываем его
                if queue_msg.type == QueueMessageType.NEW_OBJ:
                    obj: Obj = queue_msg.payload
                    csv_id_level_writer.writerow([obj.vars['id'], obj.vars['level']])
                    csv_id_object_name_writer.writerows([(obj.vars['id'], o) for o in obj.objects])

                # Если архив целиком обработан
                # счетчик обработанных архивов увеличивается на 1
                if queue_msg.type == QueueMessageType.ARCHIVE_PROCESSED:
                    archives_processed += 1

                # Если при обработке одного из архивов произошла ошибка
                # останавливаем обработку или продолжаем с другими архивами в зависимости от skip_errors
                # счетчик обработанных архивов увеличивается на 1
                if queue_msg.type == QueueMessageType.ERROR:
                    archives_processed += 1
                    if not skip_errors:
                        pool.terminate()
                        print('Process of archives terminated after error')
                        exit(1)

            # Все архивы обработаны
            work_seconds = round(timer() - start_time, 3)
            print(f'{archives_count} archives processed in {work_seconds} seconds '
                  f'by {processes_count} pool processes')


if __name__ == '__main__':
    create_archives(archives_dir, archives_count=50, files_per_archive=100)

    unpack_obj_archives_to_csv(
        id_level_file_path=results_dir / 'id_level.csv',
        id_object_name_file_path=results_dir / 'id_object_name.csv',
        skip_errors=True,
    )
