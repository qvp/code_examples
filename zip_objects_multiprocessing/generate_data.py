from uuid import uuid4
from pathlib import Path
from random import randint
from typing import Generator, Tuple

from schemas import Obj
from serializers import ObjXmlSerializer
from utils.zip import create_zip_archive


def generate_obj() -> Obj:
    """Генерирует объект"""
    vars_ = {
        'id': str(uuid4()),
        'level': str(randint(1, 100)),
    }
    objects = [str(uuid4()) for _ in range(randint(1, 10))]

    return Obj(
        vars=vars_,
        objects=objects,
    )


def generate_xml_files(files_count: int) -> Generator[Tuple[str, str], None, None]:
    """Генерирует заданное количество xml-файлов"""
    serializer = ObjXmlSerializer()

    for i in range(files_count):
        obj = generate_obj()
        obj_xml = serializer.serialize(obj)
        file_name = f'file_{i}.xml'

        yield file_name, obj_xml


def create_archives(path: Path, archives_count: int, files_per_archive: int):
    """Создает zip-архивы с xml-файлами внутри"""
    for i in range(1, archives_count + 1):
        archive_name = f'xml_objects_{i}.zip'
        create_zip_archive(path / archive_name, generate_xml_files(files_per_archive))
