import os
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED, is_zipfile
from typing import Iterable, Tuple, Generator, TypeVar

AnyStr = TypeVar('AnyStr', bytes, str)


def create_zip_archive(file_path: Path | str, files: Iterable[Tuple[str, AnyStr]]) -> None:
    with ZipFile(file_path, 'w', ZIP_DEFLATED) as zf:
        for file_name, content in files:
            zf.writestr(file_name, content)


def read_zip_archive(file_path: Path | str) -> Generator[Tuple[str, AnyStr], None, None]:
    with ZipFile(file_path, 'r') as zf:
        for file_name in zf.namelist():
            with zf.open(file_name) as file:
                yield file_name, file.read()


def dir_zip_files(path: Path) -> Generator[str, None, None]:
    for file_name in os.listdir(path):
        file_path = path / str(file_name)
        if is_zipfile(file_path):
            yield file_path
