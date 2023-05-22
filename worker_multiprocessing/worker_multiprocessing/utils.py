import socket


def create_worker_name():
    """Создает имя воркера на основе параметров машины на которой он запущен."""
    return socket.gethostname()
