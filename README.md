### Примеры кода

[worker_asyncio](https://github.com/qvp/code_examples/tree/main/worker_asyncio) (2023)  
Асинхронный воркер, построен на asyncio, aiormq, multiprocessing, docker.  
Представляет из себя слегка упрощенную версию worker_multiprocessing.  
Сделано для сравнения разных подходов к прослушиванию нескольких очередей.

[worker_multiprocessing](https://github.com/qvp/code_examples/tree/main/worker_multiprocessing) (2020)  
Воркер, основанный на процессах. Построен на multiprocessing, pika, RabbitMQ, Docker  
Более комплексная версия воркера с отслеживанием прогресса.  
Сделано на основе прод. версии.

[aiochorm](https://github.com/qvp/aiochorm) (2019)  
Асинхронная ORM для ClickHouse.  
Сделана в рамках работы над проектом Atom Secure.  
