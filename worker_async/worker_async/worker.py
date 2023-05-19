import asyncio
from asyncio import Queue
from typing import List

import aiormq

from .consumer import BaseConsumer


class Worker:
    def __init__(self, connection_url: str, consumers: List[BaseConsumer]):
        self._connection_url = connection_url
        self._consumers = consumers

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._start_consumers())
        loop.run_forever()

    async def _start_consumers(self):
        command_queue = Queue()
        connection = await aiormq.connect(self._connection_url)

        for consumer in self._consumers:
            await consumer.start_consume(connection, command_queue)
