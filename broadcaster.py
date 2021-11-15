from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Set, Optional
from asyncio import Queue

from loguru import logger


@dataclass
class Event:
    channel: str
    message: Any


class Unsubscribed(Exception):
    pass


@dataclass
class Subscriber:
    queue: "Queue[Optional[Event]]" = field(default_factory=Queue)

    async def __aiter__(self):
        try:
            while True:
                item = await self.get()
                yield item
        except Unsubscribed:
            pass
        logger.info("unsubscribed")

    async def get(self) -> Event:
        item = await self.queue.get()
        if item is None:
            raise Unsubscribed()
        return item


@dataclass
class Broadcast:
    subscribers: Dict[str, Set[Queue]] = field(default_factory=dict)

    @asynccontextmanager
    async def subscribe(self, channel: str) -> Subscriber:
        subscriber = Subscriber()
        subscribers_set = self.subscribers.get(channel, set())
        subscribers_set.add(subscriber.queue)
        self.subscribers[channel] = subscribers_set
        yield subscriber
        # Remove subscriber on context close
        self.subscribers[channel].remove(subscriber.queue)
        if not self.subscribers[channel]:
            del self.subscribers[channel]
        await subscriber.queue.put(None)
        logger.info("unsubscribed")

    async def publish(self, channel: str, message: Any):
        event = Event(channel=channel, message=message)
        for subscriber_queue in self.subscribers.get(channel, set()):
            await subscriber_queue.put(event)
