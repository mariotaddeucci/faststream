"""Testing utilities for SQLite broker."""

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast
from unittest.mock import AsyncMock, MagicMock

from typing_extensions import override

from faststream._internal.testing.broker import TestBroker, change_producer
from faststream.exceptions import SubscriberNotFound
from faststream.sqlite.broker.broker import SQLiteBroker
from faststream.sqlite.publisher.producer import SQLiteFastProducer
from faststream.sqlite.response import SQLitePublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream.sqlite.publisher.usecase import QueuePublisher
    from faststream.sqlite.subscriber.usecase import QueueSubscriber

__all__ = ("TestSQLiteBroker",)


class TestSQLiteBroker(TestBroker[SQLiteBroker]):
    """Test client for SQLite broker."""

    @contextmanager
    def _patch_producer(self, broker: SQLiteBroker) -> Iterator[None]:
        """Patch producer for testing."""
        with ExitStack() as es:
            es.enter_context(
                change_producer(
                    broker.config.broker_config, FakeProducer(broker, broker.config)
                ),
            )

            for publisher in cast("list[QueuePublisher]", broker.publishers):
                es.enter_context(
                    change_producer(publisher, FakeProducer(broker, publisher.config)),
                )

            yield

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: SQLiteBroker,
        publisher: "QueuePublisher",
    ) -> tuple["QueueSubscriber", bool]:
        """Create a fake subscriber for publisher."""
        sub: QueueSubscriber | None = None

        # Find matching subscriber
        for handler in broker.subscribers:
            handler = cast("QueueSubscriber", handler)
            if handler.queue == publisher.queue:
                sub = handler
                break

        if sub is None:
            is_real = False
            sub = broker.subscriber(queue=publisher.queue)
        else:
            is_real = True

        return sub, is_real

    @staticmethod
    async def _fake_connect(
        broker: SQLiteBroker,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncMock:
        """Mock connection for testing."""
        connection = AsyncMock()
        connection.execute = AsyncMock()
        connection.commit = AsyncMock()
        connection.close = AsyncMock()
        return connection


class FakeProducer(SQLiteFastProducer):
    """Fake producer for testing."""

    def __init__(self, broker: SQLiteBroker, config: Any) -> None:
        self.broker = broker
        self._config = config

    @override
    async def publish(self, cmd: "SQLitePublishCommand") -> int:
        """Publish message to subscribers in test mode."""
        # Find subscriber for this queue
        for sub in cast("list[QueueSubscriber]", self.broker.subscribers):
            if sub.queue == cmd.destination:
                # Create a message to consume
                msg = {
                    "type": "queue",
                    "queue": cmd.destination,
                    "message_id": 1,
                    "data": cmd.body if isinstance(cmd.body, bytes) else str(cmd.body).encode(),
                }
                await sub.consume(msg)
                return 1

        raise SubscriberNotFound(f"No subscriber found for queue: {cmd.destination}")
