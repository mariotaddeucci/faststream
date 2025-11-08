"""SQLite broker registrator."""

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.sqlite.configs import SQLiteBrokerConfig
from faststream.sqlite.message import SQLiteRawMessage
from faststream.sqlite.subscriber.usecase import QueueSubscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )


class SQLiteRegistrator(Registrator[SQLiteRawMessage, SQLiteBrokerConfig]):
    """Registrator for SQLite broker."""

    def subscriber(
        self,
        queue: str,
        *,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        no_ack: bool = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "QueueSubscriber":
        """Create a subscriber for a SQLite queue."""
        subscriber = QueueSubscriber.create(
            queue=queue,
            extra_context={},
            config=self.config,
            # subscriber args
            ack_policy=ack_policy,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            # AsyncAPI args
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )

        self._subscribers.append(subscriber)
        return subscriber

    @override
    def publisher(
        self,
        queue: str,
        *,
        # broker arguments
        middlewares: Sequence["PublisherMiddleware"] = (),
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> Any:
        """Create a publisher for a SQLite queue."""
        # Simple publisher - will be implemented later
        from faststream.sqlite.publisher.usecase import QueuePublisher

        publisher = QueuePublisher.create(
            queue=queue,
            config=self.config,
            # AsyncAPI args
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )

        self._publishers.append(publisher)
        return publisher
