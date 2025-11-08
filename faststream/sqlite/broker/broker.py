"""SQLite broker implementation."""

import logging
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from fast_depends import Provider, dependency_provider
from typing_extensions import override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.sqlite.configs import ConnectionState, SQLiteBrokerConfig
from faststream.sqlite.message import SQLiteRawMessage
from faststream.sqlite.publisher.producer import SQLiteFastProducer
from faststream.sqlite.response import SQLitePublishCommand
from faststream.specification.schema import BrokerSpec

from .logging import make_sqlite_logger_state
from .registrator import SQLiteRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    import aiosqlite
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.types import BrokerMiddleware, CustomCallable


class SQLiteBroker(
    SQLiteRegistrator,
    BrokerUsecase[SQLiteRawMessage, "aiosqlite.Connection"],
):
    """SQLite broker for FastStream."""

    def __init__(
        self,
        database: str = ":memory:",
        *,
        # broker base args
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[SQLiteRegistrator] = (),
        # AsyncAPI args
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "custom",
        description: str | None = None,
        tags: Iterable[Any] = (),
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        """Initialize SQLite broker.

        Args:
            database: Path to SQLite database file or ":memory:" for in-memory database
            graceful_timeout: Graceful shutdown timeout
            decoder: Custom decoder callable
            parser: Custom parser callable
            dependencies: Dependencies to apply to all subscribers
            middlewares: Middlewares to apply to all publishers/subscribers
            routers: Routers to include
            specification_url: AsyncAPI specification URL
            protocol: AsyncAPI protocol
            protocol_version: AsyncAPI protocol version
            description: Broker description
            tags: AsyncAPI tags
            logger: Custom logger
            log_level: Logging level
            apply_types: Whether to use FastDepends
            serializer: Custom serializer
            provider: FastDepends provider
            context: Context repository
        """
        if specification_url is None:
            specification_url = f"sqlite://{database}"

        if protocol is None:
            protocol = "sqlite"

        connection_options = {"database": database}
        connection_state = ConnectionState(connection_options)

        super().__init__(
            routers=routers,
            config=SQLiteBrokerConfig(
                connection=connection_state,
                producer=SQLiteFastProducer(
                    connection=connection_state,
                    parser=parser,
                    decoder=decoder,
                    serializer=serializer,
                ),
                # both args
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                logger=make_sqlite_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                # subscriber args
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                extra_context={
                    "broker": self,
                },
            ),
            specification=BrokerSpec(
                description=description,
                url=[specification_url],
                protocol=protocol,
                protocol_version=protocol_version,
                security=None,
                tags=tags,
            ),
        )

    @override
    async def _connect(self) -> "aiosqlite.Connection":
        """Connect to SQLite database."""
        await self.config.connect()
        return self.config.broker_config.connection.client

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        """Stop the broker."""
        await super().stop(exc_type, exc_val, exc_tb)
        await self.config.disconnect()
        self._connection = None

    async def start(self) -> None:
        """Start the broker."""
        await self.connect()
        await super().start()

    async def publish(
        self,
        message: "SendableMessage" = None,
        queue: str | None = None,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> int:
        """Publish a message to a SQLite queue.

        Args:
            message: Message to publish
            queue: Queue name
            headers: Message headers
            correlation_id: Correlation ID

        Returns:
            Message ID
        """
        cmd = SQLitePublishCommand.from_cmd(
            SQLitePublishCommand(
                message,
                queue=queue,
                headers=headers,
                correlation_id=correlation_id,
                _publish_type=self._producer.publish_type,
            )
        )

        return await self._producer.publish(cmd)

    @override
    async def ping(self, timeout: float | None = None) -> bool:
        """Check if database connection is alive."""
        try:
            cursor = await self._connection.execute("SELECT 1")
            await cursor.close()
            return True
        except Exception:
            return False
