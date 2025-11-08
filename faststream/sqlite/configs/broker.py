"""SQLite broker configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.configs import BrokerConfig
from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from faststream.sqlite.publisher.producer import SQLiteFastProducer

    from .state import ConnectionState


@dataclass(kw_only=True)
class SQLiteBrokerConfig(BrokerConfig):
    """Configuration for SQLite broker."""

    producer: "SQLiteFastProducer"
    connection: "ConnectionState"

    async def connect(self) -> None:
        """Connect producer and database."""
        self.producer.connect(self.fd_config._serializer)
        await self.connection.connect()

    async def disconnect(self) -> None:
        """Disconnect from database."""
        await self.connection.disconnect()


@dataclass(kw_only=True)
class SQLiteRouterConfig(BrokerConfig):
    """Configuration for SQLite router."""

    @property
    def connection(self) -> ConnectionError:
        raise IncorrectState
