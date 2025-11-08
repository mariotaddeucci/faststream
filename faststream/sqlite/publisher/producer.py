"""SQLite producer implementation."""

from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream.sqlite.parser import SQLiteParser, SimpleParserConfig
from faststream.sqlite.response import SQLitePublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.types import CustomCallable
    from faststream.sqlite.configs import ConnectionState


class SQLiteFastProducer(ProducerProto[SQLitePublishCommand]):
    """SQLite message producer."""

    _decoder: ParserComposition
    _parser: ParserComposition

    def __init__(
        self,
        connection: "ConnectionState",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        serializer: Optional["SerializerProto"],
    ) -> None:
        self._connection = connection

        default = SQLiteParser(SimpleParserConfig())
        self._parser = ParserComposition(
            parser,
            default.parse_message,
        )
        self._decoder = ParserComposition(
            decoder,
            default.decode_message,
        )
        self.serializer = serializer

    def connect(self, serializer: Optional["SerializerProto"]) -> None:
        """Set the serializer for the producer."""
        self.serializer = serializer

    @override
    async def publish(self, cmd: "SQLitePublishCommand") -> int:
        """Publish a message to a SQLite queue."""
        import json

        # Simple serialization - just convert body to JSON bytes
        try:
            if isinstance(cmd.body, bytes):
                body_bytes = cmd.body
            elif isinstance(cmd.body, str):
                body_bytes = cmd.body.encode()
            else:
                # Serialize to JSON
                body_bytes = json.dumps(cmd.body).encode()
        except (TypeError, ValueError):
            # If JSON serialization fails, convert to string
            body_bytes = str(cmd.body).encode()

        # Ensure queue table exists
        queue = cmd.destination
        await self._connection.client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {queue} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await self._connection.client.commit()

        # Insert into queue table
        cursor = await self._connection.client.execute(
            f"INSERT INTO {queue} (data) VALUES (?)",
            (body_bytes,),
        )
        await self._connection.client.commit()

        return cursor.lastrowid or 0

    @override
    async def publish_batch(
        self,
        cmd: "SQLitePublishCommand",
    ) -> int:
        """Publish multiple messages - just delegates to publish."""
        return await self.publish(cmd)

    @override
    async def request(self, cmd: "SQLitePublishCommand") -> Any:
        """Request/response pattern not implemented for SQLite."""
        raise NotImplementedError("Request/response pattern is not supported for SQLite broker")
