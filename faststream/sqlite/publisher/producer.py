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

    @override
    async def publish(self, cmd: "SQLitePublishCommand") -> int:
        """Publish a message to a SQLite queue."""
        import json

        msg = {
            "body": cmd.body,
            "headers": cmd.headers or {},
            "reply_to": cmd.reply_to or "",
            "correlation_id": cmd.correlation_id or "",
        }

        # Serialize the message
        if self.serializer:
            body_bytes = self.serializer.serialize(msg)
        else:
            body_bytes = json.dumps(msg).encode()

        # Insert into queue table
        queue = cmd.destination
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
