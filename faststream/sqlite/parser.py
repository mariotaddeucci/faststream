"""SQLite message parser."""

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from faststream.message import decode_message, gen_cor_id
from faststream.sqlite.message import SQLiteRawMessage

if TYPE_CHECKING:
    from faststream._internal.basic_types import DecodedMessage


@dataclass
class SimpleParserConfig:
    """Simple parser configuration."""

    pass


class SQLiteParser:
    """Parser for SQLite messages."""

    def __init__(self, config: SimpleParserConfig) -> None:
        self.config = config

    async def parse_message(self, message: SQLiteRawMessage) -> SQLiteRawMessage:
        """Parse raw SQLite message."""
        return message

    async def decode_message(self, msg: SQLiteRawMessage) -> "DecodedMessage":
        """Decode SQLite message body."""
        # Deserialize the message data
        try:
            data = json.loads(msg["data"])
        except (json.JSONDecodeError, TypeError):
            data = {"body": msg["data"], "headers": {}, "reply_to": "", "correlation_id": ""}

        body = data.get("body", msg["data"])
        headers = data.get("headers", {})
        
        return decode_message(
            message=body,
            headers=headers,
        )
