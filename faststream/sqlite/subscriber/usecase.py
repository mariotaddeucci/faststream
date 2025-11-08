"""SQLite subscriber usecase."""

import asyncio
from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import override

from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream.middlewares import AckPolicy
from faststream.sqlite.message import SQLiteRawMessage

if TYPE_CHECKING:
    import aiosqlite

    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.message import StreamMessage as BrokerStreamMessage

    from .config import SQLiteSubscriberConfig


class QueueSubscriber(SubscriberUsecase):
    """Subscriber for SQLite queue."""

    def __init__(
        self,
        config: "SQLiteSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        # Set parser and decoder before calling super().__init__
        from faststream.sqlite.parser import SQLiteParser, SimpleParserConfig

        parser = SQLiteParser(SimpleParserConfig())
        config.parser = parser.parse_message
        config.decoder = parser.decode_message

        super().__init__(config, specification, calls)
        self.config = config
        self.queue = config.queue
        self._consumer_task: asyncio.Task[None] | None = None

    @classmethod
    def create(
        cls,
        queue: str,
        extra_context: dict[str, Any],
        config: Any,
        **kwargs: Any,
    ) -> "QueueSubscriber":
        """Create a queue subscriber."""
        from faststream._internal.endpoint.subscriber.call_item import CallsCollection

        from .config import SQLiteSubscriberConfig, SQLiteSubscriberSpecificationConfig

        sub_config = SQLiteSubscriberConfig(
            queue=queue,
            _outer_config=config,
        )

        spec_config = SQLiteSubscriberSpecificationConfig(
            title_=kwargs.get("title"),
            description_=kwargs.get("description"),
            include_in_schema=kwargs.get("include_in_schema", True),
        )

        calls = CallsCollection[Any]()

        spec = SubscriberSpecification(
            _outer_config=config,
            specification_config=spec_config,
            calls=calls,
        )

        return cls(config=sub_config, specification=spec, calls=calls)

    @override
    async def start(self, *args: Any) -> None:
        """Start consuming messages from the queue."""
        await super().start()
        self._consumer_task = asyncio.create_task(self._consume_loop())

    @override
    async def stop(self) -> None:
        """Stop consuming messages."""
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        await super().stop()

    async def _consume_loop(self) -> None:
        """Main consume loop."""
        from faststream.sqlite.configs import ConnectionState

        connection: ConnectionState = self.config._outer_config.connection

        print(f"[DEBUG] Consumer loop started for queue: {self.queue}")
        
        while True:
            try:
                db: aiosqlite.Connection = connection.client
                print(f"[DEBUG] Got DB connection")

                # Ensure queue table exists
                await db.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.queue} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data BLOB NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                await db.commit()
                print(f"[DEBUG] Table created/checked")

                # Poll for messages
                async with db.execute(
                    f"SELECT id, data FROM {self.queue} ORDER BY id LIMIT 1"
                ) as cursor:
                    row = await cursor.fetchone()
                    print(f"[DEBUG] Query result: {row}")

                if row:
                    message_id, data = row
                    print(f"[DEBUG] Processing message {message_id}")
                    msg: SQLiteRawMessage = {
                        "type": "queue",
                        "queue": self.queue,
                        "message_id": message_id,
                        "data": data,
                    }
                    
                    try:
                        result = await self.consume(msg)
                        print(f"[DEBUG] Consume returned: {result}")
                    except Exception as e:
                        print(f"[DEBUG] Consume error: {e}")
                        import traceback
                        traceback.print_exc()
                        raise
                    
                    print(f"[DEBUG] Message consumed")
                    
                    # Delete the message from the queue after processing
                    await db.execute(
                        f"DELETE FROM {self.queue} WHERE id = ?",
                        (message_id,),
                    )
                    await db.commit()
                    print(f"[DEBUG] Message deleted")
                else:
                    # No messages, wait a bit before polling again
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                print(f"[DEBUG] Consumer loop cancelled")
                raise
            except Exception as e:
                # Log error and continue
                print(f"[DEBUG] Error in consumer loop: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(1)

    def get_log_context(
        self,
        message: Optional["BrokerStreamMessage[Any]"],
    ) -> dict[str, Any]:
        """Get logging context."""
        return {
            "queue": self.queue,
            "message_id": message.raw_message["message_id"] if message else "",
        }
