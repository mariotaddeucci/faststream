"""SQLite message types."""

from typing import TYPE_CHECKING, Literal, TypedDict

from typing_extensions import override

from faststream.message import StreamMessage as BrokerStreamMessage

if TYPE_CHECKING:
    import aiosqlite


class SQLiteRawMessage(TypedDict):
    """Raw SQLite message structure."""

    type: Literal["queue"]
    queue: str
    message_id: int
    data: bytes


class SQLiteMessage(BrokerStreamMessage[SQLiteRawMessage]):
    """StreamMessage for SQLite queue message."""

    @override
    async def ack(
        self,
        db: "aiosqlite.Connection | None" = None,
    ) -> None:
        """Acknowledge the message by deleting it from the queue."""
        if not self.committed and db is not None:
            message_id = self.raw_message["message_id"]
            queue = self.raw_message["queue"]
            await db.execute(
                f"DELETE FROM {queue} WHERE id = ?",
                (message_id,),
            )
            await db.commit()
        await super().ack()

    @override
    async def nack(
        self,
        db: "aiosqlite.Connection | None" = None,
    ) -> None:
        """Negative acknowledgment - keep message in queue."""
        await super().nack()

    @override
    async def reject(
        self,
        db: "aiosqlite.Connection | None" = None,
    ) -> None:
        """Reject the message - could implement dead letter queue."""
        await super().reject()
