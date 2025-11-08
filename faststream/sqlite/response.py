"""SQLite response types."""

from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import override

from faststream.response.publish_type import PublishType
from faststream.response.response import BatchPublishCommand, PublishCommand, Response

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage


class SQLiteResponse(Response):
    """Response for SQLite broker."""

    def __init__(
        self,
        body: Optional["SendableMessage"] = None,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
        )

    @override
    def as_publish_command(self) -> "SQLitePublishCommand":
        return SQLitePublishCommand(
            self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
            queue="fake-queue",  # will be replaced by reply-sender
        )


class SQLitePublishCommand(BatchPublishCommand):
    """Publish command for SQLite broker."""

    def __init__(
        self,
        message: "SendableMessage",
        /,
        *messages: "SendableMessage",
        _publish_type: "PublishType",
        correlation_id: str | None = None,
        queue: str | None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        timeout: float | None = 30.0,
    ) -> None:
        super().__init__(
            message,
            *messages,
            _publish_type=_publish_type,
            correlation_id=correlation_id,
            reply_to=reply_to,
            destination=queue or "",
            headers=headers,
        )

        # Request option
        self.timeout = timeout

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "SQLitePublishCommand"],
        *,
        batch: bool = False,
    ) -> "SQLitePublishCommand":
        if isinstance(cmd, SQLitePublishCommand):
            # NOTE: Should return a copy probably.
            return cmd

        body, extra_bodies = cls._parse_bodies(cmd.body, batch=batch)

        return cls(
            body,
            *extra_bodies,
            queue=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )
