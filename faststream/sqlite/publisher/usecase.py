"""SQLite publisher usecase."""

from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import override

from faststream._internal.endpoint.publisher import (
    PublisherSpecification,
    PublisherUsecase,
)
from faststream.sqlite.response import SQLitePublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream.response import PublishCommand

    from .config import SQLitePublisherConfig


class QueuePublisher(PublisherUsecase):
    """Publisher for SQLite queue."""

    def __init__(
        self,
        config: "SQLitePublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)
        self.config = config
        self.queue = config.queue
        self.producer = self.config._outer_config.producer

    async def start(self) -> None:
        """Start the publisher."""
        await super().start()

    @classmethod
    def create(
        cls,
        queue: str,
        config: Any,
        **kwargs: Any,
    ) -> "QueuePublisher":
        """Create a queue publisher."""
        from .config import SQLitePublisherConfig

        pub_config = SQLitePublisherConfig(
            queue=queue,
            _outer_config=config,
        )

        spec = PublisherSpecification(
            **kwargs,
        )

        return cls(config=pub_config, specification=spec)

    @override
    def _make_response_cmd(
        self,
        cmd: "PublishCommand",
    ) -> SQLitePublishCommand:
        """Make response command."""
        return SQLitePublishCommand.from_cmd(cmd)
