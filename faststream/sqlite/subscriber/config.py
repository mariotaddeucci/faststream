"""SQLite subscriber configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream.sqlite.configs import SQLiteBrokerConfig


class SQLiteSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    """Specification config for SQLite subscriber."""

    pass


@dataclass(kw_only=True)
class SQLiteSubscriberConfig(SubscriberUsecaseConfig):
    """Configuration for SQLite subscriber."""

    _outer_config: "SQLiteBrokerConfig"
    queue: str

    @property
    def ack_policy(self) -> AckPolicy:
        """Get ack policy."""
        if self._ack_policy is EMPTY:
            return AckPolicy.REJECT_ON_ERROR
        return self._ack_policy
