"""SQLite subscriber configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from faststream.sqlite.configs import SQLiteBrokerConfig


@dataclass
class SQLiteSubscriberConfig:
    """Configuration for SQLite subscriber."""

    queue: str
    _outer_config: "SQLiteBrokerConfig"
