"""SQLite publisher configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from faststream.sqlite.configs import SQLiteBrokerConfig


@dataclass
class SQLitePublisherConfig:
    """Configuration for SQLite publisher."""

    queue: str
    _outer_config: "SQLiteBrokerConfig"
    headers: dict[str, Any] | None = None
