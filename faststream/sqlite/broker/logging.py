"""SQLite broker logging configuration."""

import logging
from functools import partial
from typing import TYPE_CHECKING, Any

from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.logger.logging import get_broker_logger

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.context import ContextRepo


class SQLiteParamsStorage(DefaultLoggerStorage):
    """Storage for SQLite logger parameters."""

    def __init__(self) -> None:
        super().__init__()
        self._max_queue_name = 4
        self.logger_log_level = logging.INFO

    def set_level(self, level: int) -> None:
        """Set the logger level."""
        self.logger_log_level = level

    def register_subscriber(self, params: dict[str, Any]) -> None:
        """Register subscriber parameters for logging."""
        self._max_queue_name = max(
            (
                self._max_queue_name,
                len(params.get("queue", "")),
            ),
        )

    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        """Get or create the logger."""
        message_id_ln = 10

        if not (lg := self._get_logger_ref()):
            lg = get_broker_logger(
                name="sqlite",
                default_context={
                    "queue": "",
                },
                message_id_ln=message_id_ln,
                fmt=(
                    "%(asctime)s %(levelname)-8s - "
                    f"%(queue)-{self._max_queue_name}s | "
                    f"%(message_id)-{message_id_ln}s "
                    "- %(message)s"
                ),
                context=context,
                log_level=self.logger_log_level,
            )
            self._logger_ref.add(lg)

        return lg


make_sqlite_logger_state = partial(
    make_logger_state,
    default_storage_cls=SQLiteParamsStorage,
)
