"""SQLite broker annotations."""

from typing import Annotated

from fast_depends import Depends

from faststream.sqlite.message import SQLiteMessage


def SQLiteMessageAnnotation(msg: SQLiteMessage = Depends()) -> SQLiteMessage:
    """SQLite message annotation helper."""
    return msg


SQLiteMessage = Annotated[SQLiteMessage, Depends(SQLiteMessageAnnotation)]

__all__ = ("SQLiteMessage",)
