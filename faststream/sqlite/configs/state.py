"""SQLite connection state management."""

from typing import TYPE_CHECKING, Any

from faststream.__about__ import __version__
from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    import aiosqlite


class ConnectionState:
    """Manages SQLite database connection state."""

    def __init__(self, options: dict[str, Any] | None = None) -> None:
        self._options = options or {}
        self._connected = False
        self._client: "aiosqlite.Connection | None" = None

    @property
    def client(self) -> "aiosqlite.Connection":
        """Get the active database connection."""
        if not self._client:
            msg = "Connection is not available yet. Please, connect the broker first."
            raise IncorrectState(msg)
        return self._client

    def __bool__(self) -> bool:
        return self._connected

    async def connect(self) -> "aiosqlite.Connection":
        """Establish connection to SQLite database."""
        import aiosqlite

        database_path = self._options.get("database", ":memory:")
        
        client = await aiosqlite.connect(database_path)
        # Enable WAL mode for better concurrency
        await client.execute("PRAGMA journal_mode=WAL")
        await client.commit()

        self._client = client
        self._connected = True

        return client

    async def disconnect(self) -> None:
        """Close the database connection."""
        if self._client:
            await self._client.close()

        self._client = None
        self._connected = False
