"""Basic SQLite broker example."""

import asyncio

from faststream.sqlite import SQLiteBroker


async def main():
    """Run a basic SQLite broker example."""
    # Create broker with an in-memory database
    broker = SQLiteBroker(database=":memory:")

    # Define a subscriber for the "messages" queue
    @broker.subscriber("messages")
    async def handle_message(msg: str):
        """Handle incoming messages."""
        print(f"Received message: {msg}")

    # Start the broker
    async with broker:
        print("Broker started, publishing messages...")
        # Publish some messages
        await broker.publish("Hello, SQLite!", queue="messages")
        print("Published message 1")
        await broker.publish("Another message", queue="messages")
        print("Published message 2")

        # Give time for messages to be processed
        print("Waiting for messages to be processed...")
        await asyncio.sleep(2)
        print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
