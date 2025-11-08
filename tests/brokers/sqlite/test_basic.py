"""Basic SQLite broker tests."""

import pytest

from faststream.sqlite import SQLiteBroker, TestSQLiteBroker


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_broker_connection():
    """Test basic broker connection."""
    broker = SQLiteBroker()
    
    async with broker:
        assert broker._connection is not None
        assert await broker.ping()


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_publish_consume():
    """Test basic publish and consume."""
    broker = SQLiteBroker()
    
    messages_received = []
    
    @broker.subscriber("test_queue")
    async def handler(msg: str):
        messages_received.append(msg)
    
    async with TestSQLiteBroker(broker) as br:
        await br.publish("Hello, SQLite!", queue="test_queue")
        
    assert len(messages_received) == 1
    assert messages_received[0] == "Hello, SQLite!"


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_multiple_messages():
    """Test publishing multiple messages."""
    broker = SQLiteBroker()
    
    messages_received = []
    
    @broker.subscriber("test_queue")
    async def handler(msg: str):
        messages_received.append(msg)
    
    async with TestSQLiteBroker(broker) as br:
        await br.publish("Message 1", queue="test_queue")
        await br.publish("Message 2", queue="test_queue")
        await br.publish("Message 3", queue="test_queue")
        
    assert len(messages_received) == 3
    assert messages_received == ["Message 1", "Message 2", "Message 3"]


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_file_database():
    """Test using file-based database."""
    import tempfile
    import os
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        broker = SQLiteBroker(database=db_path)
        
        async with broker:
            assert await broker.ping()
            assert os.path.exists(db_path)
