import pytest
import asyncio

from aleph_client.asynchronous import get_fallback_session

from src.aars import Record, AARS

AARS(session=get_fallback_session())


@pytest.fixture(scope="session")
def event_loop():
    yield AARS.session.loop
    asyncio.run(AARS.session.close())


class Book(Record):
    title: str
    author: str


@pytest.mark.asyncio
async def test_fetch_all():
    books = await Book.fetch_all()
    print(len(books))
    assert len(books) > 0
    for book in books:
        assert isinstance(book, Book)
