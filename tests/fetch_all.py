import pytest
import asyncio

from aleph.sdk import AuthenticatedAlephClient
from aleph.sdk.chains.ethereum import get_fallback_account
from aleph.sdk.conf import settings

from src.aars import Record, AARS

AARS(session=AuthenticatedAlephClient(get_fallback_account(), settings.API_HOST))


@pytest.fixture(scope="session")
def event_loop():
    yield AARS.session.loop
    asyncio.run(AARS.session.close())


class Book(Record):
    title: str
    author: str


@pytest.mark.asyncio
async def test_fetch_all():
    books = await Book.fetch_objects()
    print(len(books))
    assert len(books) > 0
    for book in books:
        assert isinstance(book, Book)
