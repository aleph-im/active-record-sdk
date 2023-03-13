import asyncio
from typing import List, Optional

from aleph.sdk import AuthenticatedAlephClient
from aleph.sdk.conf import settings
from aleph.sdk.chains.ethereum import get_fallback_account

from src.aars import Record, Index, AARS
from src.aars.exceptions import AlreadyForgottenError
import pytest

AARS(session=AuthenticatedAlephClient(get_fallback_account(), settings.API_HOST))


@pytest.fixture(scope="session")
def event_loop():
    yield AARS.session.http_session.loop
    asyncio.run(AARS.session.http_session.close())


@pytest.fixture(scope="session", autouse=True)
def create_indices(request):
    Index(Book, 'title')
    Index(Book, ['title', 'author'])
    Index(Library, on='name')


class Book(Record):
    title: str
    author: str
    year: Optional[int]


class Library(Record):
    name: str
    books: List[Book]


def test_invalid_index_created():
    try:
        index = None
        with pytest.raises(ValueError):
            index = Index(Book, 'some_nonexistent_field')
    finally:
        if index:
            Record.remove_index(index)


@pytest.mark.asyncio
async def test_store_and_index():
    new_book = await Book(title='Atlas Shrugged', author='Ayn Rand').save()
    assert new_book.title == 'Atlas Shrugged'
    assert new_book.author == 'Ayn Rand'
    await asyncio.sleep(1)
    fetched_book = await Book.where_eq(title='Atlas Shrugged').first()
    print(fetched_book)
    print(new_book)
    assert new_book == fetched_book


@pytest.mark.asyncio
async def test_multi_index():
    new_book = await Book(title='Lila', author='Robert M. Pirsig', year=1991).save()
    # wait a few secs
    await asyncio.sleep(1)
    should_be_none = await Book.where_eq(title='Lila', author='Yo Momma').all()
    assert len(should_be_none) == 0
    fetched_book = await Book.where_eq(title='Lila', author='Robert M. Pirsig').first()
    assert new_book == fetched_book


@pytest.mark.asyncio
async def test_amending_record():
    book = await Book(title='Neurodancer', author='William Gibson').save()
    assert book.current_revision == 0
    book.title = 'Neuromancer'
    book = await book.save()
    assert book.title == 'Neuromancer'
    assert len(book.revision_hashes) == 2
    assert book.current_revision == 1
    assert book.revision_hashes[0] == book.id_hash
    assert book.revision_hashes[1] != book.id_hash
    await asyncio.sleep(1)
    old_book = await book.fetch_revision(rev_no=0)
    old_timestamp = old_book.timestamp
    assert old_book.title == 'Neurodancer'
    new_book = await book.fetch_revision(rev_no=1)
    assert new_book.title == 'Neuromancer'
    assert new_book.timestamp > old_timestamp


@pytest.mark.asyncio
async def test_store_and_index_record_of_records():
    books = await asyncio.gather(
        Book(title='Atlas Shrugged', author='Ayn Rand').save(),
        Book(title='The Martian', author='Andy Weir').save()
    )
    new_library = await Library(name='The Library', books=books).save()
    await asyncio.sleep(1)
    fetched_library = await Library.where_eq(name='The Library').first()
    assert new_library == fetched_library


@pytest.mark.asyncio
async def test_forget_object():
    forgettable_book = await Book(title="The Forgotten Book", author="Mechthild Gläser").save()  # I'm sorry.
    await asyncio.sleep(1)
    await forgettable_book.forget()
    assert forgettable_book.forgotten is True
    await asyncio.sleep(1)
    assert len(await Book.fetch(forgettable_book.id_hash).all()) == 0
    with pytest.raises(AlreadyForgottenError):
        await forgettable_book.forget()


@pytest.mark.asyncio
async def test_store_and_wrong_where_eq():
    new_book = await Book(title='Atlas Shrugged', author='Ayn Rand').save()
    assert new_book.title == 'Atlas Shrugged'
    assert new_book.author == 'Ayn Rand'
    with pytest.warns(UserWarning):
        fetched_book = (await Book.where_eq(title='Atlas Shrugged', foo="bar").all())
    assert len(fetched_book) == 0


@pytest.mark.asyncio
async def test_fetch_all_pagination():
    page_one = await Book.fetch_objects().page(1, 1)
    page_two = await Book.fetch_objects().page(2, 1)
    assert len(page_one) == 1
    assert len(page_two) == 1
    assert page_one[0] != page_two[0]


@pytest.mark.asyncio
@pytest.mark.skip(reason="Only if you want to forget everything")
async def test_drop_table():
    await Record.forget_all()
    assert len(await Book.fetch_objects().all()) == 0
    assert len(await Library.fetch_objects().all()) == 0


@pytest.mark.asyncio
@pytest.mark.skip(reason="This takes a long time")
async def test_sync_indices():
    await AARS.sync_indices()
    assert len(Record.get_indices()) == 3
    assert len(Book.get_indices()) == 2
    assert len(Library.get_indices()) == 1
    assert len(list(Book.get_indices()[0].hashmap.values())) > 0
    assert len(list(Book.get_indices()[1].hashmap.values())) > 0
    assert len(list(Library.get_indices()[0].hashmap.values())) > 0
