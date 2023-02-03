import asyncio
from typing import List, Optional

from aleph_client.asynchronous import get_fallback_session

from src.aars import Record, Index, AARS
from src.aars.exceptions import AlreadyForgottenError
import pytest

AARS(session=get_fallback_session())


@pytest.fixture(scope="session")
def event_loop():
    yield AARS.session.loop
    asyncio.run(AARS.session.close())


class Book(Record):
    title: str
    author: str
    year: Optional[int]


class Library(Record):
    name: str
    books: List[Book]


@pytest.mark.asyncio
async def test_store_and_index():
    Index(Book, 'title')
    new_book = await Book.create(title='Atlas Shrugged', author='Ayn Rand')
    assert new_book.title == 'Atlas Shrugged'
    assert new_book.author == 'Ayn Rand'
    await asyncio.sleep(1)
    fetched_book = (await Book.query(title='Atlas Shrugged'))[0]
    assert new_book == fetched_book


@pytest.mark.asyncio
async def test_multi_index():
    Index(Book, ['title', 'author'])
    new_book = await Book.create(title='Lila', author='Robert M. Pirsig', year=1991)
    # wait a few secs
    await asyncio.sleep(1)
    should_be_none = (await Book.query(title='Lila', author='Yo Momma'))
    assert len(should_be_none) == 0
    fetched_book = (await Book.query(title='Lila', author='Robert M. Pirsig'))[0]
    assert new_book == fetched_book


@pytest.mark.asyncio
async def test_amending_record():
    book = await Book.create(title='Neurodancer', author='William Gibson')
    assert book.current_revision == 0
    book.title = 'Neuromancer'
    book = await book.upsert()
    assert book.title == 'Neuromancer'
    assert len(book.revision_hashes) == 2
    assert book.current_revision == 1
    assert book.revision_hashes[0] == book.id_hash
    assert book.revision_hashes[1] != book.id_hash
    await asyncio.sleep(1)
    old_book = await book.fetch_revision(rev_no=0)
    assert old_book.title == 'Neurodancer'
    new_book = await book.fetch_revision(rev_no=1)
    assert new_book.title == 'Neuromancer'


@pytest.mark.asyncio
async def test_store_and_index_record_of_records():
    Index(Library, on='name')
    books = await asyncio.gather(
        Book.create(title='Atlas Shrugged', author='Ayn Rand'),
        Book.create(title='The Martian', author='Andy Weir')
    )
    new_library = await Library.create(name='The Library', books=books)
    await asyncio.sleep(1)
    fetched_library = (await Library.query(name='The Library'))[0]
    assert new_library == fetched_library


@pytest.mark.asyncio
async def test_forget_object():
    forgettable_book = await Book.create(title="The Forgotten Book", author="Mechthild Gläser")  # I'm sorry.
    await asyncio.sleep(1)
    await forgettable_book.forget()
    assert forgettable_book.forgotten is True
    await asyncio.sleep(1)
    assert len(await Book.fetch(forgettable_book.id_hash)) == 0
    with pytest.raises(AlreadyForgottenError):
        await forgettable_book.forget()


@pytest.mark.asyncio
async def test_store_and_wrong_query():
    Index(Book, 'title')
    new_book = await Book.create(title='Atlas Shrugged', author='Ayn Rand')
    assert new_book.title == 'Atlas Shrugged'
    assert new_book.author == 'Ayn Rand'
    with pytest.warns(UserWarning):
        fetched_book = (await Book.query(title='Atlas Shrugged', foo="bar"))
    assert len(fetched_book) == 0
