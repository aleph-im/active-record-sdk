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
    Index(Book, "title")
    Index(Book, ["title", "author"])
    Index(Library, on="name")


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
        with pytest.raises(KeyError):
            index = Index(Book, "some_nonexistent_field")
    finally:
        if index:
            Record.remove_index(index)


def test_duplicate_index_creation():
    with pytest.raises(ValueError):
        Index(Book, "title")


@pytest.mark.asyncio
async def test_sync_indices():
    await AARS.sync_indices()
    assert len(Record.get_indices()) == 3
    assert len(Book.get_indices()) == 2
    assert len(Library.get_indices()) == 1


@pytest.mark.asyncio
async def test_store_and_index():
    new_book = await Book(title="Atlas Shrugged", author="Ayn Rand").save()
    assert new_book.title == "Atlas Shrugged"
    assert new_book.author == "Ayn Rand"
    await asyncio.sleep(1)
    fetched_book = await Book.filter(title="Atlas Shrugged").first()
    assert new_book == fetched_book


@pytest.mark.asyncio
async def test_multi_index():
    new_book = await Book(title="Lila", author="Robert M. Pirsig", year=1991).save()
    # wait a few secs
    await asyncio.sleep(1)
    should_be_none = await Book.filter(title="Lila", author="Yo Momma").all()
    assert len(should_be_none) == 0
    fetched_book = await Book.filter(title="Lila", author="Robert M. Pirsig").first()
    assert new_book == fetched_book


@pytest.mark.asyncio
async def test_in_query():
    books = await asyncio.gather(
        Book(title="Siddhartha", author="Hermann Hesse", year=1922).save(),
        Book(title="Fahrenheit 451", author="Ray Bradbury", year=1953).save(),
    )
    await asyncio.sleep(1)
    fetched_books = await Book.filter(title__in=["Siddhartha", "Fahrenheit 451"], year__in=[1922, 1953]).all()
    assert books[0] in fetched_books
    assert books[1] in fetched_books


@pytest.mark.asyncio
async def test_amending_record():
    book = await Book(title="Neurodancer", author="William Gibson").save()
    assert book.current_revision == 0
    book.title = "Neuromancer"
    book = await book.save()
    assert book.title == "Neuromancer"
    assert len(book.revision_hashes) == 2
    assert book.current_revision == 1
    assert book.revision_hashes[0] == book.id_hash
    assert book.revision_hashes[1] != book.id_hash
    await asyncio.sleep(1)
    old_book = await book.fetch_revision(rev_no=0)
    old_timestamp = old_book.timestamp
    assert old_book.title == "Neurodancer"
    new_book = await book.fetch_revision(rev_no=1)
    assert new_book.title == "Neuromancer"
    assert new_book.timestamp > old_timestamp


@pytest.mark.asyncio
async def test_store_and_index_record_of_records():
    books = await asyncio.gather(
        Book(title="Atlas Shrugged", author="Ayn Rand").save(),
        Book(title="The Martian", author="Andy Weir").save(),
    )
    new_library = await Library(name="The Library", books=books).save()
    await asyncio.sleep(1)
    fetched_library = await Library.filter(name="The Library").first()
    assert new_library == fetched_library


@pytest.mark.asyncio
async def test_forget_object():
    forgettable_book = await Book(
        title="The Forgotten Book", author="Mechthild Gläser"
    ).save()  # I'm sorry.
    await asyncio.sleep(1)
    await forgettable_book.forget()
    assert forgettable_book.forgotten is True
    await asyncio.sleep(1)
    assert len(await Book.fetch(forgettable_book.id_hash).all()) == 0
    with pytest.raises(AlreadyForgottenError):
        await forgettable_book.forget()


@pytest.mark.asyncio
async def test_store_and_wrong_where_eq():
    new_book = await Book(title="Atlas Shrugged", author="Ayn Rand").save()
    assert new_book.title == "Atlas Shrugged"
    assert new_book.author == "Ayn Rand"
    with pytest.raises(KeyError):
        await Book.filter(title="Atlas Shrugged", foo="bar").all()


@pytest.mark.asyncio
async def test_fetch_all_pagination():
    page_one = await Book.fetch_objects().page(1, 1)
    page_two = await Book.fetch_objects().page(2, 1)
    assert len(page_one) == 1
    assert len(page_two) == 1
    assert page_one[0] != page_two[0]


@pytest.mark.asyncio
async def test_dict_field_save():
    class BookWithDictAuthor(Record):
        title: str
        author: dict

    book = await BookWithDictAuthor(
        title="Test Book", author={"first": "John", "last": "Doe"}
    ).save()
    await asyncio.sleep(1)
    fetched_book = await BookWithDictAuthor.fetch(book.id_hash).first()
    assert fetched_book.author == {"first": "John", "last": "Doe"}


@pytest.mark.asyncio
async def test_negative_limit_pagination():
    with pytest.raises(ValueError):
        await Book.fetch_objects().page(1, -1)


@pytest.mark.asyncio
async def test_large_page_size_pagination():
    page = await Book.fetch_objects().page(1, 100)
    assert len(page) <= 100


@pytest.mark.asyncio
async def test_non_existent_revision():
    book = Book(title="Test Book", author="John Doe")
    await book.save()
    with pytest.raises(IndexError):
        await book.fetch_revision(rev_no=10)


@pytest.mark.asyncio
async def test_save_without_changes():
    book = await Book(title="Test Book", author="John Doe").save()
    original_revision_count = len(book.revision_hashes)
    book = await book.save()
    assert len(book.revision_hashes) == original_revision_count


@pytest.mark.asyncio
async def test_drop_table():
    await Record.forget_all()
