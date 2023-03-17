# AARS: Aleph Active Record SDK

AARS's goal is to provide simple guardrails for the creation of document databases, based on Aleph's decentralized storage API. It provides tools for modelling, creating and managing decentralized databases, and a set of extensions for the [Aleph Python SDK](https://github.com/aleph-im/aleph-client).

You can create a model of your planned database by using the `Record` class.

## Usage

```python
from aars import Record, Index, AARS


class Book(Record):
  title: str
  author: str


# initialize the SDK and post subsequent requests to the "MyLibrary" channel on Aleph
AARS(channel="MyLibrary")

# create and add an index for the book title
Index(Book, 'title')

# create & upload a book
new_book = await Book(title='Atlas Shrugged', author='Ayn Rand').save()

# retrieve a book by its ID
book = await Book.fetch(new_book.id_hash).first()

# retrieve all books
book = await Book.where_eq(title='Atlas Shrugged').all()

# iterate asynchronously over all books
async for book in Book.fetch_objects():
    pass
```


## ToDo:
- [x] Basic CRUD operations
- [x] Versioning
  - [x] Use "amend" post_type for updates
  - [x] Fetch revisions with messages endpoint
- [x] Basic indexing/querying operations
  - [x] Single-key indexing 
  - [x] Multi-key indexing
  - [x] Query with list of keys
  - [x] Update indices function
  - [x] Allow multiple items to share one index key
- [ ] Add more comparators for where_() queries
  - [ ] where_gte()
  - [ ] where_lte()
  - [ ] where_contains()
- [ ] Persist indices to lower startup time
- [x] Automatic multi-page fetching
- [x] Encapsulate Aleph SDK as class
- [x] Local VM caching
- [x] Add tests
- [x] Add documentation
- [x] Add to indices when fetching records
- [x] Test where_eq() for fetching multiple records
- [x] Add reindexing function on AARS
- [ ] Add caching of records
  - [x] Cache records retrieved by item_hash
  - [ ] Cache records retrieved by fetch_all/timeline
    - [ ] Add item_hashes endpoint to pyaleph for quick cross-checking of cache hashes
- [x] AsyncIterator over retrieved Records
- [x] Automated pagination handling on Aleph request and user query side
