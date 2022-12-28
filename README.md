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
new_book = await Book.create(title='Atlas Shrugged', author='Ayn Rand')

# retrieve a book by its ID
book = await Book.get(new_book.id)[0]

# retrieve a book by its title
book = await Book.query(title='Atlas Shrugged')[0]
```


## ToDo:
- [x] Basic CRUD operations
- [x] Basic indexing operations
  - [x] Single-key indexing 
  - [x] Multi-key indexing
  - [ ] Name indices' post_type 
- [ ] (IN PROGRESS) Basic search/filtering operations
- [ ] Handle pagination
- [x] Encapsulate Aleph SDK as class
- [x] Local VM caching
- [x] Add tests
- [x] Add documentation
