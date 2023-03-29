# FastAPI Cookbook
This cookbook will show you how to build and test an advanced FastAPI app using the full suite of AARS features:

- Indexing for efficient querying
- Built-in pagination for large result sets
- Support for caching
- Listening for new messages on Aleph

## Basic setup
Creating a [FastAPI](https://fastapi.tiangolo.com/) app with AARS is very simple.
The following code snippet shows how to initialize AARS and add it to your FastAPI app.
```python
from fastapi import FastAPI

from aleph.sdk.vm.app import AlephApp

from aars import AARS, Record

http_app = FastAPI()

# Setup the Aleph app
app = AlephApp(http_app=http_app)

# Initialize AARS
aars = AARS(
    channel="MY_CHANNEL",
)
```
Your app is now ready to use AARS. The next step is to define your data schema.

## Interlude: Caching Aleph messages
You can use a cache to speed up subsequent fetches of Aleph messages:
```python
from aleph.sdk.vm.cache import VmCache, TestVmCache
from os import getenv
TEST_CACHE = getenv("TEST_CACHE")
if TEST_CACHE is not None and TEST_CACHE.lower() == "true":
    cache = TestVmCache()
else:
    cache = VmCache()

aars = AARS(
    channel="MY_CHANNEL",
    cache=cache
)
```
In this example, setting the `TEST_CACHE` environment variable to `true` will use a cache that you can
easily run locally with [Uvicorn](https://www.uvicorn.org/):
```shell
export TEST_CACHE=true
python -m uvicorn aars.test_cache:app --reload
```
!!! note "Caches work well with Aleph messages, as they are immutable."
    An exception to this are records that have been forgotten.
    AARS will handle cache invalidation automatically, if you use the [`forget`][aars.core.Record.forget] method to forget a record.
    But you should be aware that forgotten messages differ from the original message and other actors
    thay may forget your message can invalidate your cache without AARS noticing.

!!! tip "Use a cache whenever possible"

## Defining a data schema
Now that we have AARS initialized, we can create a record.
```python
from typing import Optional

from aars import Index

class User(Record):
    username: str
    display_name: str
    bio: Optional[str]

Index(User, 'username')
```
This code snippet creates a User record with three fields: `username`, `display_name`, and `bio`.
The `Index` class creates an index on the `username` field. This is required for efficient querying.

## Syncing indices
We call `AARS.sync_indices()` to sync the indices with the Aleph VM. This is necessary if
you have previously created records through AARS and want to query them using the indices.

The correct way to handle syncing indices is to call `AARS.sync_indices()` on the ASGI startup sequence.
This is the sequence of events that FastAPI runs when it starts up. You can read more about it
[here](https://fastapi.tiangolo.com/advanced/events/#startup-event).
```python
import asyncio

# IMPORTANT: app should be the AlephApp instance
@app.on_event("startup")
async def startup():
    await asyncio.wait_for(AARS.sync_indices(), timeout=None)
```
Note that we are using `asyncio.wait_for` to wait for the indices to sync, as we can set the timeout to `None` for
the possibly lengthy operation.

!!! Warning

    Calling `AARS.sync_indices()` scales linearly with the number of records in the VM. This means that
    if you have 1000 records, it will take roughly `1000 / 50 = 20` API calls to sync the indices. This is why it is
    recommended to call `AARS.sync_indices()` only once, after you have created all of your records and indices.
    AARS will automatically add records to the indices as they are created.
    

## Creating and querying Records with FastAPI
Now that we have a record, we can create a FastAPI endpoint to create and query records.

### Creating a Record
```python
@app.post("/users")
async def create_user(
    username: str,
    display_name: str,
    bio: Optional[str] = None,
) -> User:
    user = User(
        username=username,
        display_name=display_name,
        bio=bio,
    )
    return await user.save()
```
This endpoint will create a new user record and return it.

### Fetching Records
```python
from typing import List
@app.get("/users")
async def get_users(
    page: int = 1,
    page_size: int = 20,
) -> List[User]:
    return await User.fetch_objects().page(page=page, page_size=page_size)
```
This endpoint will return a list of users, paginated by 20 users per page.

### Using an Index to query Records
```python
@app.get("/users/{username}")
async def get_user(
    username: str,
) -> User:
    return await User.fetch_objects().where_eq(username=username).first()
```
This endpoint will return a single user, given their username.

## Listening to Aleph Messages
!!! warning "TODO"
    This section is not yet complete.