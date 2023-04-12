"""
Aleph Active Record SDK

This module provides a simple and intuitive way to interact with decentralized data storage systems, such as blockchain networks.

AARS allows you to create data models and store them on a blockchain in a way that is easily searchable and retrievable. It provides functionality for indexing and querying data based on multiple fields, as well as support for versioning and updating records.

To get started, simply define your data model as a subclass of the Record class and use the save() method to store new records on the blockchain. You can then use the various query methods provided by AARS to retrieve the data.

For more information on how to use AARS, please see the documentation.
"""
import asyncio
import math
import warnings
import logging
from abc import ABC
from operator import attrgetter
from typing import (
    Type,
    TypeVar,
    Dict,
    ClassVar,
    List,
    Set,
    Any,
    Union,
    Tuple,
    Optional,
    Generic,
    AsyncIterator,
)

from aiohttp import ServerDisconnectedError
from pydantic import BaseModel, Field

from aleph.sdk.client import AuthenticatedAlephClient
from aleph.sdk.types import Account
from aleph.sdk.chains.ethereum import get_fallback_account
from aleph.sdk.conf import settings
from aleph.sdk.vm.cache import VmCache
from aleph_message.models import PostMessage, ItemHash, ChainRef

from .utils import (
    subslices,
    async_iterator_to_list,
    IndexQuery,
    PageableResponse,
    PageableRequest,
    EmptyAsyncIterator,
)
from .exceptions import AlreadyForgottenError, AlephPermissionError, NotStoredError

logger = logging.getLogger(__name__)
R = TypeVar("R", bound="Record")


class Record(BaseModel, ABC):
    """
    A basic record which is persisted on Aleph decentralized storage.

    Records can be updated: revision numbers begin at 0 (original upload) and increment for each `.save()` call.

    Previous revisions can be restored by calling
    ```python
    await record.fetch_revision(rev_no=0)
    ```
    or
    ```python
    await record.fetch_revision(rev_hash="abc123")
    ```

    They can also be forgotten: Aleph will ask the network to forget given item, in order to allow for **GDPR-compliant**
    applications.

    Records have an `indices` class attribute, which allows one to select an index and query it with a key.
    !!! note "It uses `TypeVar("R", bound="Record")` to allow for type hinting of subclasses."
    """

    class Config:
        validate_assignment = True

    forgotten: bool = Field(
        default=False,
        alias="forgotten",
        description="Whether the record has been forgotten on Aleph",
    )
    id_hash: Optional[Union[ItemHash, str]] = Field(
        default=None, alias="id_hash", description="The hash of the record's ID"
    )
    current_revision: Optional[int] = Field(
        default=0,
        alias="current_revision",
        description="The current revision number of the record",
    )
    revision_hashes: List[ItemHash] = Field(
        default_factory=list,
        alias="revision_hashes",
        description="A list of hashes of all revisions of the record",
    )
    timestamp: Optional[float] = Field(
        default=None,
        alias="timestamp",
        description="The timestamp of the record's creation",
    )
    signer: Optional[str] = Field(
        default=None,
        alias="signer",
        description="The address of the signer of the saved record",
    )
    changed: bool = Field(
        default=False,
        alias="changed",
        description="Whether the record has been changed since the last save",
    )

    __indexed_items: ClassVar[Set[str]] = set()
    __indices: ClassVar[Dict[str, "Index"]] = {}

    def __init__(self, **data: Any):
        super().__init__(**data)

    def __repr__(self):
        return f"{type(self).__name__}({self.id_hash})"

    def __str__(self):
        return f"{type(self).__name__} {self.__dict__}"

    def __eq__(self, other):
        return (
            str(self.id_hash) == str(other.id_hash)
            and self.current_revision == other.current_revision
            and self.revision_hashes == other.revision_hashes
            and self.forgotten == other.forgotten
            and self.content == other.content
            and self.signer == other.signer
            and self.changed == other.changed
            # do not compare timestamps, they can deviate on Aleph between commitment and finalization
        )

    def __setattr__(self, key, value):
        if self.changed is False and key != "changed" and self.id_hash is not None:
            self.changed = True
        return super().__setattr__(key, value)

    @property
    def content(self) -> Dict[str, Any]:
        """
        Returns:
            A dictionary of the object's content, as it is to be stored on Aleph inside the `content` property of the POST message.
        """
        return self.dict(
            exclude={
                "id_hash",
                "current_revision",
                "revision_hashes",
                "forgotten",
                "timestamp",
                "signer",
                "changed",
            }
        )

    async def update_revision_hashes(self: R) -> R:
        """
        Updates the list of available revision hashes, in order to fetch these.

        Returns:
            The object with the updated revision hashes.
        """
        assert self.id_hash is not None
        self.revision_hashes = [self.id_hash] + await async_iterator_to_list(
            AARS.fetch_revisions(type(self), ref=self.id_hash)
        )
        if self.current_revision is None:
            # latest revision
            self.current_revision = len(self.revision_hashes) - 1
        return self

    async def fetch_revision(
        self: R, rev_no: Optional[int] = None, rev_hash: Optional[str] = None
    ) -> R:
        """
        Fetches a Record's revision by its revision number (0 => original record) or revision hash.

        Args:
            rev_no: The revision number of the revision to fetch. Negative numbers are allowed and count from the end.
            rev_hash: The hash of the revision to fetch.

        Returns:
            The object with the given revision.
        """
        if self.id_hash is None:
            raise NotStoredError(self)

        if rev_no is not None:
            if rev_no < 0:
                rev_no = len(self.revision_hashes) + rev_no
            if self.current_revision == rev_no:
                return self
            elif rev_no > len(self.revision_hashes):
                raise IndexError(f"No revision no. {rev_no} found for {self}")
            else:
                self.current_revision = rev_no
        elif rev_hash is not None:
            rev_item_hash = ItemHash(rev_hash)
            if (
                self.current_revision
                and rev_hash == self.revision_hashes[self.current_revision]
            ):
                return self
            try:
                self.current_revision = self.revision_hashes.index(rev_item_hash)
            except ValueError:
                raise IndexError(f"{rev_hash} is not a revision of {self}")
        else:
            raise ValueError("Either rev or hash must be provided")

        resp = await AARS.fetch_exact(
            type(self), self.revision_hashes[self.current_revision]
        )
        self.__dict__.update(resp.content)
        self.timestamp = resp.timestamp
        self.signer = resp.signer
        self.changed = False

        return self

    async def save(self: R) -> R:
        """
        Posts a new item to Aleph or amends it, if it was already posted. Will add new items to local indices.
        For indices to be persisted on Aleph, you need to call `save()` on the index itself or `cls.save_indices()`.
        Returns:
            The updated object itself.
        """
        if not self.changed and self.id_hash is not None:
            return self
        if self.forgotten:
            raise AlreadyForgottenError(self)
        await AARS.post_or_amend_object(self)
        if self.current_revision == 0:
            self._index()
        self.changed = False
        return self

    def _index(self):
        """
        Adds the object to all indices it is supposed to be in.
        Returns:
            The object itself for chaining.
        """
        for index in self.get_indices():
            index.add_record(self)
        self.__indexed_items.add(self.id_hash)

    @classmethod
    def is_indexed(cls: Type[R], id_hash: Union[ItemHash, str]) -> bool:
        """
        Checks if a given object is indexed.
        Args:
            id_hash: The hash of the object to check.
        Returns:
            True if the object is indexed, False otherwise.
        """
        return id_hash in cls.__indexed_items

    async def forget(self: R) -> R:
        """
        Orders Aleph to forget a specific object with all its revisions. Will remove the object from all indices.
        The content of all POST messages will be deleted, but the hashes and timestamps will remain.
        !!! note "The forgotten object should be deleted afterwards, as it is useless now."
        Raises:
            NotStoredError: If the object is not stored on Aleph.
            AlephPermissionError: If the object is not owned by the current account.
            AlreadyForgottenError: If the object was already forgotten.
        """

        if not self.forgotten:
            if self.id_hash is None:
                raise NotStoredError(self)
            if self.signer != AARS.account.get_address():
                raise AlephPermissionError(
                    AARS.account.get_address(), self.id_hash, self.signer
                )
            await AARS.forget_objects([self])
            [index.remove_record(self) for index in self.get_indices()]
            self.forgotten = True
            return self
        else:
            raise AlreadyForgottenError(self)

    @classmethod
    async def from_post(cls: Type[R], post: PostMessage) -> R:
        """
        Initializes a record object from its PostMessage.
        Args:
            post: The PostMessage to initialize from.
        Returns:
            The initialized object.
        """
        obj: R = cls(**post.content.content)
        if post.content.ref is None:
            obj.id_hash = post.item_hash
        else:
            if isinstance(post.content.ref, str):
                obj.id_hash = ItemHash(post.content.ref)
            elif isinstance(post.content.ref, ChainRef):
                obj.id_hash = post.content.ref.item_hash
            else:
                raise TypeError(f"Unknown type of ref: {type(post.content.ref)}")
        await obj.update_revision_hashes()
        assert obj.id_hash is not None
        obj.current_revision = obj.revision_hashes.index(obj.id_hash)
        obj.timestamp = post.time
        obj.signer = post.sender
        return obj

    @classmethod
    async def from_dict(cls: Type[R], post: Dict[str, Any]) -> R:
        """
        Initializes a record object from its raw Aleph data.
        Args:
            post: The raw Aleph data to initialize from.
        Returns:
            The initialized object.
        """
        obj = cls(**post["content"])
        if post.get("ref") is None:
            obj.id_hash = ItemHash(post["item_hash"])
        else:
            obj.id_hash = ItemHash(post["ref"])
        await obj.update_revision_hashes()
        assert obj.id_hash is not None
        obj.current_revision = obj.revision_hashes.index(obj.id_hash)
        obj.timestamp = post["time"]
        obj.signer = post["sender"]
        return obj

    @classmethod
    def fetch(
        cls: Type[R], hashes: Union[Union[str, ItemHash], List[Union[str, ItemHash]]]
    ) -> PageableResponse[R]:
        """
        Fetches one or more objects of given type by its/their item_hash[es].
        Args:
            hashes: The item_hash[es] of the objects to fetch.
        Returns:
            A pageable response object, which can be asynchronously iterated over.
        """
        if not isinstance(hashes, List):
            hashes = [hashes]
        items = AARS.fetch_records(cls, list(hashes))
        return PageableResponse(items)

    @classmethod
    def fetch_objects(cls: Type[R]) -> PageableRequest[R]:
        """
        Fetches all objects of given type.

        Returns:
            A pageable request object, which can be asynchronously iterated over.
        """
        return PageableRequest(AARS.fetch_records, record_type=cls)

    @classmethod
    def where_eq(cls: Type[R], **kwargs) -> PageableResponse[R]:
        """
        Queries an object by given properties through an index, in order to fetch applicable records.
        An index name is defined as
        ```python
        "Class.property1.property2"
        ```
        and is initialized by creating an `Index` instance, targeting a BaseRecord class with a list of properties.

        ```python
        Index(MyRecord, ['property1', 'property2'])
        ```

        This will create an index named 'MyRecord.property1.property2' which can be queried with:

        ```python
        MyRecord.where_eq(property1='value1', property2='value2')
        ```

        If no index is defined for the given properties, an IndexError is raised.

        If only a part of the keys is indexed for the given query, a fallback index is used and locally filtered.

        Args:
            **kwargs: The properties to query for.
        Returns:
            A pageable response object, which can be asynchronously iterated over.
        """
        query = IndexQuery(cls, **kwargs)
        index = cls.get_index(query.get_index_name())
        generator = index.lookup(query)
        return PageableResponse(generator)

    @classmethod
    def add_index(cls: Type[R], index: "Index") -> None:
        """
        Adds an index to the class. This allows the index to be used for queries and will be automatically updated
        when records are created or updated.
        Args:
            index: The index to add.
        """
        cls.__indices[repr(index)] = index

    @classmethod
    def remove_index(cls: Type[R], index: "Index") -> None:
        """
        Removes an index from the class. This stops the index from being used for queries or updates.
        Args:
            index: The index to remove.
        """
        del cls.__indices[repr(index)]

    @classmethod
    def get_index(cls: Type[R], index_name: str) -> "Index[R]":
        """
        Returns an index or any of its subindices by its name. The name is defined as
        `"<object_class>.[<object_properties>.]"` with the properties being sorted alphabetically. For example,
        `"Book.author.title"` is a valid index name, while `"Book.title.author"` is not.
        Args:
            index_name: The name of the index to fetch.
        Returns:
            The index instance or a subindex.
        """
        index = cls.__indices.get(index_name)
        if index is None:
            key_subslices = subslices(list(index_name.split(".")[1:]))
            # returns all plausible combinations of keys
            key_subslices = sorted(key_subslices, key=lambda x: len(x), reverse=True)
            for keys in key_subslices:
                name = cls.__name__ + "." + ".".join(keys)
                if cls.__indices.get(name):
                    warnings.warn(f"No index {index_name} found. Using {name} instead.")
                    return cls.__indices[name]
            raise IndexError(f"No index or subindex for {index_name} found.")
        return index

    @classmethod
    def get_indices(cls: Type[R]) -> List["Index"]:
        """
        Returns all indices of a given Record subclass.
        Returns:
            A list of existing indices.
        """
        if cls == Record:
            return list(cls.__indices.values())
        return [index for index in cls.__indices.values() if index.record_type == cls]

    @classmethod
    async def save_indices(cls: Type[R]) -> None:
        """Updates all indices of given Record subclass."""
        tasks = [index.save() for index in cls.get_indices()]
        await asyncio.gather(*tasks)

    @classmethod
    async def regenerate_indices(cls: Type[R]) -> List[R]:
        """
        Regenerates all indices of given Record subtype.
        If invoked on Record, will try to fetch all objects of the current channel and index them.

        !!! warning "This can take quite some time, depending on the amount of records to be fetched."

        Returns:
            A list of all records that were indexed.
        """
        response = cls.fetch_objects()

        records = []
        async for record in response:
            record._index()
            records.append(record)
        return records

    @classmethod
    async def forget_all(cls: Type[R]) -> List[ItemHash]:
        """
        Forgets all Records of given type of the authorized user. If invoked on Record, will try to fetch all objects
        of the current channel and forget them.
        Returns:
            A list of all item hashes that were forgotten.
        """
        response = cls.fetch_objects()

        item_hashes = []
        record_batch = []
        i = 0
        async for record in response:
            if record.signer != AARS.account.get_address():
                continue
            record_batch.append(record)
            i += 1
            if i % 50 == 0:
                item_hashes.extend([record.id_hash for record in record_batch])
                await AARS.forget_objects(record_batch)
                record_batch = []
        if record_batch:
            item_hashes.extend([record.id_hash for record in record_batch])
            await AARS.forget_objects(record_batch)

        return item_hashes


class Index(Record, Generic[R]):
    """
    An Index is a data structure that allows for fast lookup of records by their properties.
    It is used internally by the Record class and once created, is automatically updated when records are created or
    updated.

    It is not recommended using the Index class directly, but rather through the `where_eq` method of the `Record`
    class.

    Example:
        ```python
        MyRecord.where_eq(foo='bar')
        ```
        If `MyRecord` has an index on the property `foo`, this will return all records of type `MyRecord` where `foo` is
        equal to `'bar'`.
    """

    record_type: Type[R]
    index_on: List[str]
    hashmap: Dict[Tuple, Set[str]] = {}

    def __init__(self, record_type: Type[R], on: Union[str, List[str], Tuple[str]]):
        """
        Creates a new index given a record_type and a single or multiple properties to index on.

        Example:
            ```python
            Index(MyRecord, 'foo')
            ```
            Indexes all records of type MyRecord on the property 'foo'.

        This will create an index named `'MyRecord.foo'`, which is stored in the `MyRecord` class.

        Args:
            record_type: The record_type to index.
            on: The properties to index on. Can be a single property or a list/tuple of properties.
        """
        if isinstance(on, str):
            on = [on]
        # check if all properties exist
        for prop in on:
            if prop not in record_type.__fields__:
                raise KeyError(
                    f"Property {prop} does not exist on {record_type.__name__}"
                )
        super(Index, self).__init__(record_type=record_type, index_on=sorted(on))
        # check if index already exists
        if repr(self) in [repr(index) for index in Record.get_indices()]:
            raise ValueError(f"Index {self} already exists.")
        record_type.add_index(self)

    def __str__(self):
        return f"Index({self.record_type.__name__}.{'.'.join(self.index_on)})"

    def __repr__(self):
        return f"{self.record_type.__name__}.{'.'.join(self.index_on)}"

    def lookup(self, query: IndexQuery) -> AsyncIterator[R]:
        """
        Fetches records with given values for the indexed properties.

        Example:
            ```python
            index = Index(MyRecord, 'foo')
            index_query = IndexQuery(MyRecord, **{'foo': 'bar'})
            index.lookup(index_query)
            ```
            Returns all records of type `MyRecord` where `foo` is equal to `'bar'`.
            This is an anti-pattern, as the `IndexQuery` should be created by calling `MyRecord.where_eq(foo='bar')`
            instead.

        Args:
            query: The query to execute.
        Returns:
            An async iterator over the records.
        """
        assert query.record_type == self.record_type
        id_hashes: Optional[Set[str]]
        needs_filtering = False

        subquery = query
        if repr(self) != query.get_index_name():
            subquery = query.get_subquery(self.index_on)
            needs_filtering = True
        id_hashes = self.hashmap.get(tuple(subquery.values()))

        if id_hashes is None:
            return EmptyAsyncIterator()

        items = AARS.fetch_records(self.record_type, list(id_hashes))

        if needs_filtering:
            return self._filter_index_items(items, query)
        return items

    @classmethod
    async def _filter_index_items(
        cls, items: AsyncIterator[R], query: IndexQuery
    ) -> AsyncIterator[R]:
        sorted_keys = query.keys()
        async for item in items:
            # eliminate the item which does not fulfill the properties
            class_properties = vars(item)
            required_class_properties = {
                key: class_properties.get(key) for key in sorted_keys
            }
            if required_class_properties == dict(query):
                yield item

    def add_record(self, obj: R):
        """Adds a record to the index."""
        assert issubclass(type(obj), Record)
        assert obj.id_hash is not None
        key = attrgetter(*self.index_on)(obj)
        if isinstance(key, str):
            key = (key,)
        if isinstance(key, list):
            key = tuple(key)
        if key not in self.hashmap:
            self.hashmap[key] = set()
        self.hashmap[key].add(obj.id_hash)

    def remove_record(self, obj: R):
        """Removes a record from the index, i.e. when it is forgotten."""
        assert obj.id_hash is not None
        key = attrgetter(*self.index_on)(obj)
        if isinstance(key, str):
            key = (key,)
        if key in self.hashmap:
            self.hashmap[key].remove(obj.id_hash)

    def regenerate(self, items: List[R]):
        """Regenerates the index with given items."""
        self.hashmap = {}
        for item in items:
            self.add_record(item)


class AARS:
    """
    The AARS class is the main entry point for the Aleph Active Record SDK.
    It provides versatile methods to create, update, delete and query records.
    """

    account: Account
    channel: str
    api_url: str
    retry_count: int
    session: AuthenticatedAlephClient
    cache: Optional[VmCache]

    def __init__(
        self,
        account: Optional[Account] = None,
        channel: Optional[str] = None,
        api_url: Optional[str] = None,
        session: Optional[AuthenticatedAlephClient] = None,
        cache: Optional[VmCache] = None,
        retry_count: Optional[int] = None,
    ):
        """
        Initializes the SDK with an account and a channel.
        Args:
            account: Account with which to sign the messages. Defaults to the fallback account.
            channel: Channel to which to send the messages. Defaults to 'AARS_TEST'.
            api_url: The API URL to use. Defaults to an official Aleph API host.
            session: An aiohttp session to use. Defaults to a new session with the given account.
            cache: An optional Aleph VM cache to cache messages.
            retry_count: The number of times to retry a failed request. Defaults to 3.
        """
        AARS.account = account if account else get_fallback_account()
        AARS.channel = channel if channel else "AARS_TEST_v0.5.6"
        AARS.api_url = api_url if api_url else settings.API_HOST
        AARS.session = (
            session
            if session
            else AuthenticatedAlephClient(
                account=AARS.account, api_server=settings.API_HOST
            )
        )
        AARS.cache = cache
        AARS.retry_count = retry_count if retry_count else 3

    @classmethod
    async def sync_indices(cls):
        """
        Synchronizes all the indices created so far, by iteratively fetching all the messages from the channel,
        having post_types of the Record subclasses that have been declared so far.

        !!! warning "This can take quite some time on large databases."
        """
        for record in Record.__subclasses__():
            if record.get_indices():
                await record.regenerate_indices()

    @classmethod
    async def post_or_amend_object(cls, obj: R, channel: Optional[str] = None) -> R:
        """
        Posts or amends an object to Aleph. If the object is already posted, it's list of revision hashes is updated and
        the object receives the latest revision number.
        Args:
            obj: The object to post or amend.
            channel: The channel to post the object to. If None, will use the configured default channel.
        Returns:
            The object with the updated revision hashes and revision number.
        """
        if channel is None:
            channel = cls.channel
        assert isinstance(obj, Record)
        post_type = type(obj).__name__ if obj.id_hash is None else "amend"
        if (
            obj.id_hash is not None
            and obj.signer is not None
            and obj.signer != cls.account.get_address()
        ):
            raise AlephPermissionError(
                cls.account.get_address(), obj.id_hash, obj.signer
            )
        message, status = await cls.session.create_post(
            post_content=obj.content,
            post_type=post_type,
            channel=channel,
            ref=obj.id_hash,
        )
        if obj.id_hash is None:
            obj.id_hash = message.item_hash
        obj.revision_hashes.append(message.item_hash)
        obj.current_revision = len(obj.revision_hashes) - 1
        obj.timestamp = message.time
        obj.signer = message.sender
        if cls.cache:
            await cls.cache.set(message.item_hash, obj.json())
        return obj

    @classmethod
    async def forget_objects(
        cls,
        objs: List[R],
        channel: Optional[str] = None,
    ):
        """
        Forgets multiple objects from Aleph and local cache. All related revisions will be forgotten too.
        Args:
            objs: The objects to forget.
            channel: The channel to delete the object from. If None, will use the configured default channel.
        """
        if channel is None:
            channel = cls.channel
        hashes = []
        for obj in objs:
            if obj.id_hash is None:
                raise ValueError("Cannot forget an object that has not been posted.")
            if obj.signer != cls.account.get_address():
                raise AlephPermissionError(
                    obj.signer, obj.id_hash, cls.account.get_address()
                )
            hashes += [obj.id_hash] + obj.revision_hashes
        forget_task = cls.session.forget(
            hashes=hashes,
            reason=None,
            channel=channel,
        )
        if cls.cache:
            await asyncio.gather(forget_task, *[cls.cache.delete(h) for h in hashes])
        else:
            await forget_task

    @classmethod
    async def fetch_records(
        cls,
        record_type: Type[R],
        item_hashes: Optional[List[Union[str, ItemHash]]] = None,
        channel: Optional[str] = None,
        owner: Optional[str] = None,
        page_size: int = 50,
        page: Optional[int] = 1,
    ) -> AsyncIterator[R]:
        """
        Retrieves posts as objects by its aleph item_hash.
        Args:
            record_type: The type of the objects to retrieve.
            item_hashes: Aleph item_hashes of the objects to fetch.
            channel: Channel in which to look for it.
            owner: Account that owns the object.
            page_size: Number of items to fetch per page.
            page: Page number to fetch, based on page_size. If None, will fetch all pages.
        Returns:
            An iterator over the found records.
        """
        if (page and page < 1) or page_size < 1:
            raise ValueError("page and page_size must be positive and non-zero.")
        if not issubclass(record_type, Record):
            raise ValueError("record_type must be a subclass of Record.")
        channels = None if channel is None else [channel]
        owners = None if owner is None else [owner]
        if item_hashes is None and channels is None and owners is None:
            channels = [cls.channel]

        if cls.cache and item_hashes is not None:
            # TODO: Add some kind of caching for channels and owners or add recent item_hashes endpoint to the Aleph API
            records = await cls._fetch_records_from_cache(record_type, item_hashes)
            cached_ids = []
            for record in records:
                cached_ids.append(record.id_hash)
                record.changed = False
                yield record
            # Remove the cached ids from the list of item_hashes to fetch from the API
            item_hashes = [h for h in item_hashes if h not in cached_ids]
            if len(item_hashes) == 0:
                return

        async for record in cls._fetch_records_from_api(
            record_type=record_type,
            item_hashes=item_hashes,
            channels=channels,
            owners=owners,
            page_size=page_size,
            page=page,
        ):
            record.changed = False
            yield record

    @classmethod
    async def _fetch_records_from_cache(
        cls, record_type: Type[R], item_hashes: List[str]
    ) -> List[R]:
        assert cls.cache, "Cache is not set"
        raw_records = await asyncio.gather(*[cls.cache.get(h) for h in item_hashes])
        return [
            record_type.parse_raw(r)
            for r in raw_records
            if r is not None and not isinstance(r, BaseException)
        ]

    @classmethod
    async def _fetch_records_from_api(
        cls,
        record_type: Type[R],
        item_hashes: Optional[List[Union[str, ItemHash]]] = None,
        channels: Optional[List[str]] = None,
        owners: Optional[List[str]] = None,
        refs: Optional[List[str]] = None,
        page_size: int = 50,
        page: Optional[int] = 1,
    ) -> AsyncIterator[R]:
        """
        Retrieves posts as objects by its aleph item_hash.
        Args:
            record_type: The type of the objects to retrieve.
            item_hashes: Aleph item_hashes of the objects to fetch.
            channels: Channels in which to look for it.
            owners: Accounts that own the object.
            refs: References to the object.
            page_size: Number of items to fetch per page.
            page: If None, will fetch all pages.
        Returns:
            An iterator over the found records.
        """
        aleph_resp = None
        retries = cls.retry_count
        if item_hashes is not None:
            item_hashes = [str(h) for h in item_hashes]
        while aleph_resp is None:
            try:
                actual_page = page if page else 1
                aleph_resp = await cls.session.get_posts(
                    hashes=item_hashes,
                    channels=channels,
                    types=[record_type.__name__],
                    addresses=owners,
                    refs=refs,
                    pagination=page_size,
                    page=actual_page,
                )
            except ServerDisconnectedError:
                retries -= 1
                if retries == 0:
                    raise
        for post in aleph_resp["posts"]:
            yield await record_type.from_dict(post)

        if page is None:
            # Get all pages
            total_items = aleph_resp["pagination_total"]
            per_page = aleph_resp["pagination_per_page"]
            if total_items > per_page:
                for next_page in range(2, math.ceil(total_items / per_page) + 1):
                    async for record in cls._fetch_records_from_api(
                        record_type=record_type,
                        item_hashes=item_hashes,
                        channels=channels,
                        owners=owners,
                        refs=refs,
                        page=next_page,
                    ):
                        yield record

    @classmethod
    async def fetch_revisions(
        cls,
        record_type: Type[R],
        ref: str,
        channel: Optional[str] = None,
        owner: Optional[str] = None,
        page=1,
    ) -> AsyncIterator[ItemHash]:
        """Retrieves posts of revisions of an object by its item_hash.
        Args:
            record_type: The type of the objects to retrieve.
            ref: item_hash of the object, whose revisions to fetch.
            channel: Channel in which to look for it.
            owner: Account that owns the object.
            page: Page number to fetch.
        Returns:
            An iterator over the found records.
        """
        owners = None if owner is None else [owner]
        channels = None if channel is None else [channel]
        if owners is None and channels is None:
            channels = [cls.channel]

        aleph_resp = None
        retries = cls.retry_count
        while aleph_resp is None:
            try:
                aleph_resp = await cls.session.get_messages(
                    channels=channels,
                    addresses=owners,
                    refs=[ref],
                    pagination=50,
                    page=page,
                )
            except ServerDisconnectedError:
                retries -= 1
                if retries == 0:
                    raise
        for message in aleph_resp.messages:
            yield message.item_hash

        if page == 1:
            # If there are more pages, fetch them
            total_items = aleph_resp.pagination_total
            per_page = aleph_resp.pagination_per_page
            if total_items > per_page:
                for next_page in range(2, math.ceil(total_items / per_page) + 1):
                    async for item_hash in cls.fetch_revisions(
                        record_type=record_type,
                        ref=ref,
                        channel=channel,
                        owner=owner,
                        page=next_page,
                    ):
                        yield ItemHash(item_hash)

    @classmethod
    async def fetch_exact(cls, record_type: Type[R], item_hash: str) -> R:
        """Retrieves the revision of an object by its item_hash of the message. The content will be exactly the same
        as in the referenced message, so no amendments will be applied.
        Args:
            record_type: The type of the object to retrieve.
            item_hash: item_hash of the message, whose content to fetch.
        Returns:
            The record in the state it was when the message was created.
        """
        if cls.cache:
            cache_resp = await cls._fetch_records_from_cache(record_type, [item_hash])
            if len(cache_resp) > 0:
                return cache_resp[0]
        aleph_resp = await cls.session.get_messages(hashes=[item_hash])
        if len(aleph_resp.messages) == 0:
            raise ValueError(f"Message with hash {item_hash} not found.")
        message: PostMessage = aleph_resp.messages[0]
        return await record_type.from_post(message)
