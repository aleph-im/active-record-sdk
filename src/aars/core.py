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
from pydantic import BaseModel

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
    PageableRequest, EmptyAsyncIterator,
)
from .exceptions import AlreadyForgottenError

logger = logging.getLogger(__name__)
R = TypeVar("R", bound="Record")


class Record(BaseModel, ABC):
    """
    A basic record which is persisted on Aleph decentralized storage.

    Records can be updated: revision numbers begin at 0 (original upload) and increment for each `save()` call.

    Previous revisions can be restored by calling `fetch_revision(rev_no=<number>)` or `fetch_revision(
    rev_hash=<item_hash of inserted update>)`.

    They can also be forgotten: Aleph will ask the network to forget given item, in order to allow for GDPR-compliant
    applications.

    Records have an `indices` class attribute, which allows one to select an index and query it with a key.
    """

    forgotten: bool = False
    id_hash: Optional[Union[ItemHash, str]] = None
    current_revision: Optional[int] = None
    revision_hashes: List[ItemHash] = []
    timestamp: Optional[float] = None
    __indices: ClassVar[Dict[str, "Index"]] = {}

    def __repr__(self):
        return f"{type(self).__name__}({self.id_hash})"

    def __str__(self):
        return f"{type(self).__name__} {self.__dict__}"

    def __eq__(self, other):
        return (
            str(self.id_hash) == str(other.id_hash) and
            self.current_revision == other.current_revision and
            self.revision_hashes == other.revision_hashes and
            self.forgotten == other.forgotten and
            self.content == other.content
            # do not compare timestamps, they can deviate on Aleph between commitment and finalization
        )

    @property
    def content(self) -> Dict[str, Any]:
        """
        :return: content dictionary of the object, as it is to be stored on Aleph.
        """
        return self.dict(
            exclude={"id_hash", "current_revision", "revision_hashes", "forgotten", "timestamp"}
        )

    async def update_revision_hashes(self: R):
        """
        Updates the list of available revision hashes, in order to fetch these.
        """
        assert self.id_hash is not None
        self.revision_hashes = [self.id_hash] + await async_iterator_to_list(
            AARS.fetch_revisions(type(self), ref=self.id_hash)
        )
        if self.current_revision is None:
            # latest revision
            self.current_revision = len(self.revision_hashes) - 1

    async def fetch_revision(
        self: R, rev_no: Optional[int] = None, rev_hash: Optional[str] = None
    ) -> R:
        """
        Fetches a revision of the object by revision number (0 => original) or revision hash.
        :param rev_no: the revision number of the revision to fetch.
        :param rev_hash: the hash of the revision to fetch.
        """
        assert (
            self.id_hash is not None
        ), "Cannot fetch revision of an object which has not been posted yet."
        assert self.current_revision is not None

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
            if rev_hash == self.revision_hashes[self.current_revision]:
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

        return self

    async def save(self):
        """
        Posts a new item to Aleph or amends it, if it was already posted. Will add new items to local indices.
        For indices to be persisted on Aleph, you need to call `save()` on the index itself or `cls.save_indices()`.
        """
        await AARS.post_or_amend_object(self)
        if self.current_revision == 0:
            self._index()
        return self

    def _index(self):
        for index in self.get_indices():
            index.add_record(self)

    async def forget(self):
        """
        Orders Aleph to forget a specific object with all its revisions.
        The forgotten object should be deleted afterward, as it is useless now.
        """
        if not self.forgotten:
            await AARS.forget_objects([self])
            [index.remove_record(self) for index in self.get_indices()]
            self.forgotten = True
        else:
            raise AlreadyForgottenError(self)

    @classmethod
    async def from_post(cls: Type[R], post: PostMessage) -> R:
        """
        Initializes a record object from its PostMessage.
        :param post: the PostMessage to initialize from.
        """
        obj = cls(**post.content.content)
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
        return obj

    @classmethod
    async def from_dict(cls: Type[R], post: Dict[str, Any]) -> R:
        """
        Initializes a record object from its raw Aleph data.
        :post: Raw Aleph data.
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
        return obj

    @classmethod
    def fetch(cls: Type[R], hashes: Union[Union[str, ItemHash], List[Union[str, ItemHash]]]) -> PageableResponse[R]:
        """
        Fetches one or more objects of given type by its/their item_hash[es].
        """
        if not isinstance(hashes, List):
            hashes = [hashes]
        items = AARS.fetch_records(cls, list(hashes))
        return PageableResponse(items)

    @classmethod
    def fetch_objects(cls: Type[R]) -> PageableRequest[R]:
        """
        Fetches all objects of given type.

        WARNING: This can take quite some time, depending on the amount of records to be fetched.

        :return: A PageableRequest, which can be used to iterate, paginate or fetch all results at once.
        """
        return PageableRequest(AARS.fetch_records, record_type=cls)

    @classmethod
    def where_eq(cls: Type[R], **kwargs) -> PageableResponse[R]:
        """
        Queries an object by given properties through an index, in order to fetch applicable records.
        An index name is defined as '<object_class>.[<object_properties>.]' and is initialized by creating
        an Index instance, targeting a BaseRecord class with a list of properties.

        >>> Index(MyRecord, ['property1', 'property2'])

        This will create an index named 'MyRecord.property1.property2' which can be queried with:

        >>> MyRecord.where_eq(property1='value1', property2='value2')

        If no index is defined for the given properties, an IndexError is raised.

        If only a part of the keys is indexed for the given query, a fallback index is used and locally filtered.

        It will return a PageableResponse, which can be used to iterate, paginate or fetch all results at once.

        >>> response = MyRecord.where_eq(property1='value1', property2='value2')
        >>> async for record in response:
        >>>     print(record)

        >>> response = await MyRecord.where_eq(property2='value2').all()

        >>> response = await MyRecord.where_eq(property1='value1').page(2, 10)

        :param kwargs: The properties to query for.
        """
        query = IndexQuery(cls, **kwargs)
        index = cls.get_index(query.get_index_name())
        generator = index.lookup(query)
        return PageableResponse(generator)

    @classmethod
    def add_index(cls: Type[R], index: "Index") -> None:
        cls.__indices[repr(index)] = index

    @classmethod
    def get_index(cls: Type[R], index_name: str) -> "Index[R]":
        """
        Returns an index or any of its subindices by its name. The name is defined as
        '<object_class>.[<object_properties>.]' with the properties being sorted alphabetically. For example,
        Book.author.title is a valid index name, while Book.title.author is not.
        :param index_name: The name of the index to fetch.
        :return: The index instance or a subindex.
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
        if cls == Record:
            return list(cls.__indices.values())
        return [index for index in cls.__indices.values() if index.record_type == cls]

    @classmethod
    async def save_indices(cls: Type[R]) -> None:
        """Updates all indices of given type."""
        tasks = [index.save() for index in cls.get_indices()]
        await asyncio.gather(*tasks)

    @classmethod
    async def regenerate_indices(cls: Type[R]) -> List[R]:
        """
        Regenerates all indices of given type.

        WARNING: This can take quite some time, depending on the amount of records to be fetched.
        """
        response = cls.fetch_objects()

        records = []
        async for record in response:
            record._index()
            records.append(record)
        return records

    @classmethod
    async def drop_table(cls: Type[R]) -> List[ItemHash]:
        """
        Forgets all Records of given type. If invoked on Record, will try to fetch all objects of the current channel
        and forget them.

        :return: The affected item_hashes
        """
        response = cls.fetch_objects()

        item_hashes = []
        record_batch = []
        i = 0
        async for record in response:
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
    Class to define Indices.
    """

    record_type: Type[R]
    index_on: List[str]
    hashmap: Dict[Tuple, Set[str]] = {}

    def __init__(self, record_type: Type[R], on: Union[str, List[str], Tuple[str]]):
        """
        Creates a new index given a record_type and a single or multiple properties to index on.

        >>> Index(MyRecord, 'foo')

        This will create an index named 'MyRecord.foo', which is stored in the `MyRecord` class.
        It is not recommended using the index directly, but rather through the `where_eq` method of the `Record` class
        like so:

        >>> MyRecord.where_eq(foo='bar')

        This returns all records of type MyRecord where foo is equal to 'bar'.

        :param record_type: The record_type to index.
        :param on: The properties to index on.
        """
        if isinstance(on, str):
            on = [on]
        for prop in on:
            if prop not in record_type.__fields__.keys():
                raise ValueError(f"Property {prop} is not defined in {record_type.__name__}")
        super(Index, self).__init__(record_type=record_type, index_on=sorted(on))
        record_type.add_index(self)

    def __str__(self):
        return f"Index({self.record_type.__name__}.{'.'.join(self.index_on)})"

    def __repr__(self):
        return f"{self.record_type.__name__}.{'.'.join(self.index_on)}"

    def lookup(self, query: IndexQuery) -> AsyncIterator[R]:
        """
        Fetches records with given values for the indexed properties.

        :param query: The query to lookup items with.
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
        :param cache: Whether to use Aleph VM caching when running AARS code.
        :param account: Account with which to sign the messages.
        :param channel: Channel to which to send the messages.
        :param api_url: The API URL to use. Defaults to an official Aleph API host.
        :param session: An aiohttp session to use. Defaults to a new session.
        """
        AARS.account = account if account else get_fallback_account()
        AARS.channel = channel if channel else "AARS_TEST"
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
        """
        for record in Record.__subclasses__():
            if record.get_indices():
                await record.regenerate_indices()

    @classmethod
    async def post_or_amend_object(cls, obj: R, channel: Optional[str] = None) -> R:
        """
        Posts or amends an object to Aleph. If the object is already posted, it's list of revision hashes is updated and
        the object receives the latest revision number.
        :param obj: The object to post or amend.
        :param channel: The channel to post the object to. If None, will use the configured channel.
        :return: The object, as it is now on Aleph.
        """
        if channel is None:
            channel = cls.channel
        assert isinstance(obj, Record)
        post_type = type(obj).__name__ if obj.id_hash is None else "amend"
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
        :param objs: The objects to forget.
        :param channel: The channel to delete the object from. If None, will use the TEST channel of the object.
        """
        if channel is None:
            channel = cls.channel
        hashes = []
        for obj in objs:
            assert obj.id_hash is not None
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
        page: int = 1,
    ) -> AsyncIterator[R]:
        """
        Retrieves posts as objects by its aleph item_hash.

        :param record_type: The type of the objects to retrieve.
        :param item_hashes: Aleph item_hashes of the objects to fetch.
        :param channel: Channel in which to look for it.
        :param owner: Account that owns the object.
        :param page_size: Number of items to fetch per page.
        :param page: Page number to fetch, based on page_size.
        """
        assert issubclass(record_type, Record)
        channels = None if channel is None else [channel]
        owners = None if owner is None else [owner]
        if item_hashes is None and channels is None and owners is None:
            channels = [cls.channel]

        if cls.cache and item_hashes is not None:
            # TODO: Add some kind of caching for channels and owners or add recent item_hashes endpoint to the Aleph API
            records = await cls._fetch_records_from_cache(record_type, item_hashes)
            cached_ids = []
            for r in records:
                cached_ids.append(r.id_hash)
                yield r
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
        item_hashes: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        owners: Optional[List[str]] = None,
        refs: Optional[List[str]] = None,
        page_size: int = 50,
        page: int = 1,
    ) -> AsyncIterator[R]:
        aleph_resp = None
        retries = cls.retry_count
        while aleph_resp is None:
            try:
                actual_page = page if page != -1 else 1
                aleph_resp = await cls.session.get_posts(
                    hashes=item_hashes,
                    channels=channels,
                    types=[record_type.__name__],
                    addresses=owners,
                    refs=refs,
                    pagination=page_size,
                    page=actual_page,
                )
                if page == 2:
                    print(record_type.__name__, item_hashes, channels, owners, refs, page_size, page)
                    print("aleph_resp", aleph_resp)
            except ServerDisconnectedError:
                retries -= 1
                if retries == 0:
                    raise
        for post in aleph_resp["posts"]:
            yield await record_type.from_dict(post)

        if page == -1:
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
        :param record_type: The type of the objects to retrieve.
        :param ref: item_hash of the object, whose revisions to fetch.
        :param channel: Channel in which to look for it.
        :param owner: Account that owns the object.
        :param page: Page number to fetch."""
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

        :param item_hash:
        :param record_type: The type of the objects to retrieve.
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
