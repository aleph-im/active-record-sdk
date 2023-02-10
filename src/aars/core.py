import asyncio
import math
import warnings
from abc import ABC
from collections import OrderedDict
from operator import attrgetter
from typing import Type, TypeVar, Dict, ClassVar, List, Set, Any, Union, Tuple, Optional, Generic, AsyncIterator

import aiohttp
from aiohttp import ServerDisconnectedError
from aleph_client.vm.cache import VmCache
from aleph_message.models import PostMessage
from pydantic import BaseModel

import aleph_client.asynchronous as client
from aleph_client.types import Account
from aleph_client.chains.ethereum import get_fallback_account
from aleph_client.conf import settings

from .utils import subslices, async_iterator_to_list, IndexQuery
from .exceptions import AlreadyForgottenError

R = TypeVar('R', bound='Record')


class Record(BaseModel, ABC):
    """
    A basic record which is persisted on Aleph decentralized storage.

    Records can be updated: revision numbers begin at 0 (original upload) and increment for each `upsert()` call.

    Previous revisions can be restored by calling `fetch_revision(rev_no=<number>)` or `fetch_revision(
    rev_hash=<item_hash of inserted update>)`.

    They can also be forgotten: Aleph will ask the network to forget given item, in order to allow for GDPR-compliant
    applications.

    Records have an `indices` class attribute, which allows one to select an index and query it with a key.
    """
    forgotten: bool = False
    id_hash: Optional[str] = None
    current_revision: Optional[int] = None
    revision_hashes: List[str] = []
    __indices: ClassVar[Dict[str, 'Index']] = {}

    def __repr__(self):
        return f'{type(self).__name__}({self.id_hash})'

    def __str__(self):
        return f'{type(self).__name__} {self.__dict__}'

    @property
    def content(self) -> Dict[str, Any]:
        """
        :return: content dictionary of the object, as it is to be stored on Aleph.
        """
        return self.dict(exclude={'id_hash', 'current_revision', 'revision_hashes', 'forgotten'})

    async def update_revision_hashes(self: R):
        """
        Updates the list of available revision hashes, in order to fetch these.
        """
        assert self.id_hash is not None
        self.revision_hashes = [self.id_hash] + await async_iterator_to_list(AARS.fetch_revisions(type(self), ref=self.id_hash))
        if self.current_revision is None:
            # latest revision
            self.current_revision = len(self.revision_hashes) - 1

    async def fetch_revision(self: R, rev_no: Optional[int] = None, rev_hash: Optional[str] = None) -> R:
        """
        Fetches a revision of the object by revision number (0 => original) or revision hash.
        :param rev_no: the revision number of the revision to fetch.
        :param rev_hash: the hash of the revision to fetch.
        """
        assert self.id_hash is not None, 'Cannot fetch revision of an object which has not been posted yet.'
        assert self.current_revision is not None

        if rev_no is not None:
            if rev_no < 0:
                rev_no = len(self.revision_hashes) + rev_no
            if self.current_revision == rev_no:
                return self
            elif rev_no > len(self.revision_hashes):
                raise IndexError(f'No revision no. {rev_no} found for {self}')
            else:
                self.current_revision = rev_no
        elif rev_hash is not None:
            if rev_hash == self.revision_hashes[self.current_revision]:
                return self
            try:
                self.current_revision = self.revision_hashes.index(rev_hash)
            except ValueError:
                raise IndexError(f'{rev_hash} is not a revision of {self}')
        else:
            raise ValueError('Either rev or hash must be provided')

        resp = await AARS.fetch_exact(
            type(self),
            self.revision_hashes[self.current_revision]
        )
        self.__dict__.update(resp.content)

        return self

    async def save(self):
        """
        Posts a new item to Aleph or amends it, if it was already posted. Will add new items to local indices.
        For indices to be persisted on Aleph, you need to call `save()` on the index itself or `cls.save_indices()`.
        """
        await AARS.post_or_amend_object(self)
        if self.current_revision == 0:
            [index.add_record(self) for index in self.get_indices()]
        return self

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
            obj.id_hash = post.content.ref
        await obj.update_revision_hashes()
        assert obj.id_hash is not None
        obj.current_revision = obj.revision_hashes.index(obj.id_hash)
        return obj

    @classmethod
    async def from_dict(cls: Type[R], post: Dict[str, Any]) -> R:
        """
        Initializes a record object from its raw Aleph data.
        :post: Raw Aleph data.
        """
        obj = cls(**post['content'])
        if post.get('ref') is None:
            obj.id_hash = post['item_hash']
        else:
            obj.id_hash = post['ref']
        await obj.update_revision_hashes()
        assert obj.id_hash is not None
        obj.current_revision = obj.revision_hashes.index(obj.id_hash)
        return obj

    @classmethod
    async def fetch(cls: Type[R], hashes: Union[str, List[str]]) -> List[R]:
        """
        Fetches one or more objects of given type by its/their item_hash[es].
        """
        if not isinstance(hashes, List):
            hashes = [hashes]
        return await async_iterator_to_list(AARS.fetch_records(cls, list(hashes)))

    @classmethod
    async def fetch_all(cls: Type[R]) -> List[R]:
        """
        Fetches all objects of given type.
        """
        return await async_iterator_to_list(AARS.fetch_records(cls))

    @classmethod
    async def where_eq(cls: Type[R], **kwargs) -> List[R]:
        """
        Queries an object by given properties through an index, in order to fetch applicable records.
        An index name is defined as '<object_class>.[<object_properties>.]' and is initialized by creating
        an Index instance, targeting a BaseRecord class with a list of properties.

        >>> Index(MyRecord, ['property1', 'property2'])

        This will create an index named 'MyRecord.property1.property2' which can be queried with:

        >>> MyRecord.where_eq(property1='value1', property2='value2')

        If no index is defined for the given properties, an IndexError is raised.

        If only a part of the keys is indexed for the given query, a fallback index is used and locally filtered.
        """
        query = IndexQuery(cls, **kwargs)
        index = cls.get_index(query.get_index_name())
        return await index.lookup(query)

    @classmethod
    async def where_gte(cls: Type[R], **kwargs) -> List[R]:
        raise NotImplementedError()

    @classmethod
    def add_index(cls: Type[R], index: 'Index') -> None:
        cls.__indices[repr(index)] = index

    @classmethod
    def get_index(cls: Type[R], index_name: str) -> 'Index[R]':
        """
        Returns an index or any of its subindices by its name. The name is defined as
        '<object_class>.[<object_properties>.]' with the properties being sorted alphabetically. For example,
        Book.author.title is a valid index name, while Book.title.author is not.
        :param index_name: The name of the index to fetch.
        :return: The index instance or a subindex.
        """
        index = cls.__indices.get(index_name)
        if index is None:
            key_subslices = subslices(list(index_name.split('.')[1:]))
            # returns all plausible combinations of keys
            key_subslices = sorted(key_subslices, key=lambda x: len(x), reverse=True)
            for keys in key_subslices:
                name = cls.__name__ + '.' + '.'.join(keys)
                if cls.__indices.get(name):
                    warnings.warn(f'No index {index_name} found. Using {name} instead.')
                    return cls.__indices[name]
            raise IndexError(f'No index or subindex for {index_name} found.')
        return index

    @classmethod
    def get_indices(cls: Type[R]) -> List['Index']:
        if cls == Record:
            return list(cls.__indices.values())
        return [index for index in cls.__indices.values() if index.record_type == cls]

    @classmethod
    async def save_indices(cls: Type[R]) -> None:
        """Updates all indices of given type."""
        tasks = [index.save() for index in cls.get_indices()]
        await asyncio.gather(*tasks)

    @classmethod
    async def regenerate_indices(cls: Type[R]) -> None:
        """Regenerates all indices of given type."""
        items = await cls.fetch_all()
        for index in cls.get_indices():
            index.regenerate(items)


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
        It is not recommended using the index directly, but rather through the `query` method of the `Record` class like
        so:

        >>> MyRecord.query(foo='bar')

        This returns all records of type MyRecord where foo is equal to 'bar'.

        :param record_type: The record_type to index.
        :param on: The properties to index on.
        """
        if isinstance(on, str):
            on = [on]
        super(Index, self).__init__(record_type=record_type, index_on=sorted(on))
        record_type.add_index(self)

    def __str__(self):
        return f"Index({self.record_type.__name__}.{'.'.join(self.index_on)})"

    def __repr__(self):
        return f"{self.record_type.__name__}.{'.'.join(self.index_on)}"

    async def lookup(self, query: IndexQuery) -> List[R]:
        """
        Fetches records with given values for the indexed properties.

        :param query: The query to lookup items with.
        """
        assert query.record_type == self.record_type
        id_hashes: Optional[Set[str]]
        needs_filtering = False

        if len(self.index_on) == 1:
            assert (list(query.keys())[0]) == self.index_on[0]
            id_hashes = self.hashmap.get(tuple(query.values()))
        else:
            subquery = query
            if repr(self) != query.get_index_name():
                subquery = query.get_subquery(self.index_on)
                needs_filtering = True
            id_hashes = self.hashmap.get(tuple(subquery.values()))

        if id_hashes is None:
            return []

        items = await async_iterator_to_list(AARS.fetch_records(self.record_type, list(id_hashes)))

        if needs_filtering:
            return self._filter_index_items(items, query)
        return items

    @classmethod
    def _filter_index_items(cls, items: List[R], query: IndexQuery) -> List[R]:
        sorted_keys = query.keys()
        filtered_items = list()
        for item in items:
            # eliminate the item which does not fulfill the properties
            class_properties = vars(item)
            required_class_properties = {key: class_properties.get(key) for key in sorted_keys}
            if required_class_properties == dict(query):
                filtered_items.append(item)
        return filtered_items

    def add_record(self, obj: R):
        """Adds a record to the index."""
        assert issubclass(type(obj), Record)
        assert obj.id_hash is not None
        key = attrgetter(*self.index_on)(obj)
        self.hashmap[key].add(obj.id_hash)

    def remove_record(self, obj: R):
        """Removes a record from the index, i.e. when it is forgotten."""
        assert obj.id_hash is not None
        key = attrgetter(*self.index_on)(obj)
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
    session: Optional[aiohttp.ClientSession]
    cache: Optional[VmCache]

    def __init__(self,
                 account: Optional[Account] = None,
                 channel: Optional[str] = None,
                 api_url: Optional[str] = None,
                 session: Optional[aiohttp.ClientSession] = None,
                 cache: Optional[VmCache] = None,
                 retry_count: Optional[int] = None):
        """
        Initializes the SDK with an account and a channel.
        :param cache: Whether to use Aleph VM caching when running AARS code.
        :param account: Account with which to sign the messages.
        :param channel: Channel to which to send the messages.
        :param api_url: The API URL to use. Defaults to an official Aleph API host.
        :param session: An aiohttp session to use. Defaults to a new session.
        """
        AARS.account = account if account else get_fallback_account()
        AARS.channel = channel if channel else 'AARS_TEST'
        AARS.api_url = api_url if api_url else settings.API_HOST
        AARS.session = session if session else None
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
    async def post_or_amend_object(cls,
                                   obj: R,
                                   account: Optional[str] = None,
                                   channel: Optional[str] = None) -> R:
        """
        Posts or amends an object to Aleph. If the object is already posted, it's list of revision hashes is updated and
        the object receives the latest revision number.
        :param obj: The object to post or amend.
        :param account: The account to post the object with. If None, will use configured account.
        :param channel: The channel to post the object to. If None, will use the configured channel.
        :return: The object, as it is now on Aleph.
        """
        if account is None:
            account = cls.account
        if channel is None:
            channel = cls.channel
        assert isinstance(obj, Record)
        post_type = type(obj).__name__ if obj.id_hash is None else "amend"
        resp = await client.create_post(account=account,
                                        post_content=obj.content,
                                        post_type=post_type,
                                        channel=channel,
                                        ref=obj.id_hash,
                                        api_server=cls.api_url,
                                        session=cls.session)
        if obj.id_hash is None:
            obj.id_hash = resp.item_hash
        obj.revision_hashes.append(resp.item_hash)
        obj.current_revision = len(obj.revision_hashes) - 1
        if cls.cache:
            await cls.cache.set(resp.item_hash, obj.json())
        return obj

    @classmethod
    async def forget_objects(cls, objs: List[R], account: Optional[Account] = None, channel: Optional[str] = None):
        """
        Forgets multiple objects from Aleph and local cache. All related revisions will be forgotten too.
        :param objs: The objects to forget.
        :param account: The account to delete the object with. If None, will use the fallback account.
        :param channel: The channel to delete the object from. If None, will use the TEST channel of the object.
        """
        if account is None:
            account = cls.account
        if channel is None:
            channel = cls.channel
        hashes = []
        for obj in objs:
            assert obj.id_hash is not None
            hashes += [obj.id_hash] + obj.revision_hashes
        forget_task = client.forget(account=account,
                                    hashes=hashes,
                                    reason=None,
                                    channel=channel,
                                    api_server=cls.api_url,
                                    session=cls.session)
        if cls.cache:
            await asyncio.gather(
                forget_task,
                *[cls.cache.delete(h) for h in hashes]
            )
        else:
            await forget_task

    @classmethod
    async def fetch_records(cls,
                            record_type: Type[R],
                            item_hashes: Optional[List[str]] = None,
                            channel: Optional[str] = None,
                            owner: Optional[str] = None) -> AsyncIterator[R]:
        """
        Retrieves posts as objects by its aleph item_hash.

        :param record_type: The type of the objects to retrieve.
        :param item_hashes: Aleph item_hashes of the objects to fetch.
        :param channel: Channel in which to look for it.
        :param owner: Account that owns the object.
        """
        assert issubclass(record_type, Record)
        channels = None if channel is None else [channel]
        owners = None if owner is None else [owner]
        if item_hashes is None and channels is None and owners is None:
            channels = [cls.channel]

        if cls.cache and item_hashes is not None:
            records = await cls._fetch_records_from_cache(record_type, item_hashes)
            cached_ids = []
            for r in records:
                cached_ids.append(r.id_hash)
                yield r
            item_hashes = [h for h in item_hashes if h not in cached_ids]
            if len(item_hashes) == 0:
                return

        async for record in cls._fetch_records_from_api(record_type=record_type,
                                                        item_hashes=item_hashes,
                                                        channels=channels,
                                                        owners=owners):
            yield record

    @classmethod
    async def _fetch_records_from_cache(cls, record_type: Type[R], item_hashes: List[str]) -> List[R]:
        assert cls.cache
        raw_records = await asyncio.gather(*[cls.cache.get(h) for h in item_hashes])
        return [record_type.parse_raw(r) for r in raw_records if r is not None]

    @classmethod
    async def _fetch_records_from_api(cls,
                                      record_type: Type[R],
                                      item_hashes: Optional[List[str]] = None,
                                      channels: Optional[List[str]] = None,
                                      owners: Optional[List[str]] = None,
                                      refs: Optional[List[str]] = None,
                                      page=1) -> AsyncIterator[R]:
        aleph_resp = None
        retries = cls.retry_count
        while aleph_resp is None:
            try:
                aleph_resp = await client.get_posts(hashes=item_hashes,
                                                    channels=channels,
                                                    types=[record_type.__name__],
                                                    addresses=owners,
                                                    refs=refs,
                                                    api_server=cls.api_url,
                                                    session=cls.session,
                                                    pagination=50,
                                                    page=page)
            except ServerDisconnectedError:
                retries -= 1
                if retries == 0:
                    raise
        for post in aleph_resp['posts']:
            yield await record_type.from_dict(post)

        if page == 1:
            # If there are more pages, fetch them
            total_items = aleph_resp['pagination_total']
            per_page = aleph_resp['pagination_per_page']
            if total_items > per_page:
                for next_page in range(2, math.ceil(total_items / per_page) + 1):
                    async for record in cls._fetch_records_from_api(
                            record_type=record_type,
                            item_hashes=item_hashes,
                            channels=channels,
                            owners=owners,
                            refs=refs,
                            page=next_page):
                        yield record

    @classmethod
    async def fetch_revisions(cls,
                              record_type: Type[R],
                              ref: str,
                              channel: Optional[str] = None,
                              owner: Optional[str] = None,
                              page=1) -> AsyncIterator[str]:
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
                aleph_resp = await client.get_messages(channels=channels,
                                                       addresses=owners,
                                                       refs=[ref],
                                                       api_server=cls.api_url,
                                                       session=cls.session,
                                                       pagination=50)
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
                            page=next_page):
                        yield item_hash

    @classmethod
    async def fetch_exact(cls,
                          record_type: Type[R],
                          item_hash: str) -> R:
        """Retrieves the revision of an object by its item_hash of the message. The content will be exactly the same
        as in the referenced message, so no amendments will be applied.

        :param item_hash:
        :param record_type: The type of the objects to retrieve.
        """
        if cls.cache:
            cache_resp = await cls._fetch_records_from_cache(record_type, [item_hash])
            if len(cache_resp) > 0:
                return cache_resp[0]
        aleph_resp = await client.get_messages(hashes=[item_hash],
                                               api_server=cls.api_url,
                                               session=cls.session)
        if len(aleph_resp.messages) == 0:
            raise ValueError(f"Message with hash {item_hash} not found.")
        message: PostMessage = aleph_resp.messages[0]
        return await record_type.from_post(message)
