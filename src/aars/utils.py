"""
This module contains utility functions and classes for the AARS library.
Most notably it contains the [IndexQuery][aars.utils.IndexQuery] class,
which prepares a query for records using an [Index][aars.core.Index].

Also, it contains the [PageableResponse][aars.utils.PageableResponse] and
[PageableRequest][aars.utils.PageableRequest] classes, which allow for
easy pagination of queries and efficient retrieval of large amounts of data
in the asynchronous environment of AARS.
"""
import operator
from enum import Enum
from itertools import *
from typing import (AsyncIterator, Awaitable, Callable, Dict, Generic,
                    Iterator, List, Optional, OrderedDict, Tuple, Type,
                    TypeVar)

from .exceptions import AlreadyUsedError

T = TypeVar("T")


class Comparator(Enum):
    """
    An enum for the different comparison operators.
    """

    EQ = operator.eq
    IN = operator.contains


class EmptyAsyncIterator(AsyncIterator[T]):
    """
    An async iterator that can be returned when there are no results.
    """

    async def __anext__(self) -> T:
        raise StopAsyncIteration


class IndexQuery(OrderedDict, Generic[T]):
    """
    A query for a specific index. The keys are the index keys, and the values are the values to query for.
    It is an ordered dict in which the order is alphabetically determined by the keys, so that the same query
    will always have the same string representation. This is used to determine the index name.
    """

    record_type: Type[T]
    comparators: Dict[str, Comparator]

    def __init__(self, record_type: Type[T], **kwargs):
        """
        Create a new IndexQuery.
        Queries fields can use comparison suffixes to specify the type of comparison to use, like this:
        ```python
        IndexQuery(record_type=MyRecord, a__lt=1, b__in=[1, 2, 3])
        ```
        This would query for all records of type MyRecord where the field `a` is less than 1 and the field `b` is
        contained in the list [1, 2, 3].

        Args:
            record_type: The type of record that this query is for.
            **kwargs: The keys and values to query for.
        """
        # check if all kwargs are valid
        self.comparators = {}
        values = {}
        for item in sorted(kwargs.items()):
            if "__" in item[0]:
                key, comparator = item[0].split("__")
                self.comparators[key] = Comparator[comparator.upper()]
            else:
                key = item[0]
                self.comparators[key] = Comparator.EQ
            if key not in record_type.__annotations__:
                raise KeyError(
                    f"Invalid key '{key}' for record type {record_type.__name__}"
                )
            values[key] = item[1]
        super().__init__(values)
        self.record_type = record_type

    def get_index_name(self) -> str:
        """
        Get the name of the index that this query would use.
        Returns:
            The name of the index to query.
        """
        return self.record_type.__name__ + "." + ".".join(self.keys())

    def get_subquery(self, keys: List[str]) -> "IndexQuery":
        """
        Get a subquery of this query, containing only the specified keys.
        Example:
            ```python
            query = IndexQuery(record_type=MyRecord, a=1, b=2, c=3)
            subquery = query.get_subquery(["a", "c"])
            assert subquery == IndexQuery(record_type=MyRecord, a=1, c=3)
            ```
        Args:
            keys: The keys to include in the subquery.
        Returns:
            The subquery.
        """
        return IndexQuery(
            self.record_type,
            **{
                key + "__" + self.comparators[key].name: arg
                for key, arg in self.items()
                if key in keys
            },
        )

    def get_unfolded_queries(self) -> Iterator["IndexQuery"]:
        """
        Get all combinations of queries that arise from the IN comparators.
        Example:
            ```python
            query = IndexQuery(record_type=MyRecord, a__in=[1, 2], b__in=[3, 4])
            assert query.get_unfolded_queries() == [
                IndexQuery(record_type=MyRecord, a=1, b=3),
                IndexQuery(record_type=MyRecord, a=1, b=4),
                IndexQuery(record_type=MyRecord, a=2, b=3),
                IndexQuery(record_type=MyRecord, a=2, b=4),
            ]
            ```
        """
        for key, comparator in self.comparators.items():
            if comparator == Comparator.IN:
                for value in self[key]:
                    yield from IndexQuery(
                        self.record_type, **{key: value}
                    ).get_unfolded_queries()
            else:
                yield self


class PageableResponse(AsyncIterator[T], Generic[T]):
    """
    A wrapper around an AsyncIterator that allows for easy pagination and iteration, while also preventing multiple
    iterations. This is mainly used for nicer syntax when not using the async generator syntax.

    Example: Iterate over all records
        ```python
        async for record in PageableResponse(record_generator):
            print(record)
        ```

    Note: Consuming the generator
        Calling _any_ of the methods will consume the generator, so it is **not** possible to iterate over the records
        **after** calling any of the methods.

    Note: Iteration vs. Fetching all records at once
        If you plan to stop iterating over the records after a certain point, it is more efficient to use the
        `async for` syntax, as it will consume less calls to the Aleph Message API. If you want to get all records,
        AARS will automatically use the most efficient amount of API calls to do so.
    """

    record_generator: AsyncIterator[T]
    used: bool = False

    def __init__(self, record_generator: AsyncIterator[T]):
        self.record_generator = record_generator

    async def all(self) -> List[T]:
        """
        Fetch all records of this response.

        Example: Fetch all records
            ```python
            records = await PageableResponse(record_generator).all()
            ```
        Returns:
            A list of all records.
        """
        if self.used:
            raise AlreadyUsedError()
        self.used = True
        return await async_iterator_to_list(self.record_generator)

    async def page(self, page: int, page_size: int) -> List[T]:
        """
        Fetch a page of records.

        Example: Fetch a page of records
            ```python
            records = await PageableResponse(record_generator).page(2, 10)
            ```
        Args:
            page: The page number to fetch.
            page_size: The number of records per page.
        Returns:
            A list of records on the specified page.
        """
        if self.used:
            raise AlreadyUsedError()
        self.used = True
        return await async_iterator_to_list(
            self.record_generator, (page - 1) * page_size, page_size
        )

    async def first(self) -> Optional[T]:
        """
        Fetch the first record, which is usually the most recent record.

        Example: Fetch the first record
            ```python
            record = await PageableResponse(record_generator).first()
            ```
        Returns:
            The first record, or None if there are no records.
        """
        if self.used:
            raise AlreadyUsedError()
        self.used = True
        try:
            return await self.record_generator.__anext__()
        except StopAsyncIteration:
            return None

    def __anext__(self) -> Awaitable[T]:
        try:
            self.used = True
            return self.record_generator.__anext__()
        except StopAsyncIteration as e:
            raise e


class PageableRequest(AsyncIterator[T], Generic[T]):
    """
    A wrapper around a request that returns a PageableResponse. Useful if performance improvements can be obtained by
    passing page and page_size parameters to the request. Can be treated like
    [PageableResponse][aars.utils.PageableResponse].
    """

    func: Callable[..., AsyncIterator[T]]
    args: Tuple
    kwargs: Dict
    _response: Optional[PageableResponse] = None

    def __init__(self, func: Callable[..., AsyncIterator[T]], *args, **kwargs):
        """
        Create a new PageableRequest. The request will be triggered when the first record is requested.
        Args:
            func: The function to call to get the record generator.
            *args: The arguments to pass to the function.
            **kwargs: The keyword arguments to pass to the function.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        self._response = PageableResponse(self.func(*self.args, **self.kwargs))
        return self._response

    def __aiter__(self) -> AsyncIterator[T]:
        return self.response

    def __anext__(self) -> Awaitable[T]:
        return self.response.__anext__()

    @property
    def response(self):
        """
        The [PageableResponse][aars.utils.PageableResponse] of this request.
        """
        if self._response is None:
            # Trigger the request to get all records
            self._response = PageableResponse(
                self.func(*self.args, **self.kwargs, page=None, page_size=50)
            )
        return self._response

    async def all(self) -> List[T]:
        """
        Trigger the request and return all records.
        Returns:
            A list of all records.
        """
        return await self.response.all()

    async def page(self, page: int, page_size: int) -> List[T]:
        """
        Trigger the request and return a page of records.
        Args:
            page: The page number to fetch.
            page_size: The number of records per page.
        Returns:
            A list of records on the specified page.
        """
        self._response = PageableResponse(
            self.func(*self.args, **self.kwargs, page=page, page_size=page_size)
        )
        return await self.response.all()

    async def first(self) -> Optional[T]:
        """
        Trigger the request and return the first record.
        Returns:
            The first record, or None if there are no records.
        """
        self._response = PageableResponse(
            self.func(*self.args, **self.kwargs, page=1, page_size=1)
        )
        return await self.response.first()


def subslices(seq):
    """
    Return all contiguous non-empty subslices of a sequence.
    Taken from more_itertools.

    Example:
        ```python
        list(subslices([1, 2, 3])) == [[1], [1, 2], [1, 2, 3], [2], [2, 3], [3]]
        ```
    """
    #
    slices = starmap(slice, combinations(range(len(seq) + 1), 2))
    return map(operator.getitem, repeat(seq), slices)


def possible_index_names(seq):
    """
    Return all possible index names for a sequence of properties.

    Example:
        ```python
        list(possible_index_names(['A', 'B', 'C'])) == [['A'], ['A.B'], ['A.B.C'], ['B'], ['B.C'], ['C']]
        ```
    """
    return map(".".join, subslices(seq))


async def async_iterator_to_list(
    iterator: AsyncIterator[T], skip: int = 0, count: Optional[int] = None
) -> List[T]:
    """
    Return a list from an async iterator.
    Args:
        iterator: The async iterator to convert to a list.
        skip: The number of items to skip.
        count: The maximum number of items to return.
    Returns:
        A list of items.
    """
    if count is None and skip == 0:
        return [item async for item in iterator]
    else:
        items = []
        async for item in iterator:
            if skip > 0:
                skip -= 1
                continue

            items.append(item)
            if len(items) == count:
                break
        return items
