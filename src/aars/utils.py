import operator
from itertools import *
from typing import (
    AsyncIterator,
    List,
    TypeVar,
    OrderedDict,
    Generic,
    Type,
    Optional,
    Awaitable,
    Callable,
    Tuple,
    Dict,
)

from .exceptions import AlreadyUsedError

T = TypeVar("T")


class EmptyAsyncIterator(AsyncIterator[T]):
    async def __anext__(self) -> T:
        raise StopAsyncIteration


class IndexQuery(OrderedDict, Generic[T]):
    record_type: Type[T]

    def __init__(self, record_type: Type[T], **kwargs):
        super().__init__({item[0]: item[1] for item in sorted(kwargs.items())})
        self.record_type = record_type

    def get_index_name(self) -> str:
        return self.record_type.__name__ + "." + ".".join(self.keys())

    def get_subquery(self, keys: List[str]) -> "IndexQuery":
        return IndexQuery(
            self.record_type, **{key: arg for key, arg in self.items() if key in keys}
        )


class PageableResponse(AsyncIterator[T], Generic[T]):
    """
    A wrapper around an AsyncIterator that allows for easy pagination and iteration, while also preventing multiple
    iterations. This is mainly used for nicer syntax when not using the async generator syntax.
    """
    record_generator: AsyncIterator[T]
    used: bool = False

    def __init__(self, record_generator: AsyncIterator[T]):
        self.record_generator = record_generator

    async def all(self) -> List[T]:
        if self.used:
            raise AlreadyUsedError()
        self.used = True
        return await async_iterator_to_list(self.record_generator)

    async def page(self, page: int, page_size: int) -> List[T]:
        if self.used:
            raise AlreadyUsedError()
        self.used = True
        return await async_iterator_to_list(
            self.record_generator, page * page_size, page_size
        )

    async def first(self) -> Optional[T]:
        if self.used:
            raise AlreadyUsedError()
        self.used = True
        return await self.record_generator.__anext__()

    def __anext__(self) -> Awaitable[T]:
        try:
            self.used = True
            return self.record_generator.__anext__()
        except StopAsyncIteration:
            raise


class PageableRequest(AsyncIterator[T], Generic[T]):
    """
    A wrapper around a request that returns a PageableResponse. Useful if performance improvements can be obtained by
    passing page and page_size parameters to the request.
    """

    func: Callable[..., AsyncIterator[T]]
    args: Tuple
    kwargs: Dict
    response: Optional[PageableResponse] = None

    def __init__(self, func: Callable[..., AsyncIterator[T]], *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        self.response = PageableResponse(self.func(*self.args, **self.kwargs))
        return self.response

    def __aiter__(self) -> AsyncIterator[T]:
        self._prepare_response()
        assert self.response is not None
        return self.response

    def __anext__(self) -> Awaitable[T]:
        return PageableResponse(self.func(*self.args, **self.kwargs, page=1, page_size=20)).__anext__()

    def _prepare_response(self):
        if self.response is not None:
            return self.response
        self.response = PageableResponse(self.func(*self.args, **self.kwargs, page=1, page_size=20))

    async def all(self) -> List[T]:
        return await PageableResponse(
            self.func(*self.args, **self.kwargs, page=1, page_size=20)
        ).all()

    async def page(self, page, page_size) -> List[T]:
        return await PageableResponse(
            self.func(*self.args, **self.kwargs, page=page, page_size=page_size)
        ).page(page=page, page_size=page_size)

    async def first(self) -> Optional[T]:
        return await PageableResponse(
            self.func(*self.args, **self.kwargs, page=1, page_size=20)
        ).first()


def subslices(seq):
    """
    Return all contiguous non-empty subslices of a sequence.
    Taken from more_itertools.

    Example:
        list(subslices([1, 2, 3])) == [[1], [1, 2], [1, 2, 3], [2], [2, 3], [3]]
    """
    #
    slices = starmap(slice, combinations(range(len(seq) + 1), 2))
    return map(operator.getitem, repeat(seq), slices)


def possible_index_names(seq):
    """
    Return all possible index names for a sequence of properties.

    Example:
        list(possible_index_names(['A', 'B', 'C'])) == [['A'], ['A.B'], ['A.B.C'], ['B'], ['B.C'], ['C']]
    """
    return map(".".join, subslices(seq))


async def async_iterator_to_list(
    iterator: AsyncIterator[T], skip: int = 0, count: Optional[int] = None
) -> List[T]:
    """
    Return a list from an async iterator.
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
