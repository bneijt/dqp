"""Various methods to allow for easily caching Python values to disk in msgpack format.

This module supports transparently caching iterator values to disk using the `cached_iter` decorator,
or simply storing and loading to and from disk explicitly.

You can simply write an expensive iterator, decorate it with `cached_iter` and it will be cached to disk.
"""
import tempfile
import threading
from collections import defaultdict
from functools import wraps
from hashlib import blake2b
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional, TypeVar, Union

import msgpack

T = TypeVar("T")  # Iterator element type

_CACHE_LOCKS = defaultdict(threading.Lock)


def short_digester(digest_size: int = 8) -> str:
    """Return a short stable hash for the given string value.

    A shorthand for de default is `short_digest`

    # Examples
    >>> short_digester()("hello")
    '922c57d1cceebf8d'
    >>> short_digester(digest_size=10)("hello")
    '7adb7bb284a089b81d73'
    >>> short_digest("hello")
    '922c57d1cceebf8d'
    """

    def _digest(*args, **kwargs) -> str:
        nonlocal digest_size
        return blake2b(
            (repr(args) + "#" + repr(kwargs)).encode("utf-8"), digest_size=digest_size
        ).hexdigest()

    return _digest


short_digest: Callable[..., str] = short_digester()
"""Calculate a string hash of the arguments

This method is the default digester returned by `short_digester`.

All position and keyword arguments are hashed using `blake2b` into a small hexadecimal string.
The implementation is not fast and uses `repr` to stringify the values, so this might not even
be stable across runs for complex objects where `repr` only sees an object type and memory address.
"""


def cached_iter(
    get_storage_location: Optional[Callable[[str, list, dict], Path]] = None,
    base_path: Optional[Union[Path, str]] = None,
) -> Callable[[Callable[..., Iterator[T]]], Callable[..., Iterator[T]]]:
    """Decorator to cache an iterator to disk

    The default behavior is to store the iterator values in a temporary file, that is **not** deleted after the program exits.
    The name of this file is based on the name of the function and its location in the source code and iterator arguments,
    using `short_digest` to avoid filesystem path length limits.



    Args:
        get_storage_location (Callable[[str, list, dict], Path], optional):
            Used to determine the location of the cache file.
            Will receive iterator method name, arguments and keyword arguments. Is expected to return a `pathlib.Path` where the cache is stored.
            Defaults to `None`.
        base_path (Union[Path, str], optional):
            Base path of automatically generated cache files.
            If `None`, the system default temporary storage location is used.
            Defaults to `None`.

    Returns:
        decorator for an iterator.

    # Examples
    >>> import time
    >>> from dqp.disk_cache import cached_iter
    >>> @cached_iter()
    ... def slow_iter() -> Iterator[int]:
    ...     for i in range(3):
    ...         time.sleep(1)
    ...         yield i
    >>> list(slow_iter())
    [0, 1, 2]
    >>> list(slow_iter())
    [0, 1, 2]
    >>> list(slow_iter())
    [0, 1, 2]
    """
    if base_path is None:
        base_path = Path(tempfile.gettempdir())
    elif isinstance(base_path, str):
        base_path = Path(base_path)

    if get_storage_location is None:

        def get_storage_location(method_name, args, kwds) -> Path:
            nonlocal base_path
            return (
                base_path / f"dqp_{short_digest(method_name, *args, **kwds)}.msgpacks"
            )

    def _decorator(
        user_function: Callable[..., Iterator[T]]
    ) -> Callable[..., Iterator[T]]:
        method_name = f"{user_function.__module__}.{user_function.__qualname__}"

        @wraps(user_function)
        def _wrapper(*args, **kwds) -> Iterator[T]:
            nonlocal method_name, get_storage_location
            location = get_storage_location(method_name, args, kwds)
            if not location.exists():
                return tee(user_function(*args, **kwds), location)
            else:
                return scan(location)

        def cache_clear(*args, **kwds):
            nonlocal get_storage_location, method_name
            get_storage_location(method_name, args, kwds).unlink(missing_ok=True)

        _wrapper.cache_clear = cache_clear

        return _wrapper

    return _decorator


def save(location: Union[Path, str], obj: T, append: bool = False) -> None:
    """Save object to disk at location

    Args:
        location (Union[Path, str]): Where to store object
        obj (T): Object to store
        append (bool, optional): Append object to file instead of overwrite. Defaults to False.

    Raises:
        ValueError: _description_
    """
    if isinstance(location, str):
        location = Path(location)
    if isinstance(obj, tuple):
        raise ValueError(
            "msgpack does not support tuples, they would be read back as lists"
        )
    global _CACHE_LOCKS
    with _CACHE_LOCKS[location]:
        packer = msgpack.Packer()
        if append:
            with location.open("ab") as disk_cache:
                disk_cache.write(packer.pack(obj))
        else:
            with location.open("wb") as disk_cache:
                disk_cache.write(packer.pack(obj))


def load(location: Union[Path, str]) -> Optional[Any]:
    """Load object from disk at location

    Args:
        location (Union[Path, str]): Location to load object from

    Returns:
        Optional[Any]: Either None or the loaded object
    """
    if isinstance(location, str):
        location = Path(location)
    global _CACHE_LOCKS
    with _CACHE_LOCKS[location]:
        try:
            with location.open("rb") as disk_cache:
                for value in msgpack.Unpacker(disk_cache):
                    return value
        except FileNotFoundError:
            return None


def tee(iterable: Iterator[T], location: Union[Path, str]) -> Iterator[T]:
    """Write iterator to disk at the specified location while yielding values.

    Args:
        iterable (Iterator[T]): The input iterator to be written to disk.
        location (Union[Path, str]): The location where the iterator will be written.

    Yields:
        T: The values from the input iterator.

    Raises:
        ValueError: If the value in the iterator is a tuple (msgpack does not support tuples).
    """
    global _CACHE_LOCKS
    if isinstance(location, str):
        location = Path(location)
    with _CACHE_LOCKS[location]:
        packer = msgpack.Packer()
        try:
            with location.open("wb") as disk_cache:
                for value in iterable:
                    if isinstance(value, tuple):
                        raise ValueError(
                            "msgpack does not support tuples, they would be read back as lists"
                        )
                    disk_cache.write(packer.pack(value))
                    yield value
        except Exception as e:
            location.unlink(missing_ok=True)
            raise e


def scan(location: Path) -> Iterator[T]:
    """Load iterator from file on disk at location"""
    global _CACHE_LOCKS
    if isinstance(location, str):
        location = Path(location)
    with _CACHE_LOCKS[location]:
        with location.open("rb") as disk_cache:
            for value in msgpack.Unpacker(disk_cache):
                yield value


def first(iterator: Optional[Union[Iterator[T], Iterable[T]]]) -> Optional[T]:
    """Return first element of optional iterator or iterable"""
    if iterator is None:
        return None
    elif isinstance(iterator, Iterator):
        return next(iterator, None)
    else:
        return next(iter(iterator), None)


def count_iter(iterator: Union[Iterator[T], Iterable[T]]) -> int:
    """Return number of elements in iterator or iterable

    Mostly useful for debugging. As this will consume the iterator, you should only use it on cached iterators.
    """
    if isinstance(iterator, Iterator):
        return sum(1 for _ in iterator)
    else:
        return sum(1 for _ in iter(iterator))
