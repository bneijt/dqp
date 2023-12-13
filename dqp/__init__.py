from importlib.metadata import version

__version__ = version(__name__)

"""A replay-able disk-backed iterator: the first time the iterator is written to disk, the second time it is read from disk"""

from collections import defaultdict
from email.policy import default
from functools import wraps
from pathlib import Path
import tempfile
import threading
from types import NoneType
from typing import Any, Callable, Iterable, Iterator, Optional, TypeVar, Union
from click import File
import msgpack
from hashlib import blake2b

T = TypeVar("T")  # Iterator element type

CACHE_LOCKS = defaultdict(threading.Lock)


def short_digest(value: str, digest_size: int = 8) -> str:
    """Return a short stable hash for the given string value.

    # Examples
    >>> short_digest("hello")
    'a7b6eda801e5347d'
    >>> short_digest("hello", 10)
    'a4366ac0442575d817cd'

    """
    return blake2b(value.encode("utf-8"), digest_size=digest_size).hexdigest()


def cached_iter(
    location: Path = None, base_path: Path = None
) -> Callable[[Callable[..., Iterator[T]]], Callable[..., Iterator[T]]]:
    """Decorator to cache the iterator to disk

    The default behavior is to store the iterator values in a temporary file, that is **not** deleted after the program exits.
    The name of this file is based on the name of the function and its location in the source code,
    using `short_digest` to avoid filesystem path length limits.


    Args:
        user_function (Iterator[T]): Use method to decorate
        location (Path, optional): Storage location of the iterator data. Defaults to None.
        base_path (Path, optional): Base path of automatically generated cache files. Defaults to None.

    Returns:
        _type_: _description_
    """

    def _decorator(
        user_function: Callable[..., Iterator[T]]
    ) -> Callable[..., Iterator[T]]:
        nonlocal location, base_path
        if location is None:
            if base_path is None:
                base_path = Path(tempfile.gettempdir())
            method_name = f"{user_function.__module__}.{user_function.__qualname__}"
            location = base_path / f"dqp_{short_digest(method_name)}.msgpacks"

        @wraps(user_function)
        def _wrapper(*args, **kwds) -> Iterator[T]:
            nonlocal location
            if not location.exists():
                return tee(user_function(*args, **kwds), location)
            else:
                return scan(location)

        def cache_clear():
            nonlocal location
            location.unlink(missing_ok=True)

        _wrapper.cache_clear = cache_clear

        return _wrapper

    return _decorator


def save(location: Union[Path, str], obj: T, append: bool = False) -> None:
    if isinstance(location, str):
        location = Path(location)
    global CACHE_LOCKS
    with CACHE_LOCKS[location]:
        packer = msgpack.Packer()
        if append:
            with location.open("ab") as disk_cache:
                disk_cache.write(packer.pack(obj))
        else:
            with location.open("wb") as disk_cache:
                disk_cache.write(packer.pack(obj))


def load(location: Union[Path, str]) -> Optional[Any]:
    if isinstance(location, str):
        location = Path(location)
    global CACHE_LOCKS
    with CACHE_LOCKS[location]:
        try:
            with location.open("rb") as disk_cache:
                for value in msgpack.Unpacker(disk_cache):
                    return value
        except FileNotFoundError:
            return None


def tee(iterable: Iterator[T], location: Path) -> Iterator[T]:
    """Also write iterator to disk at location"""
    global CACHE_LOCKS
    with CACHE_LOCKS[location]:
        packer = msgpack.Packer()
        try:
            with location.open("wb") as disk_cache:
                for value in iterable:
                    disk_cache.write(packer.pack(value))
                    yield value
        except Exception as e:
            location.unlink(missing_ok=True)
            raise e


def scan(location: Path) -> Iterator[T]:
    """Load iterator from file on disk at location"""
    global CACHE_LOCKS
    with CACHE_LOCKS[location]:
        with location.open("rb") as disk_cache:
            for value in msgpack.Unpacker(disk_cache):
                yield value


def fst(iterator: Optional[Union[Iterator[T], Iterable[T]]]) -> Optional[T]:
    """Return first element of optional iterator or iterable"""
    if isinstance(iterator, NoneType):
        return None
    elif isinstance(iterator, Iterator):
        return next(iterator, None)
    else:
        return next(iter(iterator), None)


def count_iter(iterator: Union[Iterator[T], Iterable[T]]) -> int:
    """Return number of elements in iterator or iterable"""
    if isinstance(iterator, Iterator):
        return sum(1 for _ in iterator)
    else:
        return sum(1 for _ in iter(iterator))
