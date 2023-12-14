import tempfile
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Iterator

import pytest

from dqp.disk_cache import cached_iter, count_iter, first, load, save, scan, tee


def test_should_hash_arguments() -> None:
    """The cache default hash should include function arguments."""

    with TemporaryDirectory() as temp_dir:

        @cached_iter(base_path=temp_dir)
        def repeater(count: int, value: str = "a") -> Iterator[tuple[int, str]]:
            for idx in range(count):
                yield [idx, value]

        assert list(repeater(2, value="a")) == [[0, "a"], [1, "a"]]
        assert list(repeater(2, value="a")) == [[0, "a"], [1, "a"]]
        assert list(repeater(2, value="b")) == [[0, "b"], [1, "b"]]
        assert list(repeater(2, value="a")) == [[0, "a"], [1, "a"]]
        assert list(repeater(1, value="a")) == [[0, "a"]]


def test_should_accept_clear() -> None:
    """The cache default hash should include function arguments."""

    with TemporaryDirectory() as temp_dir:
        value = "a"

        @cached_iter(base_path=temp_dir)
        def repeater() -> Iterator[tuple[int, str]]:
            nonlocal value
            for idx in range(2):
                yield [idx, value]

        assert list(repeater()) == [[0, "a"], [1, "a"]]
        value = "b"
        assert list(repeater()) == [
            [0, "a"],
            [1, "a"],
        ], "Before clear cache, we should still see the cache"
        repeater.cache_clear()
        assert list(repeater()) == [
            [0, "b"],
            [1, "b"],
        ], "After clear cache, we should see the new value"
        assert list(repeater()) == [[0, "b"], [1, "b"]]


def test_should_not_accept_tuple() -> None:
    """Msg pack cannot serialize tuples"""
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        with pytest.raises(ValueError):
            first(tee([(1, 2)], Path(temp_file.name)))
    with pytest.raises(ValueError):
        save("/won't/be/used", (1, 2))


def test_should_maintain_order() -> None:
    """The cache default hash should include function arguments."""

    with TemporaryDirectory() as temp_dir:

        @cached_iter(base_path=temp_dir)
        def test123() -> Iterator[str]:
            for i in range(3):
                yield i

        assert list(test123()) == [0, 1, 2]
        assert list(test123()) == [0, 1, 2]
        assert list(test123()) == [0, 1, 2]


def test_save_load() -> None:
    with NamedTemporaryFile() as temp_file:
        obj = [1, 2]
        save(temp_file.name, obj)
        assert load(temp_file.name) == obj


def test_save_append() -> None:
    with NamedTemporaryFile() as temp_file:
        temp_path = Path(temp_file.name)
        save(temp_path, 1, append=True)
        save(temp_path, 2, append=True)
        assert load(temp_file.name) == 1
        assert load(temp_file.name) == 1
        assert list(scan(temp_file.name)) == [1, 2]


def test_tee():
    with NamedTemporaryFile() as temp_file:
        iterable = [1, 2, 3]
        copy = []
        for value in tee(iterable, temp_file.name):
            copy.append(value)
        assert copy == iterable, "Should see the same"
        assert (
            list(scan(temp_file.name)) == iterable
        ), "Should have stored the same on disk"


def test_first():
    iterator = [1, 2, 3]
    assert first(iterator) == 1
    assert first(None) is None


def test_count_iter():
    iterator = [1, 2, 3]
    assert count_iter(iterator) == 3
    assert count_iter(None) == 0
