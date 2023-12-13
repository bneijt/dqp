# Disk Queue Processing (dqp)

This library makes it easy to serialize Python to disk using msgpack.

The library contains two modules:

- `dqp.disk_cache`: to easily read/write data to disk and support caching iterators to disk.
- `dqp.disk_queue`: an approach to communicating lists of objects between runs with index/offset metadata.

## Example of disk_cache

```python
from typing import Iterator
from dqp.disk_cache import cached_iter
import time

@cached_iter()
def expensive_iter() -> Iterator[int]:
    for idx in range(5):
        time.sleep(1)
        yield idx

def main():
    #For replay, drop the cache
    expensive_iter.cache_clear()

    # First time it takes seconds
    for i in expensive_iter():
        print(time.asctime(), i)

    # Other times the cache is already there
    print(time.asctime(), list(expensive_iter()))
    print(time.asctime(), list(expensive_iter()))
    print(time.asctime(), list(expensive_iter()))

if __name__ == "__main__":
    main()

```
