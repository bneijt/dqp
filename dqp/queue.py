import itertools
import os
import time
from datetime import datetime, timezone
from hashlib import blake2s
from typing import Callable, Iterator, List, Optional, Tuple

import msgpack

from dqp.storage import Folder

QUEUE_FILE_HASH_SEPARATOR = "_"


class Sink:
    """Queue sink"""

    def __init__(self, base_path: str, head_timeout_seconds: int = 600):
        self.base_path = base_path
        self.head_timeout_seconds = head_timeout_seconds
        self.open(datetime.now(timezone.utc))

    def open(self, now: datetime) -> None:
        now_path = self.now_path(now)
        self.last_open_time = now
        os.makedirs(os.path.dirname(now_path), exist_ok=True)
        assert not os.path.exists(now_path), "Detached files are considered immutable"
        self.output_file = open(
            now_path,
            "wb",
        )
        self.packer = msgpack.Packer()
        self.output_path = now_path
        self.output_index = 0
        self.output_hash = blake2s()

    def write_dict(self, dictionary_value: dict):
        msg = self.packer.pack(dictionary_value)
        self.output_file.write(msg)
        self.output_file.flush()
        self.output_index += 1
        self.output_hash.update(msg)
        now = time.time()

        # Time based rotation
        if self.last_open_time.timestamp() + self.head_timeout_seconds < now:
            self.rotate()

    def now_path(self, now: datetime = datetime.now(timezone.utc)) -> str:
        return os.path.join(
            self.base_path,
            now.strftime("%Y/%m/%d/%H%M%S"),
        )

    def rotate(self):
        self.close()
        self.open(datetime.now(timezone.utc))

    def close(self):
        self.output_file.close()
        if self.output_index > 0:
            # finalize file, rename with .hash at the end.
            os.rename(
                self.output_path,
                self.output_path
                + QUEUE_FILE_HASH_SEPARATOR
                + self.output_hash.hexdigest(),
            )
        else:
            # Drop empty files
            os.unlink(self.output_path)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class Source:
    """Queue source"""

    def __init__(
        self, input_path: str, starting_from: Optional[Tuple[str, int]] = None
    ):
        """
        Read source at input_path starting from optional starting_from.
        """
        self.input_path = input_path.rstrip("/")
        assert self.input_path, "input path was empty after stripping the last /"
        self.starting_from = starting_from
        self.last = None  # type: Optional[Tuple[str, int]]

    def all_dict(self) -> Iterator[Tuple[str, int, dict]]:
        """iterator for all of the elements in the queue (ignoring starting_from if given)"""
        return itertools.chain.from_iterable(
            map(self.dicts_from, self.queue_filenames())
        )

    def __iter__(self):
        if self.starting_from is not None:
            return self.all_dict_from(self.starting_from[0], self.starting_from[1])
        return self.all_dict()

    def all_dict_from(
        self, queue_filename_prefix: str, idx: int = 0
    ) -> Iterator[Tuple[str, int, dict]]:
        """
        Start from a given position in the queues and continue
        """
        queue_iter = itertools.dropwhile(
            lambda fname: not fname.startswith(queue_filename_prefix),
            self.queue_filenames(),
        )
        return itertools.dropwhile(
            lambda el: el[1] < idx,
            itertools.chain.from_iterable(map(self.dicts_from, queue_iter)),
        )

    def unlink_to(self, queue_filename_prefix: Optional[str] = None) -> None:
        """Unlink queue files up to (not including) the given file name prefix or self.last if queue_filename_prefix is None"""
        if queue_filename_prefix is None and self.last is not None:
            queue_filename_prefix = self.last[0]
        if queue_filename_prefix is None:
            raise ValueError(
                f"Could not unlink to unknown prefix '{queue_filename_prefix}'"
            )

        found = next(
            (
                queue_filename
                for queue_filename in self.queue_filenames()
                if queue_filename.startswith(queue_filename_prefix)
            ),
            None,
        )
        if found is None:
            raise ValueError(
                f"Queue filename prefix '{queue_filename_prefix}' was not found in the queue filenames"
            )
        for queue_filename in self.queue_filenames():
            if queue_filename.startswith(queue_filename_prefix):
                break
            abs_path = os.path.join(self.input_path, queue_filename)
            os.unlink(abs_path)

    def queue_filenames(self) -> Iterator[str]:
        input_path_len = len(self.input_path) + 1  # path + slash character
        for root, dirnames, filenames in os.walk(self.input_path):
            dirnames.sort()
            filenames.sort()
            for filename in filenames:
                yield os.path.join(root, filename)[input_path_len:]

    def dicts_from(self, filename: str) -> Iterator[Tuple[str, int, dict]]:
        abs_path = os.path.join(self.input_path, filename)
        with open(abs_path, "rb") as queue_file:
            idx = 0
            for msg in msgpack.Unpacker(queue_file):
                self.last = filename, idx
                yield filename, idx, msg
                idx = idx + 1


class Project:
    """Management class for a base folder and queues storage conventions"""

    def __init__(self, base_path: str, vars_prefix: str = ""):
        self.storage_folder = Folder(base_path)
        self.closeables = []  # type: List[Callable[[], None]]
        self.vars_prefix = vars_prefix + "_" if len(vars_prefix) else vars_prefix

    def open_sink(self, name: str) -> Sink:
        sink = Sink(self.storage_folder.child(f"queue/{name}"))
        self.closeables.append(sink.close)
        return sink

    def open_source(
        self, name: str, starting_from: Optional[Tuple[str, int]] = None
    ) -> Source:
        src = Source(
            input_path=self.storage_folder.child(f"queue/{name}"),
            starting_from=starting_from,
        )

        def store_last():
            if src.last is not None:
                self.storage_folder.vars[
                    f"{self.vars_prefix}{name}_last_filename"
                ] = src.last[0]
                self.storage_folder.vars[f"{self.vars_prefix}{name}_last_idx"] = str(
                    src.last[1]
                )

        self.closeables.append(store_last)
        return src

    def continue_source(self, name: str) -> Source:
        last_filename = self.storage_folder.vars.get(
            f"{self.vars_prefix}{name}_last_filename"
        )
        starting_from = None
        if last_filename is not None:
            starting_from = (
                self.storage_folder.vars[f"{self.vars_prefix}{name}_last_filename"],
                int(self.storage_folder.vars[f"{self.vars_prefix}{name}_last_idx"]) + 1,
            )
        return self.open_source(name, starting_from)

    def state_folder(self, name: str) -> Folder:
        folder = Folder(self.storage_folder.child(f"state/{name}"))
        self.closeables.append(folder.close)
        return folder

    def close(self):
        for closer in self.closeables:
            closer()
        self.storage_folder.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
