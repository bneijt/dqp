import time
from tempfile import TemporaryDirectory

import pytest

from dqp.queue import Sink, Source


def test_all_dict():
    with TemporaryDirectory() as temp_dir:
        with Sink(temp_dir) as temp_sink:
            temp_sink.write_dict({"a": 1})
            temp_sink.write_dict({"b": 2})
            temp_sink.write_dict({"c": 3})
        s = Source(temp_dir)
        b_element = list(s.all_dict())[1]
        assert b_element[2] == {"b": 2}, "Second element should be the 'b' element"
        assert len(list(s.all_dict())) == 3, "Should have 3 elements on disk"
        assert (
            len(list(s.from_dict(b_element[0], 0))) == 3
        ), "Should have 3 elements if we iterate from index 0"
        assert b_element[1] == 1, "B should be the second element in the queue"
        assert next(s.from_dict(b_element[0], b_element[1]))[2] == {"b": 2}
        assert (
            len(list(s.from_dict(b_element[0], 3))) == 0
        ), "Should be empty after last element"


def test_last_should_be_relative():
    with TemporaryDirectory() as temp_dir:
        with Sink(temp_dir) as temp_sink:
            temp_sink.write_dict({"a": 1})
            time.sleep(1)  # Need next second in queue file name
            temp_sink.rotate()
            temp_sink.write_dict({"b": 2})
            temp_sink.write_dict({"c": 3})

        s = Source(temp_dir)
        for fn in s.queue_filenames():
            assert not fn.startswith("/"), "Must be relative"

        assert len(list(s.all_dict())) == 3, "Must have entries"
        assert s.last is not None and not s.last[0].startswith(
            "/"
        ), "Last path should be relative"
        assert temp_dir not in s.last[0], "Should not know the temp_dir"
        assert len([s.dicts_from(s.last[0])]), "Should be able to read last dicts"
        with pytest.raises(ValueError):
            s.unlink_to("does not exist")
        s.unlink_to(s.last[0])

        assert len(list(s.all_dict())) == 2, "Must have only the last block"
