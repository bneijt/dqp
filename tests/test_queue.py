import os
import time
from tempfile import TemporaryDirectory

import pytest

from dqp.queue import Project, Sink, Source


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
            len(list(s.all_dict_from(b_element[0], 0))) == 3
        ), "Should have 3 elements if we iterate from index 0"
        assert b_element[1] == 1, "B should be the second element in the queue"
        assert next(s.all_dict_from(b_element[0], b_element[1]))[2] == {"b": 2}
        assert (
            len(list(s.all_dict_from(b_element[0], 3))) == 0
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


def test_continue_source():
    with TemporaryDirectory() as project_dir:

        with Project(project_dir) as project:
            s = project.open_sink("hello")
            s.write_dict({"a": 1})
            s.write_dict({"b": 1})
            s.write_dict({"c": 1})
            s.write_dict({"d": 1})

        with Project(project_dir) as project:
            s = project.continue_source("hello")
            for filename, index, msg in s:
                assert msg == {"a": 1}
                break

        with Project(project_dir) as project:
            s = project.continue_source("hello")
            for filename, index, msg in s:
                assert msg == {"b": 1}
                break


def test_open_non_existing_source_should_raise():
    with pytest.raises(ValueError):
        with TemporaryDirectory() as project_dir:
            with Project(project_dir) as project:
                project.open_source("asfd")


def test_moving_project_dir_should_work():
    with TemporaryDirectory() as written_to:
        with TemporaryDirectory() as read_from:
            with Project(written_to) as project:
                temp_sink = project.open_sink("hello")
                temp_sink.write_dict({"a": 1})
                temp_sink.write_dict({"b": 1})
                time.sleep(1)  # Need next second in queue file name
                temp_sink.rotate()
                temp_sink.write_dict({"b": 2})
                temp_sink.write_dict({"c": 3})

            os.rename(written_to, read_from + "/project")

            with Project(read_from + "/project") as project:
                source = project.continue_source("hello")
                assert sum([1 for e in source]) == 4
                assert source.unlink_to() == 1

            with Project(read_from + "/project") as project:
                source = project.continue_source("hello")
                assert sum([1 for e in source]) == 0
                assert (
                    sum([1 for e in source.all_dict()]) == 2
                ), "Only last queue file is left"
