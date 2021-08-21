import os
from tempfile import TemporaryDirectory

from dqp.storage import VARS_FILENAME, Folder


def test_should_not_write_empty_vars_files():
    with TemporaryDirectory() as temp_dir:
        with Folder(temp_dir) as f:
            assert not os.path.exists(temp_dir + "/" + VARS_FILENAME)
        assert not os.path.exists(temp_dir + "/" + VARS_FILENAME)


def test_should_only_write_vars_if_changed():
    with TemporaryDirectory() as temp_dir:
        with Folder(temp_dir) as f:
            f.vars["a"] = "b"
            assert not os.path.exists(temp_dir + "/" + VARS_FILENAME)
        assert os.path.exists(temp_dir + "/" + VARS_FILENAME)
