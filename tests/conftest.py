import errno
import logging
import os
import platform
import shutil

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from pathlib import Path
from random import choices
from string import ascii_letters
from typing import Any
from unittest.mock import patch

import pytest

from deker.ABC import BaseStorageAdapter
from deker.client import Client
from deker.config import DekerConfig
from deker.ctx import CTX
from deker.errors import DekerCriticalError, DekerInstanceNotExistsError
from deker.uri import Uri

from tests.parameters.uri import embedded_uri

logging.basicConfig(level="ERROR")

pytest_plugins = [
    "tests.plugins.collection",
    "tests.plugins.subset",
    "tests.plugins.adapter",
    "tests.plugins.schema",
    "tests.plugins.arrays",
    "tests.plugins.factory",
]


MISSING = "Storage directory {} doesn't exist"


class Missing:
    """Stand for a call of the file system that does not find its file.

    With ``storage_gone`` the collections directory of the storage is taken away first, as if somebody removed it:
    the adapter shall tell it from a missing file of an array.

    :param collections: the collections directory of the storage
    :param storage_gone: take the collections directory away before failing
    """

    def __init__(self, collections: Path, storage_gone: bool) -> None:
        self.collections = collections
        self.storage_gone = storage_gone

    def take_away(self) -> None:
        """Take the collections directory away, if asked."""
        if self.storage_gone:
            self.collections.rename(self.collections.with_name("collections-away"))

    def __call__(self, *args, **kwargs):
        self.take_away()
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT))


def check_not_found(call: Callable[[], Any], missing: Missing, answer: Any) -> None:
    """Check that a place which does not find its file tells a missing storage from a missing file.

    The collections directory gone: ``DekerCriticalError``, logged as critical once, nothing created again. Only the
    file gone: the answer of the place as before, nothing critical.

    :param call: the call of the place
    :param missing: what the call meets
    :param answer: the answer for a missing file: the text of ``DekerInstanceNotExistsError``, or the returned value
    """
    with patch.object(logging.getLogger("Deker"), "critical") as critical:
        if missing.storage_gone:
            with pytest.raises(DekerCriticalError) as error:
                call()
            assert str(error.value) == MISSING.format(missing.collections)
            critical.assert_called_once_with(MISSING.format(missing.collections))
            assert not missing.collections.exists()
        elif isinstance(answer, str):
            with pytest.raises(DekerInstanceNotExistsError) as not_existing:
                call()
            assert str(not_existing.value) == answer
            critical.assert_not_called()
        else:
            assert call() == answer
            critical.assert_not_called()


@pytest.fixture
def own_client(tmp_path: Path) -> Iterator[Client]:
    """Client of a storage of the test's own: the test may take its collections directory away.

    :param tmp_path: directory of the test
    """
    with Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR") as client:
        yield client


@pytest.fixture
def workers() -> int:
    """Get reduced number of threads."""
    return cpu_count() // 2


@pytest.fixture
def executor(workers: int) -> ThreadPoolExecutor:
    """Creates thread executor."""
    return ThreadPoolExecutor(workers)


@pytest.fixture
def config(root_path: Path, workers) -> DekerConfig:
    """Deker config for testing env."""
    return DekerConfig(
        uri=embedded_uri(root_path),
        workers=workers,
        collections_directory="collections",
        array_data_directory="array_data",
        varray_data_directory="varray_data",
        array_symlinks_directory="array_symlinks",
        varray_symlinks_directory="varray_symlinks",
        read_timeout=60,
        write_lock_timeout=60,
        write_lock_check_interval=1,
        memory_limit=104857600,
        loglevel="ERROR",
    )


@pytest.fixture
def name() -> str:
    """Generates random 10 chars name."""
    return "".join(choices(ascii_letters, k=10))


@pytest.fixture(scope="session")
def root_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Returns root of application.

    :param tmp_path_factory:  Temporary directory factory
    """
    directory = tmp_path_factory.mktemp("test", numbered=False)
    yield directory
    if platform.system().lower() == "linux":
        path = directory.parents[0]
        if directory.exists():
            shutil.rmtree(path)


@pytest.fixture
def uri(root_path) -> Uri:
    """Creates Uri instance from string."""
    return Uri.create(embedded_uri(root_path))


@pytest.fixture
def ctx(uri: Uri, config, storage_adapter, workers) -> CTX:
    """Creates Client context."""
    return CTX(uri, config, storage_adapter, ThreadPoolExecutor(workers))


@pytest.fixture
def client(root_path: Path, storage_adapter: type[BaseStorageAdapter], workers: int, executor) -> Client:
    """Creates Client object and init all connections.

    :param root_path: path to directory with collections fixture
    :param storage_adapter: BaseStorageAdapter fixture
    :param workers: Number of workers for pool
    """
    with Client(
        uri=str(embedded_uri(root_path)),
        workers=workers,
        loglevel="ERROR",
    ) as client:
        yield client
