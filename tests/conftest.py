import logging
import platform
import shutil

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from pathlib import Path
from random import choices
from string import ascii_letters
from typing import Type

import pytest

from deker.ABC import BaseStorageAdapter
from deker.client import Client
from deker.config import DekerConfig
from deker.ctx import CTX
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


@pytest.fixture()
def workers() -> int:
    """Get reduced number of threads."""
    return cpu_count() // 2


@pytest.fixture()
def executor(workers: int) -> ThreadPoolExecutor:
    """Creates thread executor."""
    return ThreadPoolExecutor(workers)


@pytest.fixture()
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
        write_lock_timeout=60,
        write_lock_check_interval=1,
        memory_limit=104857600,
        loglevel="ERROR",
    )


@pytest.fixture()
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


@pytest.fixture()
def uri(root_path) -> Uri:
    """Creates Uri instance from string."""
    return Uri.create(embedded_uri(root_path))


@pytest.fixture()
def ctx(uri: Uri, config, storage_adapter, workers) -> CTX:
    """Creates Client context."""
    return CTX(uri, config, storage_adapter, ThreadPoolExecutor(workers))


@pytest.fixture()
def client(root_path: Path, storage_adapter: Type[BaseStorageAdapter], workers: int, executor) -> Client:
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
