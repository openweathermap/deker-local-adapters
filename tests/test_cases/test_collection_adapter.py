import errno
import gc
import json
import logging
import os
import shutil
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from uuid import uuid4

import pytest

from cantok import DefaultToken, SimpleToken
from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerAdapterClosedError,
    DekerCollectionAlreadyExistsError,
    DekerCollectionNotExistsError,
    DekerCriticalError,
    DekerMemoryError,
    DekerTrashError,
)
from deker.flock import Flock
from deker.locks import CollectionLock
from deker.schemas import ArraySchema, DimensionSchema
from deker_tools.cancel import TokenCancelledError
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from tests.conftest import MISSING, Missing, check_not_found
from tests.parameters.common import random_string
from tests.parameters.uri import embedded_uri

import deker_local_adapters.collection_adapter

from deker_local_adapters import AdaptersFactory, LocalCollectionAdapter
from deker_local_adapters.collection_adapter import empty_trash_directory, move_to_trash
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter


def files_under(root: Path) -> list[str]:
    """List the files and the symlinks under a directory, relative to it; the directories are not listed.

    :param root: directory to walk
    :returns: sorted relative paths
    """
    return sorted(
        os.path.relpath(os.path.join(directory, name), root) for directory, _, names in os.walk(root) for name in names
    )


def fill_collections_directory(path: Path, names: tuple[str, ...]) -> None:
    """Make a collection directory with its metadata file for every name.

    :param path: the collections directory
    :param names: names of the collections
    """
    for name in names:
        (path / name).mkdir()
        (path / name / f"{name}.json").write_text(json.dumps({"name": name}))


@pytest.mark.asyncio
class TestCollectionAdapter:
    """Class for testing local collection adapter."""

    def test_collection_adapter_create_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        root_path: Path,
        array_schema: ArraySchema,
        factory: AdaptersFactory,
    ):
        """Tests if adapter creates array_collection in DB.

        :param collection_adapter: CollectionAdapter
        :param root_path: Path to collections directory
        :param array_schema: ArraySchema instance
        """
        name = random_string()
        collection = Collection(
            name=name,
            schema=array_schema,
            adapter=collection_adapter,
            factory=factory,
            storage_adapter=HDF5StorageAdapter,
        )
        collections = root_path.joinpath(factory.ctx.config.collections_directory)
        try:
            assert collection_adapter.create(collection, timeout=Sentinel, token=DefaultToken()) is None
            assert json.loads((collections / name / f"{name}.json").read_text()) == json.loads(
                json.dumps(collection.as_dict)
            )
            assert not (collections / f"{name}.lock").exists()  # the lock of the collection is its json file
        finally:
            collection.delete()

    def test_collection_adapter_create_collection_memory_error(
        self,
        collection_adapter: LocalCollectionAdapter,
        factory: AdaptersFactory,
        mocker: MockerFixture,
    ):
        """Tests if adapter raises memory error on collection creation.

        :param collection_adapter: CollectionAdapter
        :param factory: AdaptersFactory
        """
        mocker.patch.object(collection_adapter.ctx.config, "memory_limit", 100)  # the ctx is shared by the class
        schema = ArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=10000),
                DimensionSchema(name="y", size=10000),
            ],
            dtype=float,
        )
        col_name = "memory_excess_adapter"
        with pytest.raises(DekerMemoryError):
            collection_adapter.create(
                Collection(
                    name=col_name,
                    schema=schema,
                    adapter=collection_adapter,
                    factory=factory,
                    storage_adapter=HDF5StorageAdapter,
                ),
                timeout=Sentinel,
                token=DefaultToken(),
            )
        assert not (collection_adapter.collections_resource / col_name).exists()  # checked before the lock

    @pytest.mark.parametrize(
        ("splitter", "expected"),
        [
            ({"vgrid": [1, 3]}, (3, 1)),
            ({"arrays_shape": [1, 3]}, (3, 1)),
            ({"vgrid": [3, 1]}, (1, 3)),
            ({"arrays_shape": [3, 1]}, (1, 3)),
        ],
    )
    def test_create_from_dict_varray_collection(
        self, client: Client, varray_collection: Collection, splitter: dict, expected: tuple
    ):
        """Check if creation from dict is correct."""
        schema = {
            "metadata_version": "0.2",
            "name": "varray_from_dict_test_collection",
            "type": "varray",
            "schema": {
                "dtype": "int",
                "fill_value": -1,
                "attributes": [
                    {"name": "primary_attr_1", "dtype": "string", "primary": True},
                    {"name": "custom_attr_1", "dtype": "datetime", "primary": False},
                ],
                "dimensions": [
                    {
                        "type": "generic",
                        "name": "dimension_1",
                        "size": 3,
                        "labels": ["label_1", "label_2", "label_3"],
                    },
                    {
                        "type": "time",
                        "name": "dimension_2",
                        "size": 3,
                        "start_value": "$custom_attr_1",
                        "step": {"days": 0, "seconds": 14400, "microseconds": 0},
                    },
                ],
            },
            "options": {
                "chunks": {"mode": "manual", "size": [1, 3]},
                "compression": {"compression": "gzip", "options": [9]},
            },
        }
        col_from_dict = None

        try:
            schema["schema"].update(**splitter)
            col_from_dict = client.collection_from_dict(schema)
            assert col_from_dict
            assert col_from_dict.name == schema["name"]
            if list(splitter.keys())[0] == "vgrid":
                other_splitter = "arrays_shape"
            else:
                other_splitter = "vgrid"
            assert getattr(col_from_dict.varray_schema, other_splitter) == expected
        finally:
            if col_from_dict:
                col_from_dict.delete()

    def test_create_from_other_varray_collection_dict(self, client: Client, varray_collection: Collection):
        """Check if creation from dict is correct."""
        col_from_dict = None

        new_col_dict = varray_collection.as_dict
        new_col_dict["name"] = str(uuid4())
        try:
            col_from_dict = client.collection_from_dict(new_col_dict)
            # Compare through dicts
            col_dict = col_from_dict.as_dict

            del col_dict["name"]
            del new_col_dict["name"]

            assert col_dict == new_col_dict
        finally:
            if col_from_dict:
                col_from_dict.delete()

    @pytest.mark.parametrize("background", [False, True])
    def test_collection_adapter_deletes_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        array_collection: Collection,
        mocker: MockerFixture,
        background: bool,
    ):
        """The directory of the collection is moved to the trash in one rename; the trash is emptied only when asked."""
        array_collection.create()
        before = files_under(array_collection.path)
        trash = collection_adapter.trash_resource
        move = mocker.spy(deker_local_adapters.collection_adapter, "move_to_trash")
        empty = mocker.spy(deker_local_adapters.collection_adapter, "empty_trash_directory")
        assert (
            collection_adapter.delete(array_collection, background=background, _empty_trash=False, token=DefaultToken())
            is None
        )
        move.assert_called_once_with(array_collection.path, trash, collection_adapter.collections_resource)
        empty.assert_not_called()
        assert not os.path.exists(array_collection.path)
        (moved,) = trash.iterdir()
        assert files_under(moved) == before
        collection_adapter.empty_trash(background=False)
        assert list(trash.iterdir()) == []

    @pytest.mark.parametrize("operation", ["delete", "clear"])
    @pytest.mark.parametrize("background", [False, True])
    def test_collection_adapter_empties_the_trash_when_asked(
        self,
        collection_adapter: LocalCollectionAdapter,
        array_collection: Collection,
        mocker: MockerFixture,
        operation: str,
        background: bool,
    ):
        """The trash is emptied after the moves, once, by the background asked; the fixture empties it afterwards."""
        array_collection.create()
        empty = mocker.patch.object(deker_local_adapters.collection_adapter, "empty_trash_directory")
        getattr(collection_adapter, operation)(
            array_collection, background=background, _empty_trash=True, token=DefaultToken()
        )
        empty.assert_called_once_with(
            collection_adapter.trash_resource, background=background, logger=collection_adapter.logger
        )
        assert files_under(array_collection.path) == (
            [] if operation == "delete" else [f"{array_collection.name}.json"]
        )

    def test_collection_adapter_delete_tolerates_a_collection_already_gone(
        self, collection_adapter: LocalCollectionAdapter, array_collection: Collection
    ):
        """Deleting a collection whose directory is gone already is not an error: another deletion moved it."""
        shutil.rmtree(array_collection.path)
        assert (
            collection_adapter.delete(array_collection, background=False, _empty_trash=False, token=DefaultToken())
            is None
        )
        assert list(collection_adapter.trash_resource.iterdir()) == []

    def test_collection_adapter_delete_raises_when_the_directory_cannot_be_moved(
        self, collection_adapter: LocalCollectionAdapter, array_collection: Collection, mocker: MockerFixture
    ):
        """A failure to move the directory is not swallowed: the collection would stay with its files."""
        mocker.patch.object(os, "rename", side_effect=PermissionError(errno.EACCES, "read-only storage"))
        with pytest.raises(PermissionError, match="read-only storage"):
            collection_adapter.delete(array_collection, background=False, _empty_trash=False, token=DefaultToken())
        assert os.path.exists(array_collection.path)

    def test_collection_adapter_get_collection(
        self,
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
    ):
        """Tests if array_collection adapter fetch array_collection from db correctly.

        :param array_collection: Pre created array_collection
        :param collection_adapter: array_collection adapter
        """
        schema = collection_adapter.read(name=array_collection.name)
        assert schema

    def test_collection_adapter_clear(
        self,
        varray_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
        mocker: MockerFixture,
    ):
        """The inner directories go to the trash one by one: the symlinks of the VArrays, of the Arrays, the data of
        the VArrays, of the Arrays; then whatever else lies in the collection; its metadata file stays.
        """
        collection = varray_collection
        collection.create()[:].update(1.0)
        path = collection.path
        (path / "stray.txt").write_text("x")
        (path / "stray_dir").mkdir()
        before = files_under(path)
        config = collection_adapter.ctx.config
        trash = collection_adapter.trash_resource
        move = mocker.spy(deker_local_adapters.collection_adapter, "move_to_trash")
        assert collection_adapter.clear(collection, background=False, _empty_trash=False, token=DefaultToken()) is None
        moved = [call.args[0] for call in move.call_args_list]
        assert moved[:4] == [
            path / config.varray_symlinks_directory,
            path / config.array_symlinks_directory,
            path / config.varray_data_directory,
            path / config.array_data_directory,
        ]
        assert sorted(moved[4:]) == [path / "stray.txt", path / "stray_dir"]
        assert {call.args[1:] for call in move.call_args_list} == {(trash, collection_adapter.collections_resource)}
        assert files_under(path) == [f"{collection.name}.json"]
        assert len(list(trash.iterdir())) == 6
        # the files of the moved directories, relative to them
        assert sorted(name.partition(os.sep)[2] for name in files_under(trash) if os.sep in name) == sorted(
            name.partition(os.sep)[2] for name in before if os.sep in name
        )
        collection_adapter.empty_trash(background=False)
        assert list(trash.iterdir()) == []

    def test_collection_adapter_iter(self, client: Client, array_schema: ArraySchema, root_path: Path, ctx):
        for root, dirs, files in os.walk(root_path / ctx.config.collections_directory):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

        names = {random_string() for _ in range(10)}
        collections = [client.create_collection(name, array_schema) for name in names]
        adapter: LocalCollectionAdapter = collections[0]._Collection__adapter
        coll_names = {coll["name"] for coll in adapter}
        assert names.issubset(coll_names)
        assert names - coll_names == set()


class TestCollectionAdapterClosed:
    def test_closed_adapter_does_not_empty_the_trash(self, ctx):
        adapter = LocalCollectionAdapter(ctx)
        ctx.is_closed = True
        try:
            with pytest.raises(DekerAdapterClosedError):
                adapter.empty_trash(background=False)
        finally:
            ctx.is_closed = False

    @pytest.mark.parametrize("method", ["create", "delete", "clear", "is_deleted"])
    def test_closed_adapter_raises(self, ctx, array_collection: Collection, method: str):
        """Tests if a collection adapter of a closed context answers with DekerAdapterClosedError."""
        adapter = LocalCollectionAdapter(ctx)
        ctx.is_closed = True
        try:
            with pytest.raises(DekerAdapterClosedError):
                getattr(adapter, method)(array_collection)
            with pytest.raises(DekerAdapterClosedError):
                adapter.read(array_collection.name)
            with pytest.raises(DekerAdapterClosedError):
                list(adapter)
        finally:
            ctx.is_closed = False

    def test_closed_factory_raises(self, ctx, uri):
        """Tests if a factory of a closed context gives no adapters."""
        factory = AdaptersFactory(ctx, uri)
        ctx.is_closed = True
        try:
            with pytest.raises(DekerAdapterClosedError):
                factory.get_collection_adapter()
        finally:
            ctx.is_closed = False


class TakeAwayUnderTheLock:
    """Stand for ``get_result`` of the collection lock: once the lock is taken, takes a directory away before the
    creation reads the metadata file, as the deletion of the collection or the loss of the collections directory does.

    :param real: the real ``get_result``
    :param directory: the directory to take away
    :param target: where it goes
    """

    def __init__(self, real, directory: Path, target: Path) -> None:
        self.real = real
        self.directory = directory
        self.target = target

    def __call__(self, lock, *args, **kwargs):
        self.directory.rename(self.target)
        return self.real(lock, *args, **kwargs)


class TestCollectionAdapterEdges:
    @staticmethod
    def collection_of(name: str, collection_adapter: LocalCollectionAdapter, client: Client, size: int) -> Collection:
        """Make a collection object, not created.

        :param name: name of the collection
        :param collection_adapter: the collection adapter
        :param client: client of the storage
        :param size: size of its only dimension
        :returns: the collection
        """
        schema = ArraySchema(dimensions=[DimensionSchema(name="x", size=size)], dtype=float)
        return Collection(name, schema, collection_adapter, client._factory, HDF5StorageAdapter)

    def test_existing_collection_is_not_created_again(
        self, collection_adapter: LocalCollectionAdapter, client: Client, name: str
    ):
        """Its json file is written: the second creator learns that the collection exists, the file stays."""
        first = self.collection_of(name, collection_adapter, client, 2)
        collection_adapter.create(first, timeout=Sentinel, token=DefaultToken())
        try:
            with pytest.raises(DekerCollectionAlreadyExistsError) as error:
                collection_adapter.create(
                    self.collection_of(name, collection_adapter, client, 3), timeout=Sentinel, token=DefaultToken()
                )
            assert str(error.value) == f"Collection {name} already exists"
            assert collection_adapter.read(name) == json.loads(json.dumps(first.as_dict))
        finally:
            first.delete()

    def test_empty_collection_file_is_written(
        self, collection_adapter: LocalCollectionAdapter, client: Client, name: str
    ):
        """An empty json file is a creation that did not write it: the next creator writes it."""
        directory = collection_adapter.collections_resource / name
        directory.mkdir()
        (directory / f"{name}.json").touch()
        collection = self.collection_of(name, collection_adapter, client, 2)
        try:
            collection_adapter.create(collection, timeout=Sentinel, token=DefaultToken())
            assert collection_adapter.read(name) == json.loads(json.dumps(collection.as_dict))
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="collection deleted")]
    )
    def test_directory_gone_once_the_lock_is_taken(
        self, own_client: Client, array_schema: ArraySchema, tmp_path: Path, mocker: MockerFixture, storage_gone: bool
    ):
        """The deletion comes after the creation: the creation returns, the collection does not exist; the loss of the
        collections directory is critical.
        """
        adapter = own_client._adapter
        directory = adapter.collections_resource if storage_gone else adapter.collections_resource / "own"
        taker = TakeAwayUnderTheLock(CollectionLock.get_result, directory, tmp_path / "away")
        mocker.patch.object(CollectionLock, "get_result", autospec=True, side_effect=taker)
        collection = self.collection_of("own", adapter, own_client, 2)
        create = partial(adapter.create, collection, timeout=Sentinel, token=DefaultToken())
        check_not_found(create, Missing(adapter.collections_resource, storage_gone), None)
        if not storage_gone:
            assert not directory.exists()
            assert (tmp_path / "away" / "own.json").read_text() == ""  # held, never written

    def test_empty_collection_file_is_no_collection(self, collection_adapter: LocalCollectionAdapter, name: str):
        """The json file of a collection being created is empty: the collection does not exist yet."""
        directory = collection_adapter.collections_resource / name
        directory.mkdir()
        (directory / f"{name}.json").touch()
        try:
            with pytest.raises(DekerCollectionNotExistsError) as error:
                collection_adapter.read(name)
            assert str(error.value) == f"{name} collection doesn't exist"
            assert name not in [meta["name"] for meta in collection_adapter]
        finally:
            shutil.rmtree(directory)

    def test_read_raises_for_missing_collection(self, collection_adapter: LocalCollectionAdapter):
        with pytest.raises(DekerCollectionNotExistsError):
            collection_adapter.read("no-such-collection")

    def test_clear_removes_foreign_files_and_directories(self, client: Client, array_collection: Collection):
        foreign_file = array_collection.path / "stray.txt"
        foreign_file.write_text("x")
        foreign_dir = array_collection.path / "stray_dir"
        foreign_dir.mkdir()
        (foreign_dir / "inner.txt").write_text("x")
        array_collection.clear()
        assert not foreign_file.exists()
        assert not foreign_dir.exists()
        assert (array_collection.path / (array_collection.name + ".json")).exists()

    def test_create_skips_memory_check_when_configured(self, root_path, array_schema, executor):
        from tests.parameters.uri import embedded_uri

        client = Client(embedded_uri(root_path), executor=executor, skip_collection_create_memory_check=True)
        collection = client.create_collection("no-memory-check", array_schema)
        try:
            assert collection.path.exists()
        finally:
            collection.delete()
            client.close()


if __name__ == "__main__":
    pytest.main()


class TestCollectionAdapterCancellation:
    """The collection adapter stops a cancelled operation before the collection is touched."""

    @pytest.mark.parametrize("operation", ["clear", "delete"])
    def test_cancelled_token_changes_nothing(self, array_collection: Collection, collection_adapter, operation):
        array = array_collection.create()
        token = SimpleToken()
        token.cancel()
        with pytest.raises(TokenCancelledError):
            getattr(collection_adapter, operation)(array_collection, background=False, _empty_trash=True, token=token)
        assert array_collection.path.exists()
        assert [a.id for a in array_collection] == [array.id]
        assert list(collection_adapter.trash_resource.iterdir()) == []

    def test_create_with_a_cancelled_token_creates_nothing(
        self, client: Client, collection_adapter, array_schema, name
    ):
        collection = Collection(
            name=name,
            schema=array_schema,
            adapter=collection_adapter,
            factory=client._factory,
            storage_adapter=HDF5StorageAdapter,
        )
        token = SimpleToken()
        token.cancel()
        with pytest.raises(TokenCancelledError):
            collection_adapter.create(collection, timeout=Sentinel, token=token)
        assert not (collection_adapter.collections_resource / name).exists()

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("operation", ["clear", "delete"])
    def test_held_collection_lock_does_not_stop_the_clearing_and_the_deletion(
        self, array_collection: Collection, collection_adapter, operation
    ):
        """The lock of the collection, its metadata file held by a creation of the name, is not waited for."""
        array_collection.create()
        with Flock(array_collection.path / f"{array_collection.name}.json"):
            start = time.monotonic()
            getattr(collection_adapter, operation)(
                array_collection, background=False, _empty_trash=True, token=DefaultToken()
            )
            assert time.monotonic() - start < 0.5
        assert files_under(array_collection.path) == (
            [] if operation == "delete" else [f"{array_collection.name}.json"]
        )
        assert list(collection_adapter.trash_resource.iterdir()) == []


class MoveAwayOnOpen:
    """Stand for ``open`` in the collection adapter: the first call moves the file away before opening it.

    The collection file was checked a moment ago, as a deletion of the collection meanwhile leaves it.

    :param target: where the file is moved to
    """

    def __init__(self, target: Path) -> None:
        self.target = target
        self.gone: list[str] = []

    def __call__(self, file: Path, *args, **kwargs):
        if not self.gone:
            file.rename(self.target / file.name)
            self.gone.append(file.stem)
        return open(file, *args, **kwargs)


class TestCollectionAdapterIterClosesTheDirectory:
    """The iteration over the collections leaves no unclosed scandir iterator, however it ends.

    An unclosed iterator warns with ResourceWarning when it is collected; pytest turns it into an error.
    """

    @pytest.fixture
    def collections_path(self, tmp_path: Path, mocker: MockerFixture) -> Path:
        """Point the collections directory of every collection adapter to an empty directory of the test.

        :param tmp_path: directory of the test
        :param mocker: mocker fixture
        """
        mocker.patch.object(LocalCollectionAdapter, "collections_resource", tmp_path)
        return tmp_path

    def test_full_iteration(self, collections_path: Path, collection_adapter: LocalCollectionAdapter):
        fill_collections_directory(collections_path, ("first", "second"))
        (collections_path / "first.lock").write_text("")
        assert sorted(meta["name"] for meta in collection_adapter) == ["first", "second"]
        gc.collect()

    def test_iteration_stopped_at_the_first_collection(
        self, collections_path: Path, collection_adapter: LocalCollectionAdapter
    ):
        fill_collections_directory(collections_path, ("first", "second"))
        iterator = iter(collection_adapter)
        assert next(iterator)["name"] in {"first", "second"}
        del iterator
        gc.collect()

    def test_iteration_of_an_empty_directory(self, collections_path: Path, collection_adapter: LocalCollectionAdapter):
        assert list(collection_adapter) == []
        gc.collect()

    def test_iteration_stopped_by_an_error(self, collections_path: Path, collection_adapter: LocalCollectionAdapter):
        """A broken collection file stops the iteration with its error."""
        fill_collections_directory(collections_path, ("broken",))
        (collections_path / "broken" / "broken.json").write_text("{not json")
        with pytest.raises(json.JSONDecodeError):
            list(collection_adapter)
        gc.collect()

    def test_directory_without_a_collection_file_is_skipped(
        self, collections_path: Path, collection_adapter: LocalCollectionAdapter
    ):
        """A directory without its collection file is not a collection: check_integrity finds it and removes it."""
        fill_collections_directory(collections_path, ("first", "second"))
        (collections_path / "half-made").mkdir()
        assert sorted(meta["name"] for meta in collection_adapter) == ["first", "second"]
        gc.collect()

    def test_collection_file_gone_meanwhile_is_skipped(
        self,
        collections_path: Path,
        collection_adapter: LocalCollectionAdapter,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
    ):
        """The collection file is there when it is checked and gone when it is opened: the collection is skipped."""
        fill_collections_directory(collections_path, ("first", "second"))
        move = MoveAwayOnOpen(tmp_path_factory.mktemp("away"))
        mocker.patch("deker_local_adapters.collection_adapter.open", create=True, side_effect=move)
        names = [meta["name"] for meta in collection_adapter]
        assert len(move.gone) == 1
        assert names == [name for name in ("first", "second") if name not in move.gone]
        gc.collect()

    def test_missing_collections_directory_is_critical(
        self, tmp_path: Path, collection_adapter: LocalCollectionAdapter, mocker: MockerFixture
    ):
        """The collections directory itself is never skipped: its absence is critical."""
        missing = tmp_path / "collections"
        mocker.patch.object(LocalCollectionAdapter, "collections_resource", missing)
        with pytest.raises(DekerCriticalError) as error:
            list(collection_adapter)
        assert str(error.value) == MISSING.format(missing)
        gc.collect()

    def test_listing_error_with_the_collections_directory_in_place_is_raised_as_it_is(
        self, collections_path: Path, collection_adapter: LocalCollectionAdapter, mocker: MockerFixture
    ):
        """Only a missing directory is critical: the error of the listing is not hidden otherwise."""
        mocker.patch.object(os, "scandir", side_effect=FileNotFoundError(errno.ENOENT, "gone", str(collections_path)))
        with pytest.raises(FileNotFoundError, match="gone"):
            list(collection_adapter)


class TestStorageDirectories:
    """The adapter, built by the client when it opens, creates the collections and the trash of the storage; nobody
    else creates them. A directory missing when it is needed is critical. Every test works on a storage of its own.
    """

    def test_adapter_creates_the_collections_and_the_trash_and_keeps_them(self, tmp_path: Path):
        storage = tmp_path / "storage"
        with Client(embedded_uri(storage)) as client:
            assert client._adapter.trash_resource == storage / "trash"
            assert sorted(path.name for path in storage.iterdir()) == ["collections", "trash"]
            (storage / "trash" / "left").write_text("file")
        with Client(embedded_uri(storage)):
            assert (storage / "trash" / "left").read_text() == "file"

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(lambda adapter, collection: adapter.read(collection.name), id="read"),
            pytest.param(lambda adapter, collection: adapter.is_deleted(collection), id="is_deleted"),
            pytest.param(
                lambda adapter, collection: adapter.delete(
                    collection, background=False, _empty_trash=True, token=DefaultToken()
                ),
                id="delete",
            ),
            pytest.param(
                lambda adapter, collection: adapter.create(collection, timeout=Sentinel, token=DefaultToken()),
                id="create",
            ),
            pytest.param(
                lambda adapter, collection: adapter.clear(
                    collection, background=False, _empty_trash=True, token=DefaultToken()
                ),
                id="clear",
            ),
        ],
    )
    def test_missing_collections_directory_is_critical_and_not_created(
        self, tmp_path: Path, array_schema: ArraySchema, mocker: MockerFixture, call
    ):
        storage = tmp_path / "storage"
        collections = storage / "collections"
        critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
        with Client(embedded_uri(storage)) as client:
            collection = client.create_collection("gone", array_schema)
            shutil.rmtree(collections)
            with pytest.raises(DekerCriticalError) as error:
                call(client._adapter, collection)
        assert str(error.value) == MISSING.format(collections)
        critical.assert_called_once_with(MISSING.format(collections))
        assert not collections.exists()

    def test_gone_collection_with_the_collections_directory_in_place_is_deleted(
        self, tmp_path: Path, array_schema: ArraySchema
    ):
        storage = tmp_path / "storage"
        with Client(embedded_uri(storage)) as client:
            collection = client.create_collection("gone", array_schema)
            assert client._adapter.is_deleted(collection) is False
            shutil.rmtree(collection.path)
            assert client._adapter.is_deleted(collection) is True


class TestEmptyTrashDirectory:
    """The only code of the removal of the trash: everything in it is removed, the trash itself stays."""

    @pytest.fixture
    def trash(self, tmp_path: Path) -> Path:
        """An empty trash of a storage of the test.

        :param tmp_path: directory of the test
        """
        trash = tmp_path / "storage" / "trash"
        trash.mkdir(parents=True)
        return trash

    @pytest.fixture
    def outside(self, tmp_path: Path) -> Path:
        """Targets of the symlinks in the trash: a file and a directory with a file, out of the trash.

        :param tmp_path: directory of the test
        """
        outside = tmp_path / "outside"
        (outside / "target-dir").mkdir(parents=True)
        (outside / "target-dir" / "kept.txt").write_text("kept")
        (outside / "target.txt").write_text("kept")
        return outside

    @staticmethod
    def fill(trash: Path, outside: Path) -> None:
        """Put into the trash what the operations leave there: files, directories and symlinks.

        :param trash: the trash
        :param outside: the targets of the symlinks
        """
        (trash / uuid4().hex).write_text("file")
        nested = trash / uuid4().hex / "array_data" / "a" / "b"
        nested.mkdir(parents=True)
        (nested / "array.hdf5").write_text("file")
        (nested.parent / "other.hdf5").write_text("file")
        (trash / uuid4().hex).mkdir()
        (trash / uuid4().hex).symlink_to(outside / "target.txt")
        (trash / uuid4().hex).symlink_to(outside / "target-dir")
        (nested.parent / "link").symlink_to(outside / "target-dir")
        (nested.parent / "file-link").symlink_to(outside / "target.txt")

    @staticmethod
    def assert_targets_kept(outside: Path) -> None:
        """The targets of the symlinks that were in the trash are untouched.

        :param outside: the targets
        """
        assert (outside / "target.txt").read_text() == "kept"
        assert (outside / "target-dir" / "kept.txt").read_text() == "kept"

    def test_everything_is_removed_symlinks_without_their_targets(self, trash: Path, outside: Path):
        self.fill(trash, outside)
        assert empty_trash_directory(trash, background=False, logger=logging.getLogger("test")) is None
        assert list(trash.iterdir()) == []
        self.assert_targets_kept(outside)

    def test_what_is_not_removed_is_counted_once_with_unique_reasons(
        self, trash: Path, outside: Path, mocker: MockerFixture
    ):
        """A directory without the right to write keeps its two files and its empty directory: three are counted, the
        directory itself is not (only its content keeps it); one more file is busy; everything else is removed.
        """
        self.fill(trash, outside)
        locked = trash / "locked"
        (locked / "empty").mkdir(parents=True)
        (locked / "first").write_text("file")
        (locked / "second").write_text("file")
        (trash / "busy").write_text("file")
        unlink = os.unlink

        def busy_unlink(path, *args, **kwargs):
            if os.path.basename(path) == "busy":
                raise OSError(errno.EBUSY, os.strerror(errno.EBUSY), path)
            return unlink(path, *args, **kwargs)

        mocker.patch.object(os, "unlink", side_effect=busy_unlink)
        locked.chmod(0o500)
        try:
            with pytest.raises(DekerTrashError) as error:
                empty_trash_directory(trash, background=False, logger=logging.getLogger("test"))
            assert str(error.value) == (
                f"The trash of the storage is not emptied: 4 files are not removed from {trash}. "
                "Reasons: Device or resource busy; Permission denied"
            )
            assert sorted(path.name for path in trash.iterdir()) == ["busy", "locked"]
            assert sorted(path.name for path in locked.iterdir()) == ["empty", "first", "second"]
            self.assert_targets_kept(outside)
        finally:
            locked.chmod(0o700)

    def test_unreadable_trash_is_counted(self, trash: Path):
        (trash / "left").write_text("file")
        trash.chmod(0o300)
        try:
            with pytest.raises(DekerTrashError) as error:
                empty_trash_directory(trash, background=False, logger=logging.getLogger("test"))
        finally:
            trash.chmod(0o700)
        assert str(error.value) == (
            f"The trash of the storage is not emptied: 1 files are not removed from {trash}. Reasons: Permission denied"
        )

    def test_missing_trash_is_critical_and_not_created(self, trash: Path, mocker: MockerFixture):
        trash.rmdir()
        critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
        with pytest.raises(DekerCriticalError) as error:
            empty_trash_directory(trash, background=False, logger=logging.getLogger("test"))
        assert str(error.value) == MISSING.format(trash)
        critical.assert_called_once_with(MISSING.format(trash))
        assert not trash.exists()

    def test_two_removals_at_once_raise_nothing(self, trash: Path, outside: Path, mocker: MockerFixture):
        """The first removal lets the second one remove everything before its own first removal of a file."""
        self.fill(trash, outside)
        unlink = os.unlink
        other: list[threading.Thread] = []

        def first_lets_the_other_go(path, *args, **kwargs):
            if not other:
                other.append(
                    threading.Thread(
                        target=empty_trash_directory,
                        args=(trash,),
                        kwargs={"background": False, "logger": logging.getLogger("test")},
                    )
                )
                other[0].start()
                other[0].join()
            return unlink(path, *args, **kwargs)

        excepthook = mocker.patch.object(threading, "excepthook")
        mocker.patch.object(os, "unlink", side_effect=first_lets_the_other_go)
        empty_trash_directory(trash, background=False, logger=logging.getLogger("test"))
        excepthook.assert_not_called()
        assert list(trash.iterdir()) == []
        self.assert_targets_kept(outside)

    def test_background_returns_while_the_removal_waits_in_a_daemon_thread_of_its_own(
        self, trash: Path, mocker: MockerFixture
    ):
        (trash / "left").write_text("file")
        walk = os.walk
        started, go = threading.Event(), threading.Event()
        threads: list[threading.Thread] = []

        def held_walk(*args, **kwargs):
            threads.append(threading.current_thread())
            started.set()
            go.wait()
            yield from walk(*args, **kwargs)

        mocker.patch.object(os, "walk", side_effect=held_walk)
        executor = ThreadPoolExecutor(1)
        assert empty_trash_directory(trash, background=True, logger=logging.getLogger("test")) is None
        assert started.wait(10)
        assert (trash / "left").exists()  # returned while the removal waits
        (thread,) = threads
        assert thread is not threading.current_thread()
        assert thread.daemon
        assert thread not in executor._threads
        assert not thread.name.startswith("ThreadPoolExecutor")
        go.set()
        thread.join(10)
        executor.shutdown()
        assert list(trash.iterdir()) == []

    @staticmethod
    def run_in_background(trash: Path, mocker: MockerFixture) -> None:
        """Run the removal in the background and wait for its thread.

        :param trash: the trash
        :param mocker: mocker fixture
        """
        walk = os.walk
        threads: list[threading.Thread] = []
        started = threading.Event()

        def watched_walk(*args, **kwargs):
            threads.append(threading.current_thread())
            started.set()
            yield from walk(*args, **kwargs)

        mocker.patch.object(os, "walk", side_effect=watched_walk)
        empty_trash_directory(trash, background=True, logger=logging.getLogger("test.trash"))
        assert started.wait(10)
        threads[0].join(10)

    def test_error_in_the_background_goes_to_the_log(
        self, trash: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ):
        locked = trash / "locked"
        locked.mkdir()
        (locked / "file").write_text("file")
        excepthook = mocker.patch.object(threading, "excepthook")
        locked.chmod(0o500)
        try:
            with caplog.at_level(logging.ERROR, logger="test.trash"):
                self.run_in_background(trash, mocker)
        finally:
            locked.chmod(0o700)
        excepthook.assert_not_called()
        assert [(record.name, record.levelno, record.getMessage()) for record in caplog.records] == [
            (
                "test.trash",
                logging.ERROR,
                f"The trash of the storage is not emptied: 1 files are not removed from {trash}. "
                "Reasons: Permission denied",
            )
        ]

    def test_missing_trash_in_the_background_is_logged_as_critical_only(
        self, trash: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ):
        trash.rmdir()
        excepthook = mocker.patch.object(threading, "excepthook")
        critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
        with caplog.at_level(logging.DEBUG, logger="test.trash"):
            self.run_in_background(trash, mocker)
        excepthook.assert_not_called()
        critical.assert_called_once_with(MISSING.format(trash))
        assert caplog.records == []
        assert not trash.exists()

    def test_adapter_empties_the_trash_of_its_storage(self, tmp_path: Path):
        storage = tmp_path / "storage"
        with Client(embedded_uri(storage)) as client:
            (storage / "trash" / "left").write_text("file")
            assert client._adapter.empty_trash(background=False) is None
            assert list((storage / "trash").iterdir()) == []


class TestMoveToTrash:
    """The only code of the move to the trash: one rename under a flat random name."""

    @pytest.fixture
    def storage(self, tmp_path: Path) -> Path:
        """A storage of the test: its collections directory with a collection, and its trash.

        :param tmp_path: directory of the test
        """
        storage = tmp_path / "storage"
        (storage / "collections" / "c" / "array_data").mkdir(parents=True)
        (storage / "trash").mkdir()
        return storage

    @staticmethod
    def move(storage: Path, path: Path) -> None:
        """Move into the trash of the storage.

        :param storage: the storage
        :param path: what to move
        """
        assert move_to_trash(path, storage / "trash", storage / "collections") is None

    @pytest.mark.parametrize("kind", ["file", "directory", "symlink"])
    def test_moved_in_one_rename_under_a_flat_random_name(self, storage: Path, kind: str, mocker: MockerFixture):
        source = storage / "collections" / "c" / "array_data" / "moved"
        target = storage / "collections" / "c" / "c.json"
        target.write_text("target")
        if kind == "file":
            source.write_text("content")
        elif kind == "directory":
            (source / "inner").mkdir(parents=True)
            (source / "inner" / "file").write_text("content")
        else:
            source.symlink_to(target)
        rename = mocker.spy(os, "rename")
        self.move(storage, source)
        assert not os.path.lexists(source)
        (moved,) = (storage / "trash").iterdir()
        rename.assert_called_once_with(source, moved)
        assert len(moved.name) == 32
        assert int(moved.name, 16) >= 0  # a hex name, no part of the source path in it
        if kind == "file":
            assert moved.read_text() == "content"
        elif kind == "directory":
            assert (moved / "inner" / "file").read_text() == "content"
        else:
            assert moved.readlink() == target
            assert target.read_text() == "target"  # the target stays where it is

    def test_moves_of_one_name_get_their_own_names(self, storage: Path):
        source = storage / "collections" / "c" / "array_data" / "moved"
        for content in ("first", "second"):
            source.write_text(content)
            self.move(storage, source)
        assert sorted(path.read_text() for path in (storage / "trash").iterdir()) == ["first", "second"]

    def test_missing_source_with_the_storage_directories_in_place_is_not_an_error(
        self, storage: Path, mocker: MockerFixture
    ):
        """Another operation moved it already."""
        critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
        self.move(storage, storage / "collections" / "c" / "array_data" / "gone")
        assert list((storage / "trash").iterdir()) == []
        critical.assert_not_called()

    @pytest.mark.parametrize("directory", ["trash", "collections"])
    def test_missing_storage_directory_is_critical(self, storage: Path, mocker: MockerFixture, directory: str):
        source = storage / "collections" / "c" / "array_data" / "moved"
        source.write_text("content")
        gone = storage / directory
        gone.rename(storage / "away")
        critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
        with pytest.raises(DekerCriticalError) as error:
            move_to_trash(source, storage / "trash", storage / "collections")
        assert str(error.value) == MISSING.format(gone)
        critical.assert_called_once_with(MISSING.format(gone))
        assert not gone.exists()  # not made again
        if directory == "trash":
            assert source.read_text() == "content"  # left where it was


class TestIterationTellsAMissingStorageFromAMissingFile:
    """The iteration over the collections skips a collection file gone meanwhile, unless the collections directory
    of the storage is gone: that is critical.
    """

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="file gone")]
    )
    def test_collection_file_not_found_by_the_iteration(
        self, own_client: Client, array_schema: ArraySchema, mocker: MockerFixture, storage_gone: bool
    ):
        own_client.create_collection("own", array_schema)
        adapter = own_client._adapter
        missing = Missing(adapter.collections_resource, storage_gone)
        mocker.patch("deker_local_adapters.collection_adapter.open", create=True, side_effect=missing)
        check_not_found(lambda: list(adapter), missing, [])
        gc.collect()

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="file gone")]
    )
    def test_collection_file_not_found_by_the_reading(
        self, own_client: Client, array_schema: ArraySchema, mocker: MockerFixture, storage_gone: bool
    ):
        """The collection file is taken away as it is opened, by a deletion meanwhile: no collection."""
        own_client.create_collection("own", array_schema)
        adapter = own_client._adapter
        missing = Missing(adapter.collections_resource, storage_gone)
        mocker.patch("deker_local_adapters.collection_adapter.open", create=True, side_effect=missing)
        check_not_found(partial(own_client.get_collection, "own"), missing, None)


class TakeAwayAfterMoves:
    """Stand for the move to the trash: after the given number of real moves, takes a directory away, as the deletion
    of the collection or the loss of the collections directory of the storage meanwhile does.

    :param real: the real move
    :param moves: number of the moves after which the directory is taken away
    :param directory: the directory to take away
    :param target: where it goes
    """

    def __init__(self, real, moves: int, directory: Path, target: Path) -> None:
        self.real = real
        self.moves = moves
        self.directory = directory
        self.target = target
        self.calls = 0

    def __call__(self, *args, **kwargs) -> None:
        self.real(*args, **kwargs)
        self.calls += 1
        if self.calls == self.moves:
            self.directory.rename(self.target)


class CreateAfterTheMove:
    """Stand for the move to the trash: after the move of the named directory, a creator makes an array in the
    collection, once, in the thread of the clearing.

    :param real: the real move
    :param name: name of the directory
    :param collection: the collection
    :param attributes: primary and custom attributes of the array
    """

    def __init__(self, real, name: str, collection: Collection, attributes: dict) -> None:
        self.real = real
        self.name = name
        self.collection = collection
        self.attributes = attributes
        self.created = None

    def __call__(self, path: Path, *args, **kwargs) -> None:
        self.real(path, *args, **kwargs)
        if path.name == self.name and self.created is None:
            self.created = self.collection.create(**self.attributes)


class TestClearingAgainstOthers:
    """The clearing moves the inner directories of the collection one by one: what the others do between the moves."""

    @pytest.mark.parametrize(
        ("collection_", "directory"),
        [
            pytest.param("array_collection_with_attributes", "array_symlinks", id="array"),
            pytest.param("varray_collection_with_attributes", "varray_symlinks", id="varray"),
        ],
    )
    def test_symlink_made_between_the_moves_without_its_file_is_removed(
        self, request, collection_adapter: LocalCollectionAdapter, mocker: MockerFixture, collection_: str, directory
    ):
        """A creator makes its file before the data directory is moved and its symlink after the symlinks directory
        is: the symlink without its file is removed at the end, the array is found neither by its attributes nor by
        its id.
        """
        collection: Collection = request.getfixturevalue(collection_)
        attributes = {
            "primary_attributes": {"primary_attribute": 3},
            "custom_attributes": {"custom_attribute": 0.5, "time_attr_name": datetime.now(timezone.utc)},
        }
        creator = CreateAfterTheMove(
            deker_local_adapters.collection_adapter.move_to_trash, directory, collection, attributes
        )
        mocker.patch.object(deker_local_adapters.collection_adapter, "move_to_trash", side_effect=creator)
        assert collection_adapter.clear(collection, background=False, _empty_trash=False, token=DefaultToken()) is None
        assert creator.created is not None
        assert collection.filter({"primary_attribute": 3}).last() is None
        assert files_under(collection.path) == [f"{collection.name}.json"]
        assert collection.filter({"id": creator.created.id}).last() is None
        name = creator.created.id + creator.created._adapter.file_ext
        trash = collection_adapter.trash_resource
        assert [path for path in files_under(trash) if path.endswith(name)] != []  # its file, moved with its directory
        collection_adapter.empty_trash(background=False)
        assert list(trash.iterdir()) == []

    @pytest.mark.parametrize(
        ("collection_", "directory"),
        [
            pytest.param("array_collection_with_attributes", "array_data", id="array"),
            pytest.param("varray_collection_with_attributes", "varray_data", id="varray"),
        ],
    )
    def test_array_made_after_its_directories_are_moved_stays(
        self, request, collection_adapter: LocalCollectionAdapter, mocker: MockerFixture, collection_: str, directory
    ):
        """A creator makes its file and its symlink after both their directories are moved: the array is whole and
        stays, its symlink too.
        """
        collection: Collection = request.getfixturevalue(collection_)
        attributes = {
            "primary_attributes": {"primary_attribute": 3},
            "custom_attributes": {"custom_attribute": 0.5, "time_attr_name": datetime.now(timezone.utc)},
        }
        creator = CreateAfterTheMove(
            deker_local_adapters.collection_adapter.move_to_trash, directory, collection, attributes
        )
        mocker.patch.object(deker_local_adapters.collection_adapter, "move_to_trash", side_effect=creator)
        assert collection_adapter.clear(collection, background=False, _empty_trash=True, token=DefaultToken()) is None
        assert collection.filter({"primary_attribute": 3}).last().id == creator.created.id
        assert len(files_under(collection.path)) == 3  # the metadata of the collection, the file and the symlink

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="collection deleted")]
    )
    def test_collection_taken_away_after_the_moves(
        self, own_client: Client, array_schema: ArraySchema, tmp_path: Path, mocker: MockerFixture, storage_gone: bool
    ):
        """The deletion of the collection after the moves leaves nothing else to move: not an error; the loss of the
        collections directory is critical.
        """
        collection = own_client.create_collection("own", array_schema)
        collection.create()
        adapter = own_client._adapter
        directory = adapter.collections_resource if storage_gone else collection.path
        taker = TakeAwayAfterMoves(
            deker_local_adapters.collection_adapter.move_to_trash, 4, directory, tmp_path / "away"
        )
        mocker.patch.object(deker_local_adapters.collection_adapter, "move_to_trash", side_effect=taker)
        clear = partial(adapter.clear, collection, background=False, _empty_trash=True, token=DefaultToken())
        check_not_found(clear, Missing(adapter.collections_resource, storage_gone), None)
        assert taker.calls == 4
