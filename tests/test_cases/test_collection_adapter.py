import errno
import gc
import json
import logging
import os
import shutil
import threading
import time

from concurrent.futures import ThreadPoolExecutor
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
    DekerLockError,
    DekerMemoryError,
    DekerTrashError,
)
from deker.flock import Flock
from deker.schemas import ArraySchema, DimensionSchema
from deker_tools.cancel import TokenCancelledError
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from tests.conftest import MISSING, Missing, check_not_found
from tests.parameters.common import random_string
from tests.parameters.uri import embedded_uri

from deker_local_adapters import AdaptersFactory, LocalCollectionAdapter
from deker_local_adapters.collection_adapter import empty_trash_directory, move_to_trash
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter


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
        collection_adapter.create(
            Collection(
                name=name,
                schema=array_schema,
                adapter=collection_adapter,
                factory=factory,
                storage_adapter=HDF5StorageAdapter,
            ),
            timeout=Sentinel,
            token=DefaultToken(),
        )
        assert root_path.joinpath(factory.ctx.config.collections_directory).joinpath(name).exists()
        # the lock file of the creation stays as the marker of the collection
        assert root_path.joinpath(factory.ctx.config.collections_directory).joinpath(name + ".lock").exists()

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

    def test_collection_adapter_deletes_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        array_collection: Collection,
        root_path: Path,
    ):
        """Tests if array_collection adapter deletes array_collection from db properly.

        :param collection_adapter: array_collection adapter
        :param array_collection: Pre created array_collection
        :param root_path: Path to collections directory
        """
        collection_adapter.delete(array_collection, timeout=Sentinel, token=DefaultToken())
        assert not os.path.exists(array_collection.path)
        assert not os.path.exists(array_collection.path.parent / (array_collection.name + ".lock"))

    def test_collection_adapter_delete_tolerates_a_collection_already_gone(
        self, collection_adapter: LocalCollectionAdapter, array_collection: Collection
    ):
        """Deleting a collection whose directory is gone already is not an error: the lock file is removed anyway."""
        shutil.rmtree(array_collection.path)
        lock = array_collection.path.parent / (array_collection.name + ".lock")
        assert lock.exists()
        collection_adapter.delete(array_collection, timeout=Sentinel, token=DefaultToken())
        assert not lock.exists()

    def test_collection_adapter_delete_raises_when_the_directory_cannot_be_removed(
        self, collection_adapter: LocalCollectionAdapter, array_collection: Collection, mocker: MockerFixture
    ):
        """A failure to remove the directory is not swallowed: the collection would stay with its files."""
        # a real OSError carries errno and strerror: rmtree of Python 3.12+ adds the filename to it before raising
        mocker.patch.object(os, "rmdir", side_effect=PermissionError(errno.EACCES, "read-only storage"))
        with pytest.raises(PermissionError, match="read-only storage"):
            collection_adapter.delete(array_collection, timeout=Sentinel, token=DefaultToken())
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
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
        root_path: Path,
    ):
        """Tests clearing data directory.

        :param array_collection: Pre created collection
        :param collection_adapter: collection adapter
        :param root_path: Path to collections directory
        """
        collection_adapter.clear(array_collection, timeout=Sentinel, token=DefaultToken())

        collection_dir = None
        for child in root_path.joinpath(collection_adapter.ctx.config.collections_directory).iterdir():
            if child.name == array_collection.name:
                collection_dir = child

        assert collection_dir is not None
        collection_dir = os.listdir(collection_dir)
        assert f"{array_collection.name}.json" in collection_dir
        assert os.path.exists(array_collection.path.parent / (array_collection.name + ".lock"))

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


class TestCollectionAdapterEdges:
    def test_create_path_raises_if_directory_exists(self, collection_adapter: LocalCollectionAdapter):
        name = "already-there"
        path = collection_adapter._LocalCollectionAdapter__create_path(name)
        try:
            with pytest.raises(DekerCollectionAlreadyExistsError):
                collection_adapter._LocalCollectionAdapter__create_path(name)
        finally:
            shutil.rmtree(path)

    def test_create_writes_nothing_into_a_missing_directory(
        self, collection_adapter: LocalCollectionAdapter, root_path: Path
    ):
        collection_adapter._LocalCollectionAdapter__create("ghost", {}, root_path / "ghost")
        assert not list(root_path.rglob("ghost*"))

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
            getattr(collection_adapter, operation)(array_collection, timeout=Sentinel, token=token)
        assert array_collection.path.exists()
        assert [a.id for a in array_collection] == [array.id]

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
    def test_clear_waits_for_the_collection_up_to_the_timeout(self, array_collection: Collection, collection_adapter):
        array = array_collection.create()
        with Flock(collection_adapter.collections_resource / (array_collection.name + ".lock")):
            start = time.monotonic()
            with pytest.raises(DekerLockError):
                collection_adapter.clear(array_collection, timeout=1, token=DefaultToken())
            assert time.monotonic() - start < 2.5
        assert [a.id for a in array_collection] == [array.id]


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
                lambda adapter, collection: adapter.delete(collection, timeout=Sentinel, token=DefaultToken()),
                id="delete",
            ),
            pytest.param(
                lambda adapter, collection: adapter.create(collection, timeout=Sentinel, token=DefaultToken()),
                id="create",
            ),
            pytest.param(
                lambda adapter, collection: adapter.clear(collection, timeout=Sentinel, token=DefaultToken()),
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

    def test_creation_of_a_collection_never_creates_the_collections_directory(
        self, tmp_path: Path, collection_adapter: LocalCollectionAdapter, mocker: MockerFixture
    ):
        missing = tmp_path / "collections"
        mocker.patch.object(LocalCollectionAdapter, "collections_resource", missing)
        with pytest.raises(FileNotFoundError):
            collection_adapter._LocalCollectionAdapter__create_path("new")
        assert not missing.exists()


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
