import errno
import gc
import os
import uuid
import warnings

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from functools import partial
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest

from cantok import DefaultToken
from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerFilterError, DekerInstanceNotExistsError, DekerLockError, DekerValidationError
from deker.flock import Flock
from deker.schemas import ArraySchema, AttributeSchema
from deker.tools import get_paths
from deker.tools.path import get_symlink_path
from deker.types.private.classes import Paths
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from tests.conftest import Missing, check_not_found

from deker_local_adapters import LocalArrayAdapter
from deker_local_adapters.errors import DekerBrokenSymlinkError
from deker_local_adapters.varray_adapter import LocalVArrayAdapter


class HeldCheck:
    """Replacement of ``check_symlink_existence`` that reports being inside the creation and waits to be let go.

    :param original: the replaced method
    """

    def __init__(self, original) -> None:
        self.original = original
        self.inside = Event()
        self.go = Event()

    def __call__(self, file: Path) -> None:
        self.inside.set()
        assert self.go.wait(10)
        self.original(file)


def twin(array: Array | VArray) -> Array | VArray:
    """Make another (V)Array with the same primary attributes.

    :param array: the array to copy the attributes from
    :returns: a new, not yet created, (V)Array
    """
    if isinstance(array, VArray):
        return VArray(
            array._VArray__collection,
            array._adapter,
            array._VArray__array_adapter,
            primary_attributes=array.primary_attributes,
            custom_attributes=array.custom_attributes,
        )
    return Array(
        array._Array__collection,
        array._adapter,
        primary_attributes=array.primary_attributes,
        custom_attributes=array.custom_attributes,
    )


def fill_symlink_directory(symlinks_path: Path, entries: tuple[str, ...] | None) -> None:
    """Make the symlink directory of the primary attributes with the entries.

    :param symlinks_path: the symlink directory
    :param entries: ``None`` for no directory; else the names of its entries: ``subdir`` is a directory,
        a name starting with ``broken`` is a symlink to a missing file, any other name is a file
    """
    if entries is None:
        return
    symlinks_path.mkdir(parents=True)
    for name in entries:
        if name == "subdir":
            (symlinks_path / name).mkdir()
        elif name.startswith("broken"):
            os.symlink(symlinks_path / "missing.hdf5", symlinks_path / name)
        else:
            (symlinks_path / name).write_text("x")


@pytest.mark.parametrize("adapter_", ["local_array_adapter", "local_varray_adapter"])
def test_broken_symlink(adapter_: str, request, local_array_adapter, local_varray_adapter):
    adapter = request.getfixturevalue(adapter_)
    if isinstance(adapter, LocalVArrayAdapter):
        collection = request.getfixturevalue("varray_collection")
        array = request.getfixturevalue("varray_with_attributes")
        schema = request.getfixturevalue("varray_schema_with_attributes")
        symlink_dir = "varray_symlinks"
    else:
        collection = request.getfixturevalue("array_collection")
        array = request.getfixturevalue("array_with_attributes")
        schema = request.getfixturevalue("array_schema_with_attributes")
        symlink_dir = "array_symlinks"

    symlinks_path = get_symlink_path(
        path_to_symlink_dir=collection.path / symlink_dir,
        primary_attributes_schema=schema.primary_attributes,
        primary_attributes=array.primary_attributes,
    )
    symlinks_path.mkdir(parents=True, exist_ok=True)
    path_for_file = symlinks_path / "test.txt"

    path_for_file.write_text("foo")

    os.symlink(path_for_file, symlinks_path / "symlink")
    path_for_file.unlink()
    with pytest.raises(DekerBrokenSymlinkError):
        adapter.get_by_primary_attributes(
            array.primary_attributes,
            schema,
            collection,
            local_array_adapter,
            local_varray_adapter if isinstance(adapter, LocalVArrayAdapter) else None,
        )


@pytest.mark.parametrize(
    ("entries", "expected"),
    [
        pytest.param(("subdir", "found.hdf5"), "found.hdf5", id="found"),
        pytest.param(("subdir", "broken.hdf5"), DekerBrokenSymlinkError, id="broken-symlink"),
        pytest.param(("subdir",), None, id="only-a-subdirectory"),
        pytest.param((), None, id="empty-directory"),
        pytest.param(None, None, id="no-directory"),
    ],
)
def test_symlink_by_primary_attributes_closes_the_directory(
    array_collection_with_attributes: Collection,
    array_with_attributes: Array,
    entries: tuple[str, ...] | None,
    expected: str | type[DekerBrokenSymlinkError] | None,
):
    """The search gives the same outcome as before and leaves no unclosed scandir iterator behind."""
    adapter = array_with_attributes._adapter
    schema = array_collection_with_attributes.array_schema
    attributes = array_with_attributes.primary_attributes
    symlinks_path = get_symlink_path(
        path_to_symlink_dir=array_collection_with_attributes.path / adapter.symlinks_dir,
        primary_attributes_schema=schema.primary_attributes,
        primary_attributes=attributes,
    )
    fill_symlink_directory(symlinks_path, entries)
    gc.collect()  # the garbage of the fixtures is not the search's
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        if expected is DekerBrokenSymlinkError:
            with pytest.raises(DekerBrokenSymlinkError, match="broken symlink"):
                adapter._symlink_by_primary_attributes(attributes, schema)
        elif expected is None:
            assert adapter._symlink_by_primary_attributes(attributes, schema) is None
        else:
            assert adapter._symlink_by_primary_attributes(attributes, schema) == Path(symlinks_path) / expected
        gc.collect()  # an unclosed iterator warns when it is collected
    assert [str(warning.message) for warning in caught if issubclass(warning.category, ResourceWarning)] == []


class TestMixinEdges:
    def test_get_varray_by_id(self, varray_collection: Collection, inserted_varray: VArray):
        found = varray_collection.filter({"id": inserted_varray.id}).first()
        assert isinstance(found, VArray)
        assert found.id == inserted_varray.id

    def test_get_varray_by_primary_attributes(
        self, varray_collection_with_attributes: Collection, inserted_varray_with_attributes: VArray
    ):
        found = varray_collection_with_attributes.filter(inserted_varray_with_attributes.primary_attributes).last()
        assert isinstance(found, VArray)
        assert found.id == inserted_varray_with_attributes.id

    def test_get_array_by_id(self, array_collection: Collection, inserted_array):
        found = array_collection.filter({"id": inserted_array.id}).first()
        assert found.id == inserted_array.id

    def test_subdirectory_in_symlink_directory_is_skipped(
        self,
        local_array_adapter: LocalArrayAdapter,
        array_collection: Collection,
        array_schema_with_attributes,
        array_with_attributes,
    ):
        from deker.tools.path import get_symlink_path

        symlinks_path = get_symlink_path(
            path_to_symlink_dir=array_collection.path / local_array_adapter.symlinks_dir,
            primary_attributes_schema=array_schema_with_attributes.primary_attributes,
            primary_attributes=array_with_attributes.primary_attributes,
        )
        (symlinks_path / "subdir").mkdir(parents=True)
        assert (
            local_array_adapter.get_by_primary_attributes(
                array_with_attributes.primary_attributes,
                array_schema_with_attributes,
                array_collection,
                local_array_adapter,
                None,
            )
            is None
        )

    def test_delete_when_the_main_directory_is_already_gone(
        self, local_array_adapter: LocalArrayAdapter, inserted_array, array_collection: Collection
    ):
        import shutil

        from deker.tools import get_paths

        paths = get_paths(inserted_array, array_collection.path)
        shutil.rmtree(paths.main)
        local_array_adapter.delete(inserted_array, background=False, _empty_trash=True, token=DefaultToken())
        assert not (paths.symlink / (inserted_array.id + local_array_adapter.file_ext)).exists()

    def test_empty_symlink_directory_finds_nothing(
        self,
        local_array_adapter: LocalArrayAdapter,
        array_collection: Collection,
        array_schema_with_attributes,
        array_with_attributes,
    ):
        from deker.tools.path import get_symlink_path

        symlinks_path = get_symlink_path(
            path_to_symlink_dir=array_collection.path / local_array_adapter.symlinks_dir,
            primary_attributes_schema=array_schema_with_attributes.primary_attributes,
            primary_attributes=array_with_attributes.primary_attributes,
        )
        symlinks_path.mkdir(parents=True, exist_ok=True)
        assert (
            local_array_adapter.get_by_primary_attributes(
                array_with_attributes.primary_attributes,
                array_schema_with_attributes,
                array_collection,
                local_array_adapter,
                None,
            )
            is None
        )

    def test_iter_skips_foreign_files(
        self, local_array_adapter: LocalArrayAdapter, inserted_array, array_collection: Collection
    ):
        data_dir = array_collection.path / local_array_adapter.data_dir
        (data_dir / "stray.txt").write_text("x")
        metas = list(local_array_adapter)
        assert [meta["id"] for meta in metas] == [inserted_array.id]

    def test_delete_removes_empty_parent_directories_only(
        self, local_array_adapter: LocalArrayAdapter, inserted_array, array_collection: Collection
    ):
        from deker.tools import get_paths

        paths = get_paths(inserted_array, array_collection.path)
        neighbour = paths.main.parent / "neighbour"
        neighbour.mkdir(parents=True)
        inserted_array.delete()
        assert not paths.main.exists()
        assert neighbour.exists()  # a non-empty parent is left alone
        assert (array_collection.path / local_array_adapter.data_dir).exists()

    def test_delete_of_a_symlink_without_target(
        self, local_array_adapter: LocalArrayAdapter, inserted_array, array_collection: Collection
    ):
        from deker.tools import get_paths

        paths = get_paths(inserted_array, array_collection.path)
        os.remove(paths.main / (inserted_array.id + local_array_adapter.file_ext))
        local_array_adapter.delete(
            inserted_array, background=False, _empty_trash=True, token=DefaultToken()
        )  # the target is already gone, the symlink is moved anyway
        assert not (paths.symlink / (inserted_array.id + local_array_adapter.file_ext)).exists()


class FirstRenameNotFound:
    """Stand for ``os.rename``: the first rename moves its file, as another deletion would, and does not find it.

    :param collections: the collections directory of the storage, taken away by the first rename too; ``None`` - kept
    """

    def __init__(self, collections: Path | None) -> None:
        self.collections = collections
        self.real = os.rename
        self.calls = 0

    def __call__(self, source, target, *args, **kwargs):
        self.calls += 1
        if self.calls > 1:
            return self.real(source, target, *args, **kwargs)
        self.real(source, Path(target).with_name("moved-by-another"))
        if self.collections is not None:
            self.real(self.collections, self.collections.with_name("collections-away"))
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(source))


class TestDeleteOfNeighbouringArrays:
    """Two Arrays whose ids share the first characters share the directories of the data tree.

    Deleting them at once from two threads is a race on those directories: the one that loses the removal
    of a shared directory must still remove the directories of its own symlink.
    """

    REPEATS = 50

    @staticmethod
    def _neighbours(array_collection: Collection, local_array_adapter: LocalArrayAdapter) -> tuple[Array, Array]:
        """Create two Arrays whose main files share the directories of the data tree.

        :param array_collection: the collection
        :param local_array_adapter: the adapter of the collection
        :returns: the Arrays
        """
        arrays = tuple(
            Array(array_collection, local_array_adapter, id_=f"8c{tail}0000-0000-4000-8000-{n:012d}")
            for n, tail in enumerate(("aa", "bb"))
        )
        for array in arrays:
            local_array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
        return arrays

    @staticmethod
    def _directories_left(array_collection: Collection, local_array_adapter: LocalArrayAdapter) -> list[Path]:
        """List every directory under the data and the symlinks directories of the collection.

        :param array_collection: the collection
        :param local_array_adapter: the adapter of the collection
        :returns: the directories, empty when the deletion was clean
        """
        return [
            Path(dirpath) / name
            for directory in (local_array_adapter.data_dir, local_array_adapter.symlinks_dir)
            for dirpath, dirnames, _ in os.walk(array_collection.path / directory)
            for name in dirnames
        ]

    def test_two_threads_leave_no_directories(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter
    ):
        """Whatever the order of the two threads, nothing stays under the data and the symlinks directories."""
        with ThreadPoolExecutor(max_workers=2) as executor:
            for _ in range(self.REPEATS):
                arrays = self._neighbours(array_collection, local_array_adapter)
                list(
                    executor.map(
                        partial(local_array_adapter.delete, background=False, _empty_trash=True, token=DefaultToken()),
                        arrays,
                    )
                )
                assert self._directories_left(array_collection, local_array_adapter) == []

    def test_a_directory_removed_by_the_neighbour_is_skipped(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter, mocker: MockerFixture
    ):
        """A shared directory that is gone by the time of rmdir was removed by the other thread: the climb goes on."""
        first, second = self._neighbours(array_collection, local_array_adapter)
        shared = array_collection.path / local_array_adapter.data_dir / "8" / "c"
        local_array_adapter.delete(first, background=False, _empty_trash=True, token=DefaultToken())
        assert shared.exists()
        real_rmdir = os.rmdir

        def rmdir_lost_to_the_neighbour(path: str | os.PathLike, **kwargs: object) -> None:
            real_rmdir(path, **kwargs)
            if Path(path) == shared:
                raise FileNotFoundError(2, "the neighbour was first", str(path))

        mocker.patch("deker_local_adapters.mixin.os.rmdir", rmdir_lost_to_the_neighbour)
        local_array_adapter.delete(second, background=False, _empty_trash=True, token=DefaultToken())
        assert self._directories_left(array_collection, local_array_adapter) == []

    def test_a_directory_filled_meanwhile_is_left(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter, mocker: MockerFixture
    ):
        """A directory that is not empty anymore at rmdir (something was created in it) stays, without an error."""
        first, second = self._neighbours(array_collection, local_array_adapter)
        shared = array_collection.path / local_array_adapter.data_dir / "8" / "c"
        local_array_adapter.delete(first, background=False, _empty_trash=True, token=DefaultToken())
        real_rmdir = os.rmdir

        def rmdir_of_a_filled_directory(path: str | os.PathLike, **kwargs: object) -> None:
            if Path(path) == shared:
                (shared / "newcomer").mkdir()
                raise OSError(errno.ENOTEMPTY, "Directory not empty", str(path))
            real_rmdir(path, **kwargs)

        mocker.patch("deker_local_adapters.mixin.os.rmdir", rmdir_of_a_filled_directory)
        local_array_adapter.delete(second, background=False, _empty_trash=True, token=DefaultToken())
        assert (shared / "newcomer").exists()
        left = [shared.parent, shared, shared / "newcomer"]
        assert self._directories_left(array_collection, local_array_adapter) == left
        symlink = array_collection.path / local_array_adapter.symlinks_dir / (second.id + local_array_adapter.file_ext)
        assert not symlink.exists()

    def test_other_errors_of_rmdir_are_raised(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter, mocker: MockerFixture
    ):
        """Any other failure of rmdir is an error of the deletion, as before."""
        array = array_collection.create()
        mocker.patch("deker_local_adapters.mixin.os.rmdir", side_effect=PermissionError(errno.EACCES, "no"))
        with pytest.raises(PermissionError):
            local_array_adapter.delete(array, background=False, _empty_trash=True, token=DefaultToken())
        mocker.stopall()
        local_array_adapter.delete(array, background=False, _empty_trash=True, token=DefaultToken())  # the rest

    def test_a_data_file_moved_meanwhile_is_not_an_error(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter, mocker: MockerFixture
    ):
        """Another deletion may move the data file first: the symlink is still moved."""
        array = array_collection.create()
        paths = get_paths(array, array_collection.path)
        file = array.id + local_array_adapter.file_ext
        mocker.patch.object(os, "rename", side_effect=FirstRenameNotFound(None))
        local_array_adapter.delete(array, background=False, _empty_trash=True, token=DefaultToken())
        assert not (paths.main / file).exists()
        assert not (paths.symlink / file).exists()
        assert self._directories_left(array_collection, local_array_adapter) == []


@pytest.mark.parametrize("array_", ["array_with_attributes", "varray_with_attributes"])
class TestSymlinkDirectoryLock:
    """Creation of a (V)Array with primary attributes holds the directory of its symlink exclusively."""

    @pytest.mark.timeout(60)
    def test_second_creator_waits_and_is_refused(self, array_: str, request, mocker: MockerFixture):
        """The second creator of the same primary attributes waits for the first one and then learns they are taken."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        held = HeldCheck(adapter.check_symlink_existence)
        mocker.patch.object(adapter, "check_symlink_existence", held)
        other = twin(array)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(adapter.create, array, timeout=Sentinel, token=DefaultToken())
            assert held.inside.wait(10)
            second = pool.submit(adapter.create, other, timeout=Sentinel, token=DefaultToken())
            with pytest.raises(FutureTimeoutError):
                second.result(timeout=0.5)  # the directory is held by the first creator
            held.go.set()
            assert first.result(timeout=10) is array
            with pytest.raises(DekerValidationError):
                second.result(timeout=10)
        paths = get_paths(array, adapter.collection_path)
        assert os.listdir(paths.symlink) == [array.id + adapter.file_ext]
        assert not (get_paths(other, adapter.collection_path).main / (other.id + adapter.file_ext)).exists()

    @pytest.mark.timeout(60)
    def test_creator_gives_up_after_the_timeout(self, array_: str, request, mocker: MockerFixture):
        """A creator that cannot take the directory within the timeout fails and leaves no files."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        mocker.patch.object(adapter.ctx.config, "write_lock_timeout", 1)
        paths = get_paths(array, adapter.collection_path)
        paths.create(adapter.collection_path)
        with Flock(paths.symlink), pytest.raises(DekerLockError):
            adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert os.listdir(paths.symlink) == []
        assert not (paths.main / (array.id + adapter.file_ext)).exists()

    def test_lock_is_released_after_creation(self, array_: str, request):
        """The directory is free once the (V)Array is created."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        adapter.create(array, timeout=Sentinel, token=DefaultToken())
        paths = get_paths(array, adapter.collection_path)
        with Flock(paths.symlink):
            pass

    def test_lock_is_released_after_a_failed_creation(self, array_: str, request):
        """The directory is free once a creator is refused."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        adapter.create(array, timeout=Sentinel, token=DefaultToken())
        with pytest.raises(DekerValidationError):
            adapter.create(twin(array), timeout=Sentinel, token=DefaultToken())
        with Flock(get_paths(array, adapter.collection_path).symlink):
            pass


class TestGetIdByPrimaryAttributes:
    """The id of an array by its primary attributes, without reading its metadata: a VArray finds its Arrays so."""

    def test_finds_the_id_of_an_array(
        self, array_collection_with_attributes: Collection, inserted_array_with_attributes: Array
    ):
        adapter = inserted_array_with_attributes._adapter
        found = adapter.get_id_by_primary_attributes(
            inserted_array_with_attributes.primary_attributes, array_collection_with_attributes.array_schema
        )
        assert found == inserted_array_with_attributes.id

    def test_finds_the_id_of_a_varray(
        self, varray_collection_with_attributes: Collection, inserted_varray_with_attributes: VArray
    ):
        adapter = inserted_varray_with_attributes._adapter
        found = adapter.get_id_by_primary_attributes(
            inserted_varray_with_attributes.primary_attributes, varray_collection_with_attributes.varray_schema
        )
        assert found == inserted_varray_with_attributes.id

    def test_finds_the_id_of_an_array_of_a_varray(self, inserted_varray: VArray):
        """The Arrays of a VArray are found by its id and the position in the vgrid."""
        subset = inserted_varray[:]
        vposition = subset._VSubset__arrays[0].vposition
        collection = inserted_varray._VArray__collection
        array = collection.arrays.filter({"vid": inserted_varray.id, "v_position": vposition}).last()
        found = subset._VSubset__array_adapter.get_id_by_primary_attributes(
            {"vid": inserted_varray.id, "v_position": vposition}, collection.array_schema
        )
        assert found == array.id

    def test_does_not_read_the_metadata(
        self,
        array_collection_with_attributes: Collection,
        inserted_array_with_attributes: Array,
        mocker: MockerFixture,
    ):
        adapter = inserted_array_with_attributes._adapter
        read_meta = mocker.spy(adapter, "read_meta")
        storage_read_meta = mocker.spy(adapter.storage_adapter, "read_meta")
        adapter.get_id_by_primary_attributes(
            inserted_array_with_attributes.primary_attributes, array_collection_with_attributes.array_schema
        )
        assert read_meta.call_count == 0
        assert storage_read_meta.call_count == 0

    def test_returns_none_without_an_array(
        self, array_collection_with_attributes: Collection, array_with_attributes: Array
    ):
        """An array that was never created has no symlink directory; an empty directory is the same."""
        adapter = array_with_attributes._adapter
        schema = array_collection_with_attributes.array_schema
        attributes = array_with_attributes.primary_attributes
        assert adapter.get_id_by_primary_attributes(attributes, schema) is None
        symlinks_path = get_symlink_path(
            path_to_symlink_dir=array_collection_with_attributes.path / adapter.symlinks_dir,
            primary_attributes_schema=schema.primary_attributes,
            primary_attributes=attributes,
        )
        symlinks_path.mkdir(parents=True, exist_ok=True)
        assert adapter.get_id_by_primary_attributes(attributes, schema) is None

    def test_returns_none_after_the_array_is_deleted(
        self, array_collection_with_attributes: Collection, inserted_array_with_attributes: Array
    ):
        adapter = inserted_array_with_attributes._adapter
        schema = array_collection_with_attributes.array_schema
        attributes = inserted_array_with_attributes.primary_attributes
        inserted_array_with_attributes.delete()
        assert adapter.get_id_by_primary_attributes(attributes, schema) is None

    def test_a_broken_symlink_raises(self, array_collection_with_attributes: Collection, array_with_attributes: Array):
        adapter = array_with_attributes._adapter
        schema = array_collection_with_attributes.array_schema
        symlinks_path = get_symlink_path(
            path_to_symlink_dir=array_collection_with_attributes.path / adapter.symlinks_dir,
            primary_attributes_schema=schema.primary_attributes,
            primary_attributes=array_with_attributes.primary_attributes,
        )
        symlinks_path.mkdir(parents=True, exist_ok=True)
        target = symlinks_path / "target.hdf5"
        target.write_text("gone")
        os.symlink(target, symlinks_path / "link.hdf5")
        target.unlink()
        with pytest.raises(DekerBrokenSymlinkError):
            adapter.get_id_by_primary_attributes(array_with_attributes.primary_attributes, schema)

    def test_finds_the_same_array_as_the_filter(
        self, array_collection_with_attributes: Collection, inserted_array_with_attributes: Array
    ):
        """Both ways of looking an array up agree: the id and the whole array found by the primary attributes."""
        adapter = inserted_array_with_attributes._adapter
        schema = array_collection_with_attributes.array_schema
        attributes = inserted_array_with_attributes.primary_attributes
        found = adapter.get_by_primary_attributes(attributes, schema, array_collection_with_attributes, adapter, None)
        assert found is not None
        assert adapter.get_id_by_primary_attributes(attributes, schema) == found.id

    def test_a_schema_without_primary_attributes_is_refused(self, array_collection: Collection, inserted_array: Array):
        """Without primary attributes nothing identifies an array: the first file of the directory is not the answer."""
        adapter = inserted_array._adapter
        with pytest.raises(DekerFilterError, match="primary attributes"):
            adapter.get_id_by_primary_attributes({}, array_collection.array_schema)

    @pytest.mark.parametrize(
        ("attributes", "message"),
        [
            pytest.param({}, "primary_attribute is missing", id="missing"),
            pytest.param({"primary_attribute": 1, "extra": 2}, "don't exist in schema", id="extra"),
        ],
    )
    def test_incomplete_or_extra_attributes_are_refused(
        self,
        array_collection_with_attributes: Collection,
        inserted_array_with_attributes: Array,
        attributes: dict,
        message: str,
    ):
        adapter = inserted_array_with_attributes._adapter
        with pytest.raises(DekerFilterError, match=message):
            adapter.get_id_by_primary_attributes(attributes, array_collection_with_attributes.array_schema)


class TestGetByPrimaryAttributesChecksTheAttributes:
    """The whole-array search refuses an empty, incomplete or oversized set of attributes, like filter does."""

    def test_a_schema_without_primary_attributes_is_refused(self, array_collection: Collection, inserted_array: Array):
        adapter = inserted_array._adapter
        with pytest.raises(DekerFilterError, match="primary attributes"):
            adapter.get_by_primary_attributes({}, array_collection.array_schema, array_collection, adapter, None)

    @pytest.mark.parametrize(
        ("attributes", "message"),
        [
            pytest.param({}, "primary_attribute is missing", id="missing"),
            pytest.param({"primary_attribute": 1, "extra": 2}, "don't exist in schema", id="extra"),
        ],
    )
    def test_incomplete_or_extra_attributes_are_refused(
        self,
        array_collection_with_attributes: Collection,
        inserted_array_with_attributes: Array,
        attributes: dict,
        message: str,
    ):
        adapter = inserted_array_with_attributes._adapter
        with pytest.raises(DekerFilterError, match=message):
            adapter.get_by_primary_attributes(
                attributes,
                array_collection_with_attributes.array_schema,
                array_collection_with_attributes,
                adapter,
                None,
            )


class MoveAwayOnFirstRead:
    """Stand for ``read_meta`` of an adapter class: the first call moves the main file away first, as a deletion
    meanwhile does; every call does the real work then.

    :param method: the real method
    :param target: where the file is moved to
    """

    def __init__(self, method, target: Path) -> None:
        self.method = method
        self.target = target
        self.calls = 0
        self.gone: list[Path] = []

    def __call__(self, adapter, path: Path):
        self.calls += 1
        if self.calls == 1:
            path.rename(self.target / path.name)
            self.gone.append(path)
        return self.method(adapter, path)


class TestArrayWhoseFileIsGone:
    """An Array or a VArray whose main file is gone does not exist: it is deleted, not found, skipped by the walk."""

    @pytest.mark.parametrize("array_", ["inserted_array", "inserted_varray"])
    @pytest.mark.parametrize("gone", ["nothing", "file", "file-and-directory"])
    def test_is_deleted(self, request, tmp_path: Path, array_: str, gone: str):
        """The directory of the main file may stay: the Array is deleted when its main file is gone."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        file = adapter._get_main_path_to_file(array)
        if gone != "nothing":
            file.rename(tmp_path / file.name)
        if gone == "file-and-directory":
            file.parent.rmdir()
        try:
            assert adapter.is_deleted(array) is (gone != "nothing")
        finally:
            if gone != "nothing":
                file.parent.mkdir(exist_ok=True)
                (tmp_path / file.name).rename(file)

    @pytest.mark.parametrize("array_", ["inserted_array", "inserted_varray"])
    def test_get_by_id_of_a_file_gone_meanwhile(self, request, tmp_path: Path, mocker: MockerFixture, array_: str):
        """The file is there when it is looked for and gone when it is read: the Array is not found."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        file = adapter._get_main_path_to_file(array)
        read = MoveAwayOnFirstRead(type(adapter).read_meta, tmp_path)
        mocker.patch.object(type(adapter), "read_meta", autospec=True, side_effect=read)
        if isinstance(array, VArray):
            arguments = (array._VArray__collection, array._VArray__array_adapter, adapter)
        else:
            arguments = (array._Array__collection, adapter, None)
        try:
            assert adapter.get_by_id(array.id, *arguments) is None
            assert read.calls == 1
            assert read.gone == [file]
        finally:
            (tmp_path / file.name).rename(file)

    @pytest.mark.parametrize("collection_", ["array_collection", "varray_collection"])
    def test_walk_skips_what_disappears(self, request, tmp_path: Path, mocker: MockerFixture, collection_: str):
        """The other arrays are yielded, the one gone meanwhile is skipped without an error."""
        collection: Collection = request.getfixturevalue(collection_)
        arrays = [collection.create() for _ in range(2)]
        adapter = arrays[0]._adapter
        read = MoveAwayOnFirstRead(type(adapter).read_meta, tmp_path)
        mocker.patch.object(type(adapter), "read_meta", autospec=True, side_effect=read)
        try:
            metas = list(adapter)
            assert read.calls == 2
            assert len(read.gone) == 1
            gone = read.gone[0].name.removesuffix(adapter.file_ext)
            assert [meta["id"] for meta in metas] == [array.id for array in arrays if array.id != gone]
        finally:
            for file in read.gone:
                (tmp_path / file.name).rename(file)
            for array in arrays:
                array.delete()


# --- the rules of the creators: the collection is never made again, the created files are checked afterwards ---------

BEFORE_FIRST_DIRECTORY = "before the first directory"
BETWEEN_DIRECTORY_LEVELS = "between the levels of the directory of the main file"
BEFORE_SYMLINK_DIRECTORIES = "between the directory of the main file and the directory of the symlink"
BEFORE_MAIN_FILE = "after the directories, before the main file"
WHILE_MAIN_FILE_IS_WRITTEN = "while the main file is written"
BEFORE_SYMLINK = "before the symlink"
BEFORE_CHECK = "before the check"
AFTER_CHECK = "after the check"

CREATORS = ["array", "array_with_attributes", "varray", "varray_with_attributes"]


class At:
    """Hook of a creator: runs an action once, when the creator reaches the point.

    :param point: point of the creator the action runs at
    :param action: function to call
    :param arguments: arguments of the function
    """

    def __init__(self, point: str, action, *arguments) -> None:
        self.point = point
        self.action = action
        self.arguments = arguments
        self.done = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.done:
            self.done = True
            self.action(*self.arguments)


class Around:
    """Stand for a function a creator calls on its way: tells the hook the points before and after the calls.

    :param original: the real function
    :param hook: hook of the creator
    :param before: points told before the calls, by the number of the call from 0
    :param after: points told after the calls, by the number of the call from 0, whatever the call ended with
    """

    def __init__(
        self, original, hook, before: dict[int, str] | None = None, after: dict[int, str] | None = None
    ) -> None:
        self.original = original
        self.hook = hook
        self.before = before or {}
        self.after = after or {}
        self.calls = 0

    def __call__(self, *args, **kwargs):
        number = self.calls
        self.calls += 1
        if number in self.before:
            self.hook(self.before[number])
        try:
            return self.original(*args, **kwargs)
        finally:
            if number in self.after:
                self.hook(self.after[number])


def created_paths(array: Array | VArray) -> tuple[Path, Path]:
    """Build the paths of the main file and of the symlink of a (V)Array, without touching the disk.

    :param array: the (V)Array
    :returns: the main file and the symlink
    """
    adapter = array._adapter
    paths = get_paths(array, adapter.collection_path)
    name = array.id + adapter.file_ext
    return paths.main / name, paths.symlink / name


def watch_creator(mocker: MockerFixture, array: Array | VArray, hook) -> None:
    """Tell the hook every point the creator of a (V)Array reaches, by the calls it makes on its way.

    :param mocker: mocker of the test: the calls are restored after it
    :param array: the (V)Array to be created
    :param hook: hook of the creator
    """
    adapter = array._adapter
    # the levels of the directory of the main file are made first, one ``os.mkdir`` each, then those of the symlink
    levels = len(created_paths(array)[0].parent.relative_to(adapter.collection_path).parts)
    directories = Around(Paths.create, hook, before={0: BEFORE_FIRST_DIRECTORY})
    mocker.patch.object(Paths, "create", autospec=True, side_effect=directories)
    mkdir = Around(os.mkdir, hook, before={levels: BEFORE_SYMLINK_DIRECTORIES}, after={0: BETWEEN_DIRECTORY_LEVELS})
    mocker.patch.object(os, "mkdir", side_effect=mkdir)
    main = Around(adapter._create_main_file, hook, before={0: BEFORE_MAIN_FILE})
    mocker.patch.object(adapter, "_create_main_file", side_effect=main)
    written = {0: WHILE_MAIN_FILE_IS_WRITTEN}
    if isinstance(adapter, LocalVArrayAdapter):
        # the json file is open when its metadata is made
        mocker.patch.object(
            VArray, "_create_meta", autospec=True, side_effect=Around(VArray._create_meta, hook, written)
        )
    else:
        # the hdf5 file is open when its metadata dataset is made
        storage = adapter.storage_adapter
        mocker.patch.object(
            storage, "_create_meta_dataset", side_effect=Around(storage._create_meta_dataset, hook, written)
        )
    symlink = Around(os.symlink, hook, before={0: BEFORE_SYMLINK}, after={0: BEFORE_CHECK})
    mocker.patch.object(os, "symlink", side_effect=symlink)
    created = Around(adapter._create_on_disk, hook, after={0: AFTER_CHECK})
    mocker.patch.object(adapter, "_create_on_disk", side_effect=created)


class Trash:
    """Stand for the trash of the deletion design: what is deleted leaves the collection in one rename.

    :param directory: directory to move into, made here
    """

    def __init__(self, directory: Path) -> None:
        directory.mkdir()
        self.directory = directory
        self.moved: list[tuple[Path, Path]] = []

    def move(self, path: Path) -> None:
        """Move a file or a directory here under a new name; a missing one is not an error.

        :param path: what to move
        """
        target = self.directory / uuid.uuid4().hex
        try:
            os.rename(path, target)
        except FileNotFoundError:
            return
        self.moved.append((path, target))

    def clear_collection(self, collection: Path) -> None:
        """Move every inner directory of the collection here, one rename each, as the clearing does.

        :param collection: directory of the collection
        """
        for name in sorted(os.listdir(collection)):
            if (collection / name).is_dir():
                self.move(collection / name)

    def put_back(self, path: Path) -> None:
        """Put what was moved from the path back, for the fixtures; whatever was made at the path meanwhile comes here.

        :param path: the path
        """
        if os.path.lexists(path):
            os.rename(path, self.directory / uuid.uuid4().hex)
        for source, target in self.moved:
            if source == path:
                os.rename(target, path)
                return


def files(root: Path) -> list[str]:
    """List the files and the symlinks under a directory, relative to it; the directories are not listed.

    :param root: directory to walk
    :returns: sorted relative paths
    """
    return sorted(
        os.path.relpath(os.path.join(directory, name), root) for directory, _, names in os.walk(root) for name in names
    )


def gone_path(gone: str, array: Array | VArray) -> Path:
    """Name the path an error of the creator tells about.

    :param gone: ``collection``, ``data directory``, ``main directory``, ``symlink directory``, ``main file`` or
        ``symlink``
    :param array: the (V)Array being created
    :returns: the path
    """
    adapter = array._adapter
    main, symlink = created_paths(array)
    paths = {
        "collection": adapter.collection_path,
        "data directory": adapter.collection_path / adapter.data_dir,
        "main directory": main.parent,
        "symlink directory": symlink.parent,
        "main file": main,
        "symlink": symlink,
    }
    return paths[gone]


def creation_files(array: Array | VArray, created: tuple[str, ...]) -> list[str]:
    """List the files of a collection with a (V)Array being created, relative to the collection.

    :param array: the (V)Array
    :param created: what of the (V)Array is there: ``main``, ``symlink``
    :returns: sorted relative paths, the metadata file of the collection with them
    """
    collection = array._adapter.collection_path
    main, symlink = created_paths(array)
    paths = {"main": main, "symlink": symlink}
    return sorted([f"{collection.name}.json", *(str(paths[name].relative_to(collection)) for name in created)])


@pytest.mark.parametrize("array_", CREATORS)
class TestCreatorMeetsTheDeletionOfTheCollection:
    """Wherever the deletion of the collection comes, nothing of the creator stays among the collections."""

    @pytest.mark.parametrize(
        ("point", "gone", "moved"),
        [
            (BEFORE_FIRST_DIRECTORY, "collection", ()),
            (BETWEEN_DIRECTORY_LEVELS, "data directory", ()),
            (BEFORE_SYMLINK_DIRECTORIES, "collection", ()),
            (BEFORE_MAIN_FILE, "main directory", ()),
            (WHILE_MAIN_FILE_IS_WRITTEN, "symlink directory", ("main",)),
            (BEFORE_SYMLINK, "symlink directory", ("main",)),
            (BEFORE_CHECK, "main file", ("main", "symlink")),
            (AFTER_CHECK, None, ("main", "symlink")),
        ],
    )
    def test_creator_never_makes_the_collection_again(
        self, array_: str, point: str, gone: str | None, moved: tuple[str, ...], request, tmp_path, mocker
    ):
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        collection = adapter.collection_path
        trash = Trash(tmp_path / "trash")
        hook = At(point, trash.move, collection)
        watch_creator(mocker, array, hook)
        try:
            if gone is None:
                assert adapter.create(array, timeout=Sentinel, token=DefaultToken()) is array
            else:
                with pytest.raises(DekerInstanceNotExistsError) as error:
                    adapter.create(array, timeout=Sentinel, token=DefaultToken())
                assert str(error.value) == f"{gone_path(gone, array)} doesn't exist"
            assert hook.done
            assert not collection.exists()
            (hidden,) = os.listdir(trash.directory)
            assert files(trash.directory / hidden) == creation_files(array, moved)
        finally:
            trash.put_back(collection)


class TestCreatorMeetsTheClearingOfTheCollection:
    """A clearing that moves all the inner directories at one point leaves the collection whole, without leftovers."""

    @pytest.mark.parametrize(
        ("point", "gone", "kept"),
        [
            (BEFORE_FIRST_DIRECTORY, None, ("main", "symlink")),
            (BETWEEN_DIRECTORY_LEVELS, "data directory", ()),
            (BEFORE_SYMLINK_DIRECTORIES, "main directory", ()),
            (BEFORE_MAIN_FILE, "main directory", ()),
            (WHILE_MAIN_FILE_IS_WRITTEN, "symlink directory", ()),
            (BEFORE_SYMLINK, "symlink directory", ()),
            (BEFORE_CHECK, "main file", ()),
            (AFTER_CHECK, None, ()),
        ],
    )
    def test_creator_leaves_no_orphan(
        self, array: Array, point: str, gone: str | None, kept: tuple[str, ...], tmp_path, mocker
    ):
        adapter = array._adapter
        collection = adapter.collection_path
        old = Array(array._Array__collection, adapter)
        adapter.create(old, timeout=Sentinel, token=DefaultToken())
        trash = Trash(tmp_path / "trash")
        hook = At(point, trash.clear_collection, collection)
        watch_creator(mocker, array, hook)
        if gone is None:
            assert adapter.create(array, timeout=Sentinel, token=DefaultToken()) is array
        else:
            with pytest.raises(DekerInstanceNotExistsError) as error:
                adapter.create(array, timeout=Sentinel, token=DefaultToken())
            assert str(error.value) == f"{gone_path(gone, array)} doesn't exist"
        assert hook.done
        assert files(collection) == creation_files(array, kept)
        # the old Array is moved away with the clearing: its main file and its symlink
        assert sum(name.endswith(old.id + adapter.file_ext) for name in files(trash.directory)) == 2


class TestCreatorChecksItsFiles:
    """The creator checks its files after it made them, removes what it made if one is gone and tells so."""

    @pytest.mark.parametrize("array_", CREATORS)
    @pytest.mark.parametrize(("gone", "other"), [("main file", "symlink"), ("symlink", "main file")])
    def test_file_gone_before_the_check_takes_the_other_away(
        self, array_: str, gone: str, other: str, request, tmp_path, mocker
    ):
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        trash = Trash(tmp_path / "trash")
        hook = At(BEFORE_CHECK, trash.move, gone_path(gone, array))
        watch_creator(mocker, array, hook)
        with pytest.raises(DekerInstanceNotExistsError) as error:
            adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert str(error.value) == f"{gone_path(gone, array)} doesn't exist"
        assert hook.done
        assert not os.path.lexists(gone_path(other, array))
        assert len(os.listdir(trash.directory)) == 1

    @pytest.mark.parametrize("array_", CREATORS)
    def test_created_files_pass_the_check(self, array_: str, request, mocker):
        """Guard against a false rollback: a (V)Array without primary attributes keeps its symlink in the root of the
        symlinks directory, one with them in the directory of their values; both pass the check."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        main, symlink = created_paths(array)
        unlink = mocker.spy(Path, "unlink")
        assert adapter.create(array, timeout=Sentinel, token=DefaultToken()) is array
        assert main.is_file()
        assert symlink.is_symlink()
        assert symlink.resolve() == main
        root = adapter.collection_path / adapter.symlinks_dir
        assert (symlink.parent == root) is (not array.primary_attributes)
        unlink.assert_not_called()

    def test_symlink_directory_gone_before_its_lock_is_not_existing(
        self, local_array_adapter: LocalArrayAdapter, tmp_path
    ):
        directory = tmp_path / "gone"
        with pytest.raises(DekerInstanceNotExistsError) as error:
            local_array_adapter._lock_symlink_directory(directory, timeout=Sentinel, token=DefaultToken())
        assert str(error.value) == f"{directory} doesn't exist"

    def test_symlink_directory_gone_before_its_check_is_not_existing(
        self, local_array_adapter: LocalArrayAdapter, tmp_path
    ):
        directory = tmp_path / "gone"
        with pytest.raises(DekerInstanceNotExistsError) as error:
            local_array_adapter.check_symlink_existence(directory / "array.hdf5")
        assert str(error.value) == f"{directory} doesn't exist"


STORAGE_GONE = [pytest.param(True, id="collections gone"), pytest.param(False, id="file gone")]


class TestNotFoundTellsAMissingStorageFromAMissingFile:
    """Every place of the common part of the adapters that does not find a file tells a missing collections
    directory of the storage (critical) from a missing file (the answer as before).
    """

    @pytest.fixture
    def array(self, own_client: Client, array_schema: ArraySchema) -> Array:
        """An Array of a storage of the test's own.

        :param own_client: client of the storage
        :param array_schema: schema of the collection
        """
        return own_client.create_collection("own", array_schema).create()

    @staticmethod
    def files(array: Array) -> tuple[Path, Path]:
        """The main file and the symlink of the Array.

        :param array: the Array
        :returns: the main file and the symlink
        """
        adapter = array._adapter
        paths = get_paths(array, adapter.collection_path)
        name = array.id + adapter.file_ext
        return paths.main / name, paths.symlink / name

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_main_file_not_found_by_is_deleted(self, array: Array, storage_gone: bool):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        os.remove(self.files(array)[0])
        missing.take_away()
        check_not_found(lambda: adapter.is_deleted(array), missing, True)

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_symlink_not_found_by_the_deletion(self, array: Array, storage_gone: bool):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        _, symlink = self.files(array)
        symlink.unlink()
        missing.take_away()
        check_not_found(lambda: adapter._delete(symlink), missing, None)

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_main_file_not_found_by_the_deletion(self, array: Array, storage_gone: bool, mocker: MockerFixture):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        _, symlink = self.files(array)
        mocker.patch.object(
            os, "rename", side_effect=FirstRenameNotFound(missing.collections if storage_gone else None)
        )
        check_not_found(lambda: adapter._delete(symlink), missing, None)
        assert not os.path.lexists(symlink) or storage_gone

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_symlink_directory_not_found_by_its_check(self, array: Array, storage_gone: bool):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        file = adapter.collection_path / adapter.symlinks_dir / "absent" / "file.hdf5"
        missing.take_away()
        check_not_found(lambda: adapter.check_symlink_existence(file), missing, f"{file.parent} doesn't exist")

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_symlink_directory_not_found_by_its_lock(self, array: Array, storage_gone: bool):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        directory = adapter.collection_path / adapter.symlinks_dir / "absent"
        missing.take_away()
        check_not_found(
            lambda: adapter._lock_symlink_directory(directory, timeout=Sentinel, token=DefaultToken()),
            missing,
            f"{directory} doesn't exist",
        )

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_directory_of_the_main_file_not_found_by_the_creation(
        self, array: Array, storage_gone: bool, mocker: MockerFixture
    ):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        main, symlink = self.files(array)
        os.remove(main)
        symlink.unlink()
        mocker.patch.object(adapter, "_create_main_file", side_effect=missing)
        check_not_found(
            lambda: adapter._create_files(array, main, symlink, ()), missing, f"{main.parent} doesn't exist"
        )

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_directory_of_the_symlink_not_found_by_the_creation(
        self, array: Array, storage_gone: bool, mocker: MockerFixture
    ):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        main, symlink = self.files(array)
        os.remove(main)
        symlink.unlink()
        mocker.patch.object(os, "symlink", side_effect=missing)
        check_not_found(
            lambda: adapter._create_files(array, main, symlink, ()), missing, f"{symlink.parent} doesn't exist"
        )
        assert not main.exists()

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_checked_path_not_found_after_the_creation(self, array: Array, storage_gone: bool, mocker: MockerFixture):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        main, symlink = self.files(array)
        os.remove(main)
        symlink.unlink()
        checked = adapter.collection_path / "absent"
        make_symlink = os.symlink

        def symlink_then_take_away(*args, **kwargs):
            make_symlink(*args, **kwargs)
            missing.take_away()

        mocker.patch.object(os, "symlink", side_effect=symlink_then_take_away)
        check_not_found(
            lambda: adapter._create_files(array, main, symlink, (checked,)), missing, f"{checked} doesn't exist"
        )
        assert not os.path.lexists(main)
        assert not os.path.lexists(symlink)

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_symlink_directory_of_primary_attributes_not_found_by_the_search(self, array: Array, storage_gone: bool):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        schema = SimpleNamespace(primary_attributes=(AttributeSchema(name="x", dtype=int, primary=True),))
        missing.take_away()
        check_not_found(lambda: adapter._symlink_by_primary_attributes({"x": 1}, schema), missing, None)

    @pytest.mark.parametrize("storage_gone", STORAGE_GONE)
    def test_main_file_not_found_by_its_id(self, array: Array, storage_gone: bool):
        adapter = array._adapter
        missing = Missing(adapter.collection_path.parent, storage_gone)
        missing.take_away()
        check_not_found(lambda: adapter.get_by_id(str(uuid.uuid4()), array._collection, adapter, None), missing, None)
