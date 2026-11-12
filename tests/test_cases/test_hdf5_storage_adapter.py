"""Direct tests of the HDF5 storage adapter: the ``empty_cells`` counter and the ``data`` dataset life cycle."""

import json

from pathlib import Path

import h5py
import numpy as np
import pytest

from deker.errors import DekerArrayError

from deker_local_adapters.storage_adapters.hdf5.hdf5_options import HDF5CompressionOpts, HDF5Options
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter

SHAPE = (100,)
DTYPE = np.float64
FILL = np.nan
TOTAL = 100


@pytest.fixture
def adapter() -> HDF5StorageAdapter:
    return HDF5StorageAdapter()


@pytest.fixture
def array_file(tmp_path: Path, adapter: HDF5StorageAdapter) -> Path:
    path = tmp_path / "array.hdf5"
    adapter.create(path, SHAPE, {"id": "test"})
    return path


def empty_cells(path: Path) -> int:
    with h5py.File(path, "r", locking=False) as f:
        return int(f["empty_cells"][()])


def has_data(path: Path) -> bool:
    with h5py.File(path, "r", locking=False) as f:
        return "data" in f


def real_values(path: Path) -> int:
    with h5py.File(path, "r", locking=False) as f:
        return int(np.count_nonzero(~np.isnan(f["data"][()])))


def write(adapter: HDF5StorageAdapter, path: Path, bounds, data) -> None:
    adapter.update_data(path, bounds, np.asarray(data, dtype=DTYPE), DTYPE, SHAPE, FILL, None)


class TestEmptyCellsCounter:
    def test_new_array_is_fully_empty(self, array_file):
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_first_write_counts_written_cells(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        assert empty_cells(array_file) == 50
        assert real_values(array_file) == 50

    def test_overwrite_does_not_count_twice(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        write(adapter, array_file, np.index_exp[0:50], np.arange(50) + 1)
        assert empty_cells(array_file) == 50

    def test_writing_fill_values_over_data_frees_cells(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        write(adapter, array_file, np.index_exp[0:25], np.full(25, FILL))
        assert empty_cells(array_file) == 75
        assert real_values(array_file) == 25

    def test_writing_fill_values_everywhere_deletes_dataset(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        write(adapter, array_file, np.index_exp[0:50], np.full(50, FILL))
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_writing_fill_values_into_empty_array_creates_nothing(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.full(50, FILL))
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_mixed_data_counts_only_fill_values(self, adapter, array_file):
        data = np.arange(50, dtype=DTYPE)
        data[::2] = FILL
        write(adapter, array_file, np.index_exp[0:50], data)
        assert empty_cells(array_file) == 75
        assert real_values(array_file) == 25

    def test_single_cell_write(self, adapter, array_file):
        adapter.update_data(array_file, np.index_exp[7], np.float64(1.5), DTYPE, SHAPE, FILL, None)
        assert empty_cells(array_file) == 99
        adapter.update_data(array_file, np.index_exp[7], np.float64(FILL), DTYPE, SHAPE, FILL, None)
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_non_nan_fill_value(self, adapter, tmp_path):
        path = tmp_path / "zeros.hdf5"
        adapter.create(path, SHAPE, {"id": "zeros"})
        adapter.update_data(path, np.index_exp[0:10], np.arange(1, 11, dtype=DTYPE), DTYPE, SHAPE, 0.0, None)
        assert empty_cells(path) == 90
        adapter.update_data(path, np.index_exp[0:10], np.zeros(10, dtype=DTYPE), DTYPE, SHAPE, 0.0, None)
        assert empty_cells(path) == TOTAL
        assert not has_data(path)


class TestClearData:
    def test_clearing_an_empty_region_twice_keeps_the_data(self, adapter, array_file):
        """Regression: the counter used to grow blindly and the second clear deleted the live cells 25:50."""
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        assert empty_cells(array_file) == 75
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        assert empty_cells(array_file) == 75
        assert has_data(array_file)
        assert real_values(array_file) == 25

    def test_clearing_everything_piece_by_piece_deletes_dataset(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        adapter.clear_data(array_file, SHAPE, np.index_exp[25:50], FILL)
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_clearing_the_whole_array_deletes_dataset(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        adapter.clear_data(array_file, SHAPE, np.index_exp[:], FILL)
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_clearing_an_array_without_data_is_a_no_op(self, adapter, array_file):
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)


class TestDriftedCounter:
    """Files written by older versions may carry a wrong counter; the deletion must never trust it blindly."""

    @staticmethod
    def set_counter(path: Path, value: int) -> None:
        with h5py.File(path, "r+", locking=False) as f:
            f["empty_cells"][()] = value

    def test_overcounted_counter_does_not_delete_live_data_on_clear(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        self.set_counter(array_file, 90)  # true value is 50
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        assert has_data(array_file)
        assert real_values(array_file) == 25
        assert empty_cells(array_file) == 75  # corrected by the scan

    def test_overcounted_counter_does_not_delete_live_data_on_update(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        self.set_counter(array_file, 95)  # true value is 50
        write(adapter, array_file, np.index_exp[0:10], np.full(10, FILL))
        assert has_data(array_file)
        assert real_values(array_file) == 40
        assert empty_cells(array_file) == 60  # corrected by the scan

    def test_undercounted_counter_stays_safe(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        self.set_counter(array_file, 0)  # true value is 50
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:50], FILL)
        assert has_data(array_file)  # the counter says 50, the scan is not even reached
        assert empty_cells(array_file) == 50
        assert real_values(array_file) == 0


class TestStorageAdapterBrokenFiles:
    @pytest.fixture
    def file_without_meta(self, tmp_path: Path) -> Path:
        path = tmp_path / "broken.hdf5"
        HDF5StorageAdapter().create(path, (10,), {"id": "broken"})
        with h5py.File(path, "r+", locking=False) as f:
            del f["meta"]
        return path

    def test_read_meta_raises_without_meta(self, file_without_meta):
        with pytest.raises(DekerArrayError, match="No metadata"):
            HDF5StorageAdapter().read_meta(file_without_meta)

    def test_update_meta_raises_without_meta(self, file_without_meta):
        with pytest.raises(DekerArrayError, match="No metadata"):
            HDF5StorageAdapter().update_meta_custom_attributes(file_without_meta, {"a": 1})


if __name__ == "__main__":
    pytest.main()


class TestEmptyArrayRead:
    """``read_data`` on an array without a ``data`` dataset allocates only the requested region."""

    def test_slice_returns_fill_values_of_subset_shape(self, adapter, array_file):
        data = adapter.read_data(array_file, SHAPE, np.index_exp[10:30], FILL, DTYPE)
        assert data.shape == (20,)
        assert data.dtype == DTYPE
        assert np.all(np.isnan(data))

    def test_full_read_returns_array_shape(self, adapter, array_file):
        data = adapter.read_data(array_file, SHAPE, np.index_exp[...], FILL, DTYPE)
        assert data.shape == SHAPE

    def test_integer_index_returns_scalar_like_a_stored_dataset(self, adapter, array_file):
        empty = adapter.read_data(array_file, SHAPE, np.index_exp[5], FILL, DTYPE)
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        stored = adapter.read_data(array_file, SHAPE, np.index_exp[5], FILL, DTYPE)
        assert type(empty) is type(stored)
        assert np.ndim(empty) == 0
        assert np.isnan(empty)

    def test_non_nan_fill_value(self, adapter, tmp_path):
        path = tmp_path / "ints.hdf5"
        adapter.create(path, (4, 4), {"id": "ints"})
        data = adapter.read_data(path, (4, 4), np.index_exp[1:3, :], -1, np.int32)
        assert data.shape == (2, 4)
        assert data.dtype == np.int32
        assert np.all(data == -1)


class TestDatasetCreation:
    """The first write creates the dataset with a fill value instead of a full-size in-memory array."""

    def test_dataset_carries_fill_value(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:10], np.arange(10))
        with h5py.File(array_file, "r", locking=False) as f:
            assert np.isnan(f["data"].fillvalue)

    def test_unwritten_region_reads_as_fill_value(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[10:20], np.arange(10))
        data = adapter.read_data(array_file, SHAPE, np.index_exp[...], FILL, DTYPE)
        assert np.array_equal(data[10:20], np.arange(10))
        assert np.all(np.isnan(data[:10]))
        assert np.all(np.isnan(data[20:]))

    def test_non_nan_fill_value_is_stored_in_dataset(self, adapter, tmp_path):
        path = tmp_path / "ints.hdf5"
        shape = (10, 10)
        adapter.create(path, shape, {"id": "ints"})
        adapter.update_data(path, np.index_exp[0, 0:5], np.arange(5, dtype=np.int32), np.int32, shape, -1, None)
        with h5py.File(path, "r", locking=False) as f:
            assert f["data"].fillvalue == -1
            stored = f["data"][()]
        assert np.array_equal(stored[0, 0:5], np.arange(5))
        assert np.count_nonzero(stored == -1) == 95

    def test_chunked_dataset_materialises_only_written_chunks(self, adapter, tmp_path):
        path = tmp_path / "chunked.hdf5"
        shape = (1000, 1000)
        adapter.create(path, shape, {"id": "chunked"})
        options = HDF5Options(
            chunks=(100, 100),
            compression_opts=HDF5CompressionOpts(compression="gzip", compression_opts=1),
        )
        adapter.update_data(path, np.index_exp[0:100, 0:100], np.ones((100, 100)), DTYPE, shape, FILL, options)
        with h5py.File(path, "r", locking=False) as f:
            ds = f["data"]
            assert ds.chunks == (100, 100)
            assert ds.compression == "gzip"
            # one chunk out of a hundred is written: storage is a fraction of the full array
            assert ds.id.get_storage_size() < ds.nbytes / 10
            assert np.all(ds[0:100, 0:100] == 1)
            assert np.all(np.isnan(ds[100:200, 100:200]))

    def test_scalar_data_is_broadcast_on_creation(self, adapter, array_file):
        adapter.update_data(array_file, np.index_exp[0:10], np.asarray(7.0), DTYPE, SHAPE, FILL, None)
        assert real_values(array_file) == 10
        assert empty_cells(array_file) == 90


class TestMetaDataset:
    """The ``meta`` dataset is sized exactly to its UTF-8 payload and survives non-ASCII content."""

    @staticmethod
    def meta_bytes(path: Path) -> tuple[bytes, int]:
        with h5py.File(path, "r", locking=False) as f:
            ds = f["meta"]
            return ds[()], ds.dtype.itemsize

    def test_created_meta_is_sized_to_payload(self, adapter, tmp_path):
        path = tmp_path / "meta.hdf5"
        meta = {"id": "test", "custom_attributes": {"name": "температура"}}
        adapter.create(path, SHAPE, meta)
        stored, itemsize = self.meta_bytes(path)
        assert stored == json.dumps(meta, default=str).encode("utf-8")
        assert itemsize == len(stored)
        assert adapter.read_meta(path) == meta

    @pytest.mark.parametrize("metadata", ['{"id": "raw"}', b'{"id": "raw"}'])
    def test_create_accepts_prepared_metadata(self, adapter, tmp_path, metadata):
        path = tmp_path / "meta.hdf5"
        adapter.create(path, SHAPE, metadata)
        assert adapter.read_meta(path) == {"id": "raw"}

    def test_updated_meta_is_sized_to_payload(self, adapter, array_file):
        attributes = {"station": "Москва", "height": 10}
        assert adapter.update_meta_custom_attributes(array_file, attributes) == attributes
        stored, itemsize = self.meta_bytes(array_file)
        assert itemsize == len(stored)
        assert adapter.read_meta(array_file)["custom_attributes"] == attributes
