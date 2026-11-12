"""Direct tests of the HDF5 storage adapter: the ``empty_cells`` counter and the ``data`` dataset life cycle."""

from pathlib import Path

import h5py
import numpy as np
import pytest

from deker.errors import DekerArrayError

from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter


SHAPE = (100,)
DTYPE = np.float64
FILL = np.nan
TOTAL = 100


@pytest.fixture()
def adapter() -> HDF5StorageAdapter:
    return HDF5StorageAdapter()


@pytest.fixture()
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
    @pytest.fixture()
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
