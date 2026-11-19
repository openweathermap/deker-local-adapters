# deker-local-adapters - local filesystem HDF5 storage for deker
# Copyright (C) 2023-2026  Openweather Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import json
import os

from pathlib import Path

import h5py
import hdf5plugin
import numpy as np

from deker.ABC.base_adapters import BaseStorageAdapter
from deker.errors import DekerArrayError
from deker.log import SelfLoggerMixin
from deker.tools import calculate_total_cells_in_array
from deker.types import ArrayMeta, Numeric, Slice
from deker_tools.slices import create_shape_from_slice, positive_bounds
from h5py import Dataset
from numpy import ndarray

from deker_local_adapters.storage_adapters.hdf5.hdf5_options import HDF5Options

os.environ["HDF5_PLUGIN_PATH"] = hdf5plugin.PLUGIN_PATH
os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"


def flip_axes(data: ndarray | Numeric, axes: tuple[int, ...]) -> ndarray | Numeric:
    """Flip the data along the axes, if there are any; a scalar is returned as is.

    :param data: data to flip
    :param axes: axes to flip along
    :returns: flipped view of the data, or the data itself
    """
    if not axes or np.ndim(data) == 0:
        return data
    return np.flip(data, axes)


class HDF5StorageAdapter(SelfLoggerMixin, BaseStorageAdapter[HDF5Options]):
    """Local HDF5 storage adapter.

    Wraps `h5py` files managing logic. A file holds three datasets: ``meta`` with the JSON metadata of the
    array, ``empty_cells`` with the number of cells never written or cleared, and ``data``, which appears with
    the first write and disappears when every cell is empty again. A file is opened without the HDF5 locking:
    the locks are the ``flock`` ones of Deker.
    """

    file_ext: str = ".hdf5"
    storage_options = HDF5Options

    @staticmethod
    def _count_fill_values(values: ndarray | Numeric, fill_value: Numeric) -> int:
        """Count the cells equal to ``fill_value`` (``NaN`` is compared as ``NaN``).

        :param values: array or a scalar to count in
        :param fill_value: array fill_value
        :returns: number of cells holding the fill_value
        """
        array = np.asarray(values)
        if np.isnan(fill_value):
            return int(np.count_nonzero(np.isnan(array)))
        return int(np.count_nonzero(array == fill_value))

    @staticmethod
    def _covers_whole_array(array_shape: tuple[int, ...], subset_shape: tuple[int, ...]) -> bool:
        """Check that a region selects every cell of the array.

        The shapes are compared by the number of cells, not by the dimensions: an integer index drops a
        dimension of size 1 and still selects every cell, while a stride over a whole axis does not.

        :param array_shape: shape of the array
        :param subset_shape: shape of the region selected by the index expression
        :returns: ``True`` if the region is the whole array
        """
        return calculate_total_cells_in_array(subset_shape) == calculate_total_cells_in_array(array_shape)

    @staticmethod
    def _create_meta_dataset(f: h5py.File, meta: str | bytes) -> Dataset:
        """Store ``meta`` as a scalar fixed-length bytes dataset sized exactly to its UTF-8 payload.

        :param f: open hdf5 file
        :param meta: JSON metadata
        :returns: the created ``meta`` dataset
        """
        encoded = meta if isinstance(meta, bytes) else meta.encode("utf-8")
        ds = f.create_dataset("meta", data=encoded, dtype=f"S{len(encoded)}", shape=())
        ds.flush()
        return ds

    @classmethod
    def _delete_data_if_empty(cls, f: h5py.File, ds: Dataset, fill_value: Numeric, total_cells: int) -> int:
        """Delete the ``data`` dataset if every cell holds the fill_value; return the true number of empty cells.

        The ``empty_cells`` counter is the only hint that the array may be empty, and it may have drifted in files
        written by older versions, so the dataset is scanned before the irreversible deletion. If it is not empty,
        the counter is corrected instead.

        :param f: opened hdf5 file
        :param ds: the ``data`` dataset
        :param fill_value: array fill_value
        :param total_cells: number of cells in the array
        :returns: number of empty cells in the array after the operation
        """
        empty_cells = cls._count_fill_values(ds[()], fill_value)
        if empty_cells >= total_cells:
            del f["data"]
            return total_cells
        return empty_cells

    def create(self, path: Path, array_shape: tuple[int, ...], metadata: str | bytes | dict) -> None:
        """Create new hdf5 file with metadata.

        The file is created in one step: an existing file is refused, never reopened or truncated.

        :param path: path to hdf5 file
        :param array_shape: shape of the array
        :param metadata: array metadata
        :raises FileExistsError: if the file already exists
        """
        try:
            self.logger.debug("trying to create %s", path)
            # free space released by dataset recreation (``meta`` updates) is tracked across file opens
            # and reused instead of growing the file; readable by HDF5 >= 1.10.1 (any h5py 3.x)
            with h5py.File(path, "w-", locking=False, fs_strategy="fsm", fs_persist=True) as f:
                self.logger.debug("%s opened in 'w-'-mode", path)
                if not isinstance(metadata, (str, bytes)):
                    metadata = json.dumps(metadata, default=str)
                self._create_meta_dataset(f, metadata)
                empty_cells = f.create_dataset(
                    "empty_cells", data=calculate_total_cells_in_array(array_shape), shape=()
                )
                empty_cells.flush()
                f.flush()
            self.logger.debug("%s created and closed", path)
        except FileExistsError:
            self.logger.debug("%s already exists", path)
            raise
        except Exception as e:
            self.logger.exception(e)
            raise e

    def read_data(
        self,
        path: Path,
        array_shape: tuple[int, ...],
        bounds: Slice,
        fill_value: Numeric,
        dtype: type[Numeric],
    ) -> ndarray | Numeric:
        """Read array data from hdf5 file.

        :param path: path to hdf5 file
        :param array_shape: shape of the array
        :param bounds: array slice
        :param fill_value: Value to fill empty array
        :param dtype: array dtype
        :returns: data within the bounds, filled with ``fill_value`` where the array is empty;
            a scalar when the bounds select a single cell
        """
        self.logger.debug("trying to read data from %s", path)
        with h5py.File(path, mode="r", locking=False) as f:
            self.logger.debug("%s opened in 'r'-mode", path)
            self.logger.debug("trying to read data from %s", path)
            ds: Dataset | None = f.get("data")
            if ds is None:
                # no data stored yet: nothing to read, only the requested region is allocated
                data = np.full(create_shape_from_slice(array_shape, bounds), fill_value, dtype=dtype)
                if data.ndim == 0:
                    data = data[()]  # an integer index yields a scalar, as it does for a stored dataset
            else:
                forward, flipped = positive_bounds(array_shape, bounds)
                data = flip_axes(ds[forward], flipped)
        self.logger.debug("%s data read OK and closed", path)
        return data

    def read_empty_cells(self, path: Path) -> int:
        """Read the stored counter of empty cells of an array.

        :param path: path to hdf5 file
        :returns: the ``empty_cells`` dataset as stored
        :raises DekerArrayError: if the file has no counter
        """
        with h5py.File(path, mode="r", locking=False) as f:
            if "empty_cells" not in f:
                raise DekerArrayError(f"No empty cells counter in {path}. Try to delete and recreate the array.")
            return int(f["empty_cells"][()])

    def count_empty_cells(self, path: Path, array_shape: tuple[int, ...], fill_value: Numeric) -> int:
        """Count the empty cells of an array in its data, whatever the stored counter says.

        A file without the ``data`` dataset is empty whole.

        :param path: path to hdf5 file
        :param array_shape: array shape
        :param fill_value: array fill_value
        :returns: number of cells holding the fill value
        """
        with h5py.File(path, mode="r", locking=False) as f:
            if "data" not in f:
                return calculate_total_cells_in_array(array_shape)
            return self._count_fill_values(f["data"][()], fill_value)

    def write_empty_cells(self, path: Path, empty_cells: int) -> None:
        """Replace the stored counter of empty cells of an array.

        :param path: path to hdf5 file
        :param empty_cells: the new value of the counter
        """
        with h5py.File(path, mode="r+", locking=False) as f:
            f["empty_cells"][()] = empty_cells
            f.flush()

    def read_meta(self, path: Path) -> ArrayMeta:
        """Read array metadata from hdf5 file.

        :param path: path to hdf5 file
        :returns: array metadata decoded from the ``meta`` dataset
        :raises DekerArrayError: if the file has no ``meta`` dataset
        """
        self.logger.debug("trying to read meta from %s", path)
        with h5py.File(path, mode="r", locking=False) as f:
            self.logger.debug("%s opened in 'r'-mode", path)
            ds: Dataset = f.get("meta")
            if not ds:
                raise DekerArrayError("No metadata in the array. Try to delete and recreate the array.")
            data = ds[()]
        decoded = data.decode("utf-8")
        meta = json.loads(decoded)
        self.logger.debug("%s meta read OK and closed", path)
        return meta

    def update_data(
        self,
        path: Path,
        bounds: Slice,
        data: ndarray | Numeric,
        dtype: type[Numeric],
        shape: tuple[int, ...],
        fill_value: Numeric,
        collection_options: HDF5Options | None,
    ) -> None:
        """Update array data in hdf5 file and keep the counter of empty cells true.

        The ``data`` dataset is created with the declared fill value at the first write, so only the written
        region is stored; it is removed when the write leaves every cell empty. The counter moves by the
        difference between the empty cells of the region before and after the write; a write that covers the
        whole array does not read the old content.

        :param path: path to hdf5 file
        :param bounds: array slice
        :param data: new data for array slice, of the shape of the bounds or a number
        :param dtype: array dtype
        :param shape: array shape
        :param fill_value: array fill_value
        :param collection_options: chunking and compression options
        """
        try:
            self.logger.debug("trying to update data in %s", path)
            with h5py.File(path, mode="r+", locking=False) as f:
                self.logger.debug("%s opened in 'r+'-mode", path)
                empty_cells_ds: Dataset = f["empty_cells"]
                total_cells = calculate_total_cells_in_array(shape)
                subset_shape = create_shape_from_slice(shape, bounds)
                subset_cells = calculate_total_cells_in_array(subset_shape)
                ds: Dataset | None = f.get("data")
                fill_values_after = self._count_fill_values(np.broadcast_to(np.asarray(data), subset_shape), fill_value)
                # HDF5 selects forwards only: a negative step becomes its forward twin and the data is flipped to match
                forward, flipped = positive_bounds(shape, bounds)
                data = flip_axes(data, flipped)

                if self._covers_whole_array(shape, subset_shape):
                    # every cell is replaced: the old content and the old counter play no part in the result
                    empty_cells = fill_values_after
                    if empty_cells >= total_cells and ds is not None:
                        # nothing but fill values is written: the dataset goes away without being written or scanned
                        del f["data"]
                        ds = None
                else:
                    # the counter moves by the difference between the empty cells in the region before and after
                    fill_values_before = (
                        subset_cells if ds is None else self._count_fill_values(ds[forward], fill_value)
                    )
                    empty_cells = int(empty_cells_ds[()]) - fill_values_before + fill_values_after

                if ds is None:
                    if empty_cells >= total_cells:
                        # writing fill values into an array that has no data yet: nothing to store
                        empty_cells = total_cells
                    else:
                        # the dataset is created without data: HDF5 serves unwritten regions from ``fillvalue``,
                        # so only the written region is materialised and no full-size array is built in memory
                        ds_kwargs: dict = {"shape": shape, "dtype": dtype, "fillvalue": fill_value}

                        if collection_options:
                            compression = collection_options.compression_opts.compression
                            options = collection_options.compression_opts.compression_opts
                            ds_kwargs.update(
                                {
                                    "chunks": collection_options.chunks,
                                    "compression": compression,
                                    "compression_opts": options,
                                }
                            )

                        ds = f.create_dataset("data", **ds_kwargs)
                        ds[forward] = data
                        ds.flush()
                else:
                    ds[forward] = data
                    ds.flush()
                    if empty_cells >= total_cells:
                        empty_cells = self._delete_data_if_empty(f, ds, fill_value, total_cells)

                empty_cells_ds[()] = empty_cells
                empty_cells_ds.flush()
                f.flush()
            self.logger.debug("%s data updated OK and closed", path)
        except Exception as e:
            self.logger.exception(e)
            raise e

    def update_meta_custom_attributes(self, path: Path, attributes: dict) -> dict:
        """Replace the custom attributes in the ``meta`` dataset of the file; the dataset is recreated.

        :param path: path to hdf5 file
        :param attributes: new custom attributes
        :returns: the stored custom attributes
        :raises DekerArrayError: if the file has no ``meta`` dataset
        """
        try:
            self.logger.debug("trying to update meta in %s", path)
            with h5py.File(path, "r+", locking=False) as f:
                self.logger.debug("%s opened in 'r+'-mode", path)
                ds = f.get("meta")
                if not ds:
                    raise DekerArrayError("No metadata in the array. Try to delete and recreate the array.")
                meta = json.loads(ds[()])
                del f["meta"]
                f.flush()

                meta["custom_attributes"] = attributes
                self._create_meta_dataset(f, json.dumps(meta, default=str))
                f.flush()
            self.logger.debug("%s meta updated OK and closed", path)
            return attributes
        except Exception as e:
            self.logger.exception(e)
            raise e

    def clear_data(self, path: Path, array_shape: tuple[int, ...], bounds: Slice, fill_value: Numeric) -> None:
        """Set the cells within the bounds to the fill value and keep the counter of empty cells true.

        The ``data`` dataset is removed when every cell is empty afterwards; a file without data is left as it is.

        :param path: path to hdf5 file
        :param array_shape: array shape
        :param bounds: array bounds to clear
        :param fill_value: array fill_value
        """
        try:
            self.logger.debug("trying to clear data in %s", path)
            with h5py.File(path, "r+", locking=False) as f:
                self.logger.debug("%s opened in 'r+'-mode", path)
                ds: Dataset | None = f.get("data")
                if ds is not None:
                    empty_cells_ds: Dataset = f["empty_cells"]
                    total_cells = calculate_total_cells_in_array(array_shape)
                    subset_shape = create_shape_from_slice(array_shape, bounds)

                    if self._covers_whole_array(array_shape, subset_shape):
                        # the whole array is cleared
                        del f["data"]
                        empty_cells = total_cells
                    else:
                        # only the cells that held data become empty; the ones already empty do not count twice
                        subset_cells = calculate_total_cells_in_array(subset_shape)
                        forward, _ = positive_bounds(array_shape, bounds)  # the fill value needs no flipping
                        fill_values_before = self._count_fill_values(ds[forward], fill_value)
                        empty_cells = int(empty_cells_ds[()]) + subset_cells - fill_values_before
                        ds[forward] = fill_value
                        ds.flush()
                        if empty_cells >= total_cells:
                            empty_cells = self._delete_data_if_empty(f, ds, fill_value, total_cells)

                    empty_cells_ds[()] = empty_cells
                    empty_cells_ds.flush()
                    f.flush()
            self.logger.debug("%s data cleared OK and closed", path)
        except Exception as e:
            self.logger.exception(e)
            raise e
