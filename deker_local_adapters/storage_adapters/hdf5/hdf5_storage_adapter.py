# deker-local-adapters - local filesystem HDF5 storage for deker
# Copyright (C) 2023  OpenWeather
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
from deker_tools.slices import create_shape_from_slice
from h5py import Dataset
from numpy import ndarray

from deker_local_adapters.storage_adapters.hdf5.hdf5_options import HDF5Options

os.environ["HDF5_PLUGIN_PATH"] = hdf5plugin.PLUGIN_PATH
os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"


class HDF5StorageAdapter(SelfLoggerMixin, BaseStorageAdapter):
    """Local HDF5 storage adapter.

    Wraps `h5py` files managing logic.
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

        :param path: path to hdf5 file
        :param array_shape: shape of the array
        :param metadata: array metadata
        """
        try:
            self.logger.debug(f"trying to create {path}")
            # free space released by dataset recreation (``meta`` updates) is tracked across file opens
            # and reused instead of growing the file; readable by HDF5 >= 1.10.1 (any h5py 3.x)
            with h5py.File(path, "w", locking=False, fs_strategy="fsm", fs_persist=True) as f:
                self.logger.debug(f"{path} opened in 'w'-mode")
                if not isinstance(metadata, (str, bytes)):
                    metadata = json.dumps(metadata, default=str)
                self._create_meta_dataset(f, metadata)
                empty_cells = f.create_dataset(
                    "empty_cells", data=calculate_total_cells_in_array(array_shape), shape=()
                )
                empty_cells.flush()
                f.flush()
            self.logger.debug(f"{path} created and closed")
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
    ) -> ndarray:
        """Read array data from hdf5 file.

        :param path: path to hdf5 file
        :param array_shape: shape of the array
        :param bounds: array slice
        :param fill_value: Value to fill empty array
        :param dtype: array dtype
        :returns: data within the bounds, filled with ``fill_value`` where the array is empty
        """
        self.logger.debug(f"trying to read data from {path}")
        with h5py.File(path, mode="r", locking=False) as f:
            self.logger.debug(f"{path} opened in 'r'-mode")
            self.logger.debug(f"trying to read data from {path}")
            ds: Dataset | None = f.get("data")
            if ds is None:
                # no data stored yet: nothing to read, only the requested region is allocated
                data = np.full(create_shape_from_slice(array_shape, bounds), fill_value, dtype=dtype)
                if data.ndim == 0:
                    data = data[()]  # an integer index yields a scalar, as it does for a stored dataset
            else:
                data = ds[bounds]
        self.logger.debug(f"{path} data read OK and closed")
        return data

    def read_meta(self, path: Path) -> ArrayMeta:
        """Read array metadata from hdf5 file.

        :param path: path to hdf5 file
        :returns: array metadata decoded from the ``meta`` dataset
        """
        self.logger.debug(f"trying to read meta from {path}")
        with h5py.File(path, mode="r", locking=False) as f:
            self.logger.debug(f"{path} opened in 'r'-mode")
            ds: Dataset = f.get("meta")
            if not ds:
                raise DekerArrayError("No metadata in the array. Try to delete and recreate the array.")
            data = ds[()]
        decoded = data.decode("utf-8")
        meta = json.loads(decoded)
        self.logger.debug(f"{path} meta read OK and closed")
        return meta

    def update_data(
        self,
        path: Path,
        bounds: Slice,
        data: ndarray,
        dtype: type[Numeric],
        shape: tuple,
        fill_value: Numeric,
        collection_options: HDF5Options | None,
    ) -> None:
        """Update array data in hdf5 file.

        :param path: path to hdf5 file
        :param bounds: array slice
        :param data: new data for array slice
        :param dtype: array dtype
        :param shape: array shape
        :param fill_value: array fill_value
        :param collection_options: chunking and compression options
        """
        try:
            self.logger.debug(f"trying to update data in {path}")
            with h5py.File(path, mode="r+", locking=False) as f:
                self.logger.debug(f"{path} opened in 'r+'-mode")
                empty_cells_ds: Dataset = f["empty_cells"]
                total_cells = calculate_total_cells_in_array(shape)
                subset_shape = create_shape_from_slice(shape, bounds)
                subset_cells = calculate_total_cells_in_array(subset_shape)
                ds: Dataset | None = f.get("data")

                # the counter moves by the difference between the empty cells in the region before and after the write
                fill_values_before = subset_cells if ds is None else self._count_fill_values(ds[bounds], fill_value)
                fill_values_after = self._count_fill_values(np.broadcast_to(np.asarray(data), subset_shape), fill_value)
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
                        ds[bounds] = data
                        ds.flush()
                else:
                    ds[bounds] = data
                    ds.flush()
                    if empty_cells >= total_cells:
                        empty_cells = self._delete_data_if_empty(f, ds, fill_value, total_cells)

                empty_cells_ds[()] = empty_cells
                empty_cells_ds.flush()
                f.flush()
            self.logger.debug(f"{path} data updated OK and closed")
        except Exception as e:
            self.logger.exception(e)
            raise e

    def update_meta_custom_attributes(self, path: Path, attributes: dict) -> dict:
        """Update metadata in the existing array in hdf5 file.

        :param path: path to hdf5 file
        :param attributes: new custom attributes
        :returns: the stored custom attributes
        """
        try:
            self.logger.debug(f"trying to update meta in {path}")
            with h5py.File(path, "r+", locking=False) as f:
                self.logger.debug(f"{path} opened in 'r+'-mode")
                ds = f.get("meta")
                if not ds:
                    raise DekerArrayError("No metadata in the array. Try to delete and recreate the array.")
                meta = json.loads(ds[()])
                del f["meta"]
                f.flush()

                meta["custom_attributes"] = attributes
                self._create_meta_dataset(f, json.dumps(meta, default=str))
                f.flush()
            self.logger.debug(f"{path} meta updated OK and closed")
            return attributes
        except Exception as e:
            self.logger.exception(e)
            raise e

    def clear_data(self, path: Path, array_shape: tuple[int, ...], bounds: Slice, fill_value: Numeric) -> None:
        """Clear array data in hdf5 file.

        :param path: path to hdf5 file
        :param array_shape: array shape
        :param bounds: array bounds to update
        :param fill_value: array fill_value
        """
        try:
            self.logger.debug(f"trying to clear data in {path}")
            with h5py.File(path, "r+", locking=False) as f:
                self.logger.debug(f"{path} opened in 'r+'-mode")
                ds: Dataset | None = f.get("data")
                if ds is not None:
                    empty_cells_ds: Dataset = f["empty_cells"]
                    total_cells = calculate_total_cells_in_array(array_shape)
                    subset_shape = create_shape_from_slice(array_shape, bounds)

                    if ds.shape == subset_shape:
                        # the whole array is cleared
                        del f["data"]
                        empty_cells = total_cells
                    else:
                        # only the cells that held data become empty; the ones already empty do not count twice
                        subset_cells = calculate_total_cells_in_array(subset_shape)
                        fill_values_before = self._count_fill_values(ds[bounds], fill_value)
                        empty_cells = int(empty_cells_ds[()]) + subset_cells - fill_values_before
                        ds[bounds] = fill_value
                        ds.flush()
                        if empty_cells >= total_cells:
                            empty_cells = self._delete_data_if_empty(f, ds, fill_value, total_cells)

                    empty_cells_ds[()] = empty_cells
                    empty_cells_ds.flush()
                    f.flush()
            self.logger.debug(f"{path} data cleared OK and closed")
        except Exception as e:
            self.logger.exception(e)
            raise e
