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

import os

from collections.abc import Generator
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from cantok import AbstractToken
from deker.ABC.base_adapters import BaseArrayAdapter
from deker.arrays import Array
from deker.ctx import CTX
from deker.errors import DekerArrayError, DekerInstanceNotExistsError
from deker.locks import ReadArrayLock, WriteArrayLock
from deker.schemas import AttributeSchema
from deker.tools import get_main_path, get_symlink_path
from deker.tools.decorators import check_ctx_state
from deker.types import ArrayMeta, Data, Numeric, Slice
from deker_tools.cancel import check, mark_started
from deker_tools.typings import SentinelType
from numpy import ndarray

from deker_local_adapters.mixin import LocalAdapterMixin

if TYPE_CHECKING:
    from concurrent.futures import ThreadPoolExecutor

    from deker.ABC import BaseStorageAdapter
    from deker.ABC.base_collection import BaseCollectionOptions
    from deker.collection import Collection


class LocalArrayAdapter(LocalAdapterMixin[Array], BaseArrayAdapter[Path]):
    """Adapter of the ``Arrays`` of a collection on the local disk.

    An ``Array`` is a main file in the data directory, named by its id, and a symlink to it in the symlinks
    directory, placed by its primary attributes. The reads hold a shared lock on the main file, the writes,
    the clears, the attribute updates and the deletion an exclusive one.
    """

    def __init__(
        self,
        collection_path: Path,
        ctx: "CTX",
        storage_adapter: type["BaseStorageAdapter"],
        executor: "ThreadPoolExecutor",
        collection_options: Optional["BaseCollectionOptions"] = None,
    ) -> None:
        """LocalArrayAdapter constructor.

        :param collection_path: directory of the collection
        :param ctx: context of the client
        :param storage_adapter: type of the adapter of the files
        :param executor: thread pool the Arrays of a VArray are processed in
        :param collection_options: chunking and compression options of the collection
        """
        self.collection_options = collection_options
        self.data_dir = ctx.config.array_data_directory
        self.symlinks_dir = ctx.config.array_symlinks_directory
        self.file_ext = storage_adapter.file_ext
        self.collection_path = collection_path
        self.dirs = (self.data_dir, self.symlinks_dir)
        super().__init__(collection_path, ctx, executor, storage_adapter)

    def _get_symlink_filename(
        self,
        vid: str,
        vpos: tuple[int, ...],
        array_primary_attributes_schema: tuple[AttributeSchema, ...],
    ) -> Path | None:
        """Find the symlink of the Array of a VArray at a position.

        :param vid: id of the VArray
        :param vpos: ``v_position`` of the Array in the ``vgrid``
        :param array_primary_attributes_schema: schemas of the primary attributes of the Arrays
        :returns: the symlink, ``None`` if there is no Array at the position
        """
        sympath_to_dir = get_symlink_path(
            self.collection_path / self.symlinks_dir,
            array_primary_attributes_schema,
            {"vid": vid, "v_position": vpos},
        )
        try:
            files = [file for file in sympath_to_dir.iterdir() if file.is_file() and file.name.endswith(self.file_ext)]
            if files:
                # just one file is expected inside the list
                return files[0]
        except FileNotFoundError:
            return None

    @check_ctx_state
    def create(self, array: Array, *, timeout: int | SentinelType, token: AbstractToken) -> Array:
        """Create a new array on disk storage.

        :param array: Array instance
        :param timeout: seconds to wait for the symlink directory of the primary attributes, as the call gave them;
          ``Sentinel`` -
          ``write_lock_timeout`` of the client
        :param token: token of the write operation
        :returns: the same instance, once its files are created
        :raises DekerArrayError: if the array file already exists or its VArray does not exist
        :raises DekerValidationError: if an array with the same primary attributes exists
        """
        check(token)
        # If the array is part of a varray, check that such varray exists
        if array._vid:
            varray_path = get_main_path(array._vid, Path(self.ctx.config.varray_data_directory))
            if not (self.collection_path / varray_path).exists():
                raise DekerArrayError(f"VArray with id={array._vid} doesn't exist")
        return self._create_on_disk(array, timeout=timeout, token=token)

    def _create_main_file(self, array: Array, main_filename: Path) -> None:
        """Create the hdf5 file of an Array with its metadata.

        :param array: Array instance
        :param main_filename: path to the hdf5 file
        :raises FileExistsError: if the file already exists
        """
        self.storage_adapter.create(main_filename, array.shape, array._create_meta())

    @check_ctx_state
    @ReadArrayLock()
    def read_data(
        self, array: Array, bounds: Slice, *, timeout: int | SentinelType, token: AbstractToken
    ) -> Numeric | ndarray:
        """Read data from the existing array, holding its file shared.

        The data read are not returned if the read was cancelled while the file was read.

        :param array: Array instance
        :param bounds: array bounds to read
        :param timeout: seconds of the read as the call gave them
        :param token: token of the read operation
        :returns: data within the bounds; a scalar for a single cell
        :raises DekerLockError: if a writer holds the file
        :raises DekerInstanceNotExistsError: if the file is gone
        """
        check(token)
        filename = self._get_main_path_to_file(array)
        try:
            data = self.storage_adapter.read_data(filename, array.shape, bounds, array.fill_value, array.dtype)
        except FileNotFoundError:
            raise DekerInstanceNotExistsError(f"{filename} doesn't exist") from None
        check(token)
        return data

    @check_ctx_state
    @ReadArrayLock()
    def read_meta(self, array: Array | Path) -> ArrayMeta:
        """Read array metadata, holding its file shared.

        :param array: Array instance or the path to its main file
        :returns: array metadata
        :raises DekerLockError: if a writer holds the file
        :raises DekerInstanceNotExistsError: if the file is gone
        """
        filename = array if isinstance(array, Path) else self._get_main_path_to_file(array)

        try:
            result = self.storage_adapter.read_meta(filename)
        except FileNotFoundError:
            raise DekerInstanceNotExistsError(f"{filename} doesn't exist") from None
        return result

    @WriteArrayLock()
    def _delete_held(self, array: Array, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Delete the array holding its file exclusively.

        :param array: Array instance
        :param timeout: seconds to wait for the lock of the file as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client
        :param token: token of the write operation
        """
        super().delete(array, timeout=timeout, token=token)

    @check_ctx_state
    def delete(self, array: Array, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Delete the array, waiting for the readers and writers of its file to leave.

        :param array: Array instance
        :param timeout: seconds to wait for the lock of the file as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client
        :param token: token of the write operation
        :raises DekerLockError: if the file is still held when the timeout is over
        """
        check(token)
        if self._get_main_path_to_file(array).exists():
            self._delete_held(array, timeout=timeout, token=token)
        else:
            # the main file is gone already: the symlink is removed anyway
            super().delete(array, timeout=timeout, token=token)

    @check_ctx_state
    @WriteArrayLock()
    def update(
        self, array: Array, bounds: Slice, data: Data, *, timeout: int | SentinelType, token: AbstractToken
    ) -> None:
        """Update data in the existing array, holding its file exclusively.

        :param array: Array instance
        :param bounds: array bounds to be updated
        :param data: new data of the shape of the bounds, or a number that fills them
        :param timeout: seconds to wait for the lock of the file as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client
        :param token: token of the write operation
        :raises DekerArrayError: if the data is ``None``
        :raises DekerArrayTypeError: if the data does not fit the dtype of the array
        :raises IndexError: if the data does not fit the shape of the bounds
        :raises DekerLockError: if the file is not released within the write lock timeout
        :raises DekerInstanceNotExistsError: if the file is gone
        """
        check(token)
        filename = self._get_main_path_to_file(array)

        if data is None:
            raise DekerArrayError("Updating data shall not be None")
        data = self._process_data(array.dtype, array.shape, data, bounds)
        mark_started(token)
        try:
            self.storage_adapter.update_data(
                filename,
                bounds,
                data,
                array.dtype,
                array.shape,
                array.fill_value,
                self.collection_options,
            )
        except FileNotFoundError:
            raise DekerInstanceNotExistsError(f"{filename} doesn't exist") from None

    @check_ctx_state
    @WriteArrayLock()
    def update_meta_custom_attributes(
        self, array: Array, attributes: dict, *, timeout: int | SentinelType, token: AbstractToken
    ) -> None:
        """Replace the custom attributes in the file of the array, holding it exclusively, and in the object.

        :param array: Array instance
        :param attributes: new custom attributes
        :param timeout: seconds to wait for the lock of the file as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client
        :param token: token of the write operation
        :raises DekerLockError: if the file is not released within the write lock timeout
        :raises DekerInstanceNotExistsError: if the file is gone
        """
        check(token)
        filename = self._get_main_path_to_file(array)

        mark_started(token)
        try:
            array.custom_attributes = self.storage_adapter.update_meta_custom_attributes(
                filename,
                attributes,
            )
        except FileNotFoundError:
            raise DekerInstanceNotExistsError(f"{filename} doesn't exist") from None
        except Exception as e:
            self.logger.exception(e)
            raise e

    @check_ctx_state
    @WriteArrayLock()
    def clear(self, array: Array, bounds: Slice, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Set the cells within the bounds to the fill value, holding the file exclusively.

        :param array: Array instance
        :param bounds: array bounds to clear
        :param timeout: seconds to wait for the lock of the file as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client
        :param token: token of the write operation
        :raises DekerLockError: if the file is not released within the write lock timeout
        :raises DekerInstanceNotExistsError: if the file is gone
        """
        check(token)
        filename = self._get_main_path_to_file(array)
        mark_started(token)
        try:
            self.storage_adapter.clear_data(
                filename,
                array.shape,
                bounds,
                array.fill_value,
            )
        except FileNotFoundError:
            raise DekerInstanceNotExistsError(f"{filename} doesn't exist") from None

    @check_ctx_state
    def _adapter_iter(self, vid: str) -> Generator[Path, None, None]:
        """Iterate over all the Arrays symlinks in VArray.

        :param vid: Array vid attribute value
        :yields: paths to the Arrays symlinks
        """
        root_path = self.collection_path / self.symlinks_dir / vid
        for root, _, files in os.walk(root_path):
            for file in files:
                if file.endswith(self.file_ext):
                    symlink = Path(os.path.join(root, file))
                    yield symlink

    @check_ctx_state
    def delete_all_by_vid(
        self, vid: str, collection: "Collection", *, timeout: int | SentinelType, token: AbstractToken
    ) -> None:
        """Delete all the Arrays of a VArray, each waiting for its readers and writers.

        Once one Array is started, every Array is deleted whatever happens to the token: the deletion of a VArray
        is cancelled only before any of its files is touched.

        :param vid: VArray id
        :param collection: collection instance
        :param timeout: seconds to wait for the lock of the file as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client
        :param token: token of the write operation
        """
        check(token)
        try:
            metas = self.executor.map(self.read_meta, self._adapter_iter(vid))
            arrays = [Array._create_from_meta(collection, meta, self) for meta in metas]
            list(self.executor.map(partial(self.delete, timeout=timeout, token=token), arrays))
        except Exception as e:
            self.logger.exception(e)
            raise e
