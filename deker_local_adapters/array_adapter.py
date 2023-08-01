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

import logging
import os

from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional, Tuple, Union, Type

from deker.ABC.base_adapters import BaseArrayAdapter
from deker.arrays import Array
from deker.ctx import CTX
from deker.errors import DekerArrayError
from deker.locks import CreateArrayLock, ReadArrayLock, WriteArrayLock
from deker.log import SelfLoggerMixin
from deker.schemas import AttributeSchema
from deker.tools import create_array_from_meta, get_main_path, get_symlink_path
from deker.tools.decorators import check_ctx_state
from deker.types import ArrayMeta, Numeric, Slice
from numpy import ndarray

from deker_local_adapters.mixin import LocalAdapterMixin


if TYPE_CHECKING:
    from concurrent.futures import ThreadPoolExecutor
    from deker.ABC import BaseStorageAdapter
    from deker.ABC.base_collection import BaseCollectionOptions
    from deker.collection import Collection


class LocalArrayAdapter(SelfLoggerMixin, LocalAdapterMixin, BaseArrayAdapter):
    """Default adapter for managing Array instances."""

    def __init__(
        self,
        collection_path: Path,
        ctx: "CTX",
        storage_adapter: Type["BaseStorageAdapter"],
        executor: "ThreadPoolExecutor",
        collection_options: Optional["BaseCollectionOptions"] = None,
    ) -> None:
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
        vpos: Tuple[int, ...],
        array_primary_attributes_schema: Tuple[AttributeSchema, ...],
    ) -> Optional[Path]:
        sympath_to_dir = get_symlink_path(
            self.collection_path / self.symlinks_dir,
            array_primary_attributes_schema,
            {"vid": vid, "v_position": vpos},
        )
        try:
            files = [
                file
                for file in sympath_to_dir.iterdir()
                if file.is_file() and file.name.endswith(self.file_ext)
            ]
            if files:
                # just one file is expected inside the list
                return files[0]
        except FileNotFoundError:
            return None

    @check_ctx_state
    @CreateArrayLock()
    def create(self, array: Array) -> Array:
        """Create a new array.

        :param array: Array instance
        """
        main_filename, sym_filename = self.get_filenames_for_create_methods(array)
        self.check_array_file_existence(main_filename)

        # If the array is part of a varray, check that such varray exists
        if array._vid:
            varray_path = get_main_path(array._vid, Path(self.ctx.config.varray_data_directory))
            if not (self.collection_path / varray_path).exists():
                raise DekerArrayError(f"VArray with id={array._vid} doesn't exist")

        # Check that there is no array with the given primary attributes.
        if array.primary_attributes:
            self.check_symlink_existence(sym_filename)

        # Create meta for the array
        array_meta = array._create_meta()
        # Create the array (write to disk)
        self.storage_adapter.create(
            main_filename,
            array.shape,
            array_meta,
        )
        # Create symlink
        os.symlink(main_filename, sym_filename)
        return array

    @check_ctx_state
    @ReadArrayLock()
    def read_data(self, array: Array, bounds: Slice) -> Union[Numeric, ndarray]:
        """Read data from the existing array.

        :param array: Array instance
        :param bounds: array bounds to read
        """
        filename = self._get_main_path_to_file(array)

        return self.storage_adapter.read_data(
            filename, array.shape, bounds, array.fill_value, array.dtype
        )

    @check_ctx_state
    @ReadArrayLock()
    def read_meta(self, array: Union[Array, Path]) -> ArrayMeta:
        """Read array metadata.

        :param array: Array instance
        """
        if isinstance(array, Array):
            filename = self._get_main_path_to_file(array)
        else:
            filename = array

        result = self.storage_adapter.read_meta(filename)
        return result

    @check_ctx_state
    @WriteArrayLock()
    def update(self, array: Array, bounds: Slice, data: Any) -> None:  # type: ignore
        """Update data in the existing array.

        :param array: Array instance
        :param bounds: array bounds to be updated
        :param data: np.ndarray with array data
        """
        filename = self._get_main_path_to_file(array)

        if data is None:
            raise ValueError("Updating data shall not be None")
        data = self._process_data(array.dtype, array.shape, data, bounds)  # type: ignore
        self.storage_adapter.update_data(
            filename,
            bounds,
            data,
            array.dtype,
            array.shape,
            array.fill_value,
            self.collection_options,
        )

    @check_ctx_state
    @WriteArrayLock()
    def update_meta_custom_attributes(self, array: Array, attributes: dict) -> None:
        """Update metadata in the existing array.

        :param array: Array instance
        :param attributes: new custom attributes
        """
        filename = self._get_main_path_to_file(array)

        try:
            array.custom_attributes = self.storage_adapter.update_meta_custom_attributes(
                filename,
                attributes,
            )
        except Exception as e:
            logging.exception(e)
            raise e

    @check_ctx_state
    @WriteArrayLock()
    def clear(self, array: Array, bounds: Slice) -> None:
        """Clear data in the existing array.

        :param array: Array instance
        :param bounds: array bounds to update
        """
        filename = self._get_main_path_to_file(array)
        self.storage_adapter.clear_data(
            filename,
            array.shape,
            bounds,
            array.fill_value,
        )

    @check_ctx_state
    def _adapter_iter(self, vid: str) -> Generator[Path, None, None]:
        """Iterate over all the Arrays symlinks in VArray.

        :param vid: Array vid attribute value
        """
        root_path = self.collection_path / self.symlinks_dir / vid
        for root, _, files in os.walk(root_path):
            for file in files:
                if file.endswith(self.file_ext):
                    symlink = Path(os.path.join(root, file))
                    yield symlink

    @check_ctx_state
    def delete_all_by_vid(self, vid: str, collection: "Collection") -> None:
        """Delete all arrays of VArray.

        :param vid: VArray id
        :param collection: collection instance
        """

        def _delete(array: Array) -> None:
            array.delete()

        try:
            metas = self.executor.map(self.read_meta, self._adapter_iter(vid))
            if metas:
                arrays = [create_array_from_meta(collection, meta, self) for meta in metas]
                list(self.executor.map(_delete, arrays))
        except Exception as e:
            self.logger.exception(e)
            raise e
