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
import logging
import os

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Type, Union

from deker.ABC import BaseStorageAdapter
from deker.ABC.base_adapters import BaseVArrayAdapter
from deker.arrays import VArray
from deker.ctx import CTX
from deker.errors import DekerArrayError
from deker.locks import CreateArrayLock, UpdateMetaAttributeLock
from deker.log import SelfLoggerMixin
from deker.tools.decorators import check_ctx_state
from deker.types import ArrayMeta, Numeric, Slice
from numpy import ndarray

from deker_local_adapters.mixin import LocalAdapterMixin


class LocalVArrayAdapter(SelfLoggerMixin, LocalAdapterMixin, BaseVArrayAdapter):
    """Adapter for virtual arrays."""

    def __init__(
        self,
        collection_path: Path,
        ctx: "CTX",
        storage_adapter: Type["BaseStorageAdapter"],
        executor: ThreadPoolExecutor,
    ) -> None:
        self.data_dir = ctx.config.varray_data_directory
        self.symlinks_dir = ctx.config.varray_symlinks_directory
        self.file_ext = ".json"
        self.collection_path = collection_path
        self.dirs = (self.data_dir, self.symlinks_dir)
        super().__init__(collection_path, ctx, executor, storage_adapter)

    @check_ctx_state
    def read_data(self, array: "VArray", bounds: Slice) -> Union[Numeric, ndarray]:
        """Not implemented."""
        raise NotImplementedError

    @check_ctx_state
    def update(self, array: "VArray", bounds: Slice, data: Any) -> None:
        """Not implemented."""
        raise NotImplementedError

    @check_ctx_state
    def clear(self, array: "VArray", bounds: Slice) -> None:
        """Not implemented."""
        raise NotImplementedError

    @check_ctx_state
    @CreateArrayLock()
    def create(self, array: VArray) -> VArray:
        """Create varray.

        :param array: VArray instance
        """
        main_filename, sym_filename = self.get_filenames_for_create_methods(array)
        self.check_array_file_existence(main_filename)

        if array.primary_attributes:
            self.check_symlink_existence(sym_filename)

        os.makedirs(main_filename.parent, exist_ok=True)
        varray_meta = array._create_meta()
        with open(main_filename, "w", encoding="utf-8") as f:
            f.write(varray_meta)
        os.symlink(main_filename, sym_filename)
        return array

    @check_ctx_state
    def read_meta(self, array: Union[VArray, Path]) -> ArrayMeta:
        """Read VArray metadata.

        :param array:  VArray instance
        """
        if isinstance(array, VArray):
            filename = self._get_main_path_to_file(array)
        else:
            filename = array
        try:
            with open(filename, encoding="utf-8") as f:
                data = f.read()
                meta = json.loads(data)
                return meta
        except Exception:
            raise DekerArrayError(
                "No metadata in the varray. Try to delete and recreate the varray."
            )

    @check_ctx_state
    @UpdateMetaAttributeLock()
    def update_meta_custom_attributes(self, array: VArray, attributes: dict) -> None:
        """Update meta data in VArray.

        :param array: VArray instance
        :param attributes: new custom attributes
        """
        filename = self._get_main_path_to_file(array)
        try:
            with open(filename, "r") as f:
                meta = json.loads(f.read())
            meta["custom_attributes"] = attributes
            json_meta = json.dumps(meta, default=str)
            with open(filename, "w") as f:
                f.write(json_meta)
            array.custom_attributes = attributes
        except Exception as e:
            logging.exception(e)
            raise e
