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

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from deker.ABC import BaseStorageAdapter
from deker.ABC.base_adapters import BaseVArrayAdapter
from deker.arrays import VArray
from deker.ctx import CTX
from deker.errors import DekerArrayError
from deker.locks import UpdateMetaAttributeLock
from deker.tools.decorators import check_ctx_state
from deker.types import ArrayMeta, Data, Numeric, Slice
from numpy import ndarray

from deker_local_adapters.mixin import LocalAdapterMixin


class LocalVArrayAdapter(LocalAdapterMixin[VArray], BaseVArrayAdapter[Path]):
    """Adapter of the ``VArrays`` of a collection on the local disk.

    A ``VArray`` is a json file with its metadata in the data directory, named by its id, and a symlink to it
    in the symlinks directory, placed by its primary attributes. It holds no data: the data live in its
    ``Arrays``, which a ``VSubset`` reads and writes through the ``Arrays`` adapter.
    """

    def __init__(
        self,
        collection_path: Path,
        ctx: "CTX",
        storage_adapter: type["BaseStorageAdapter"],
        executor: ThreadPoolExecutor,
    ) -> None:
        """LocalVArrayAdapter constructor.

        :param collection_path: directory of the collection
        :param ctx: context of the client
        :param storage_adapter: type of the adapter of the files of the Arrays
        :param executor: thread pool the Arrays of a VArray are processed in
        """
        self.data_dir = ctx.config.varray_data_directory
        self.symlinks_dir = ctx.config.varray_symlinks_directory
        self.file_ext = ".json"
        self.collection_path = collection_path
        self.dirs = (self.data_dir, self.symlinks_dir)
        super().__init__(collection_path, ctx, executor, storage_adapter)

    @check_ctx_state
    def read_data(self, array: "VArray", bounds: Slice) -> Numeric | ndarray:
        """Not implemented: a VArray holds no data, a VSubset reads its Arrays.

        :param array: VArray instance
        :param bounds: bounds to read
        :returns: nothing, the call never returns
        :raises NotImplementedError: always
        """
        raise NotImplementedError

    @check_ctx_state
    def update(self, array: "VArray", bounds: Slice, data: Data) -> None:
        """Not implemented: a VArray holds no data, a VSubset writes its Arrays.

        :param array: VArray instance
        :param bounds: bounds to update
        :param data: new data
        :raises NotImplementedError: always
        """
        raise NotImplementedError

    @check_ctx_state
    def clear(self, array: "VArray", bounds: Slice) -> None:
        """Not implemented: a VArray holds no data, a VSubset clears its Arrays.

        :param array: VArray instance
        :param bounds: bounds to clear
        :raises NotImplementedError: always
        """
        raise NotImplementedError

    @check_ctx_state
    def create(self, array: VArray) -> VArray:
        """Create a new virtual array on disk storage.

        :param array: VArray instance
        :returns: the same instance, once its files are created
        :raises DekerArrayError: if the varray file already exists
        :raises DekerValidationError: if a varray with the same primary attributes exists
        """
        return self._create_on_disk(array)

    def _create_main_file(self, array: VArray, main_filename: Path) -> None:
        """Create the json file of a VArray with its metadata.

        :param array: VArray instance
        :param main_filename: path to the json file
        :raises FileExistsError: if the file already exists
        """
        with open(main_filename, "x", encoding="utf-8") as f:
            f.write(array._create_meta())

    @check_ctx_state
    def read_meta(self, array: VArray | Path) -> ArrayMeta:
        """Read VArray metadata from its json file.

        :param array: VArray instance or the path to its main file
        :returns: VArray metadata
        :raises DekerArrayError: if the file is missing or is not a json
        """
        filename = array if isinstance(array, Path) else self._get_main_path_to_file(array)
        try:
            with open(filename, encoding="utf-8") as f:
                data = f.read()
                meta = json.loads(data)
                return meta
        except Exception as e:
            raise DekerArrayError(f"VArray metadata cannot be read from {filename}: {e}")

    @check_ctx_state
    @UpdateMetaAttributeLock()
    def update_meta_custom_attributes(self, array: VArray, attributes: dict) -> None:
        """Replace the custom attributes in the json file of the VArray, holding it exclusively, and in the object.

        The new metadata is written to a temporary file next to the json file and moved over it in one step,
        so a reader never meets a half-written file.

        :param array: VArray instance
        :param attributes: new custom attributes
        :raises DekerLockError: if the file is held by another writer
        """
        filename = self._get_main_path_to_file(array)
        temporary = filename.with_name(filename.name + ".tmp")
        try:
            with open(filename, encoding="utf-8") as f:
                meta = json.loads(f.read())
            meta["custom_attributes"] = attributes
            json_meta = json.dumps(meta, default=str)
            try:
                with open(temporary, "w", encoding="utf-8") as f:
                    f.write(json_meta)
                os.replace(temporary, filename)
            finally:
                temporary.unlink(missing_ok=True)
            array.custom_attributes = attributes
        except Exception as e:
            self.logger.exception(e)
            raise e
