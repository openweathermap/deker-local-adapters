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

import os

from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Optional, Tuple, Union

from deker.arrays import Array, VArray
from deker.errors import DekerArrayError, DekerValidationError
from deker.tools import get_main_path, get_paths, get_symlink_path
from deker.tools.decorators import check_ctx_state
from deker.types import ArrayMeta
from deker_tools.path import is_empty

from deker_local_adapters.errors import DekerBrokenSymlinkError


if TYPE_CHECKING:
    from deker.ABC import BaseArraysSchema
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.collection import Collection


class LocalAdapterMixin(object):
    """Mixin for local adapters.

    This particular mixin depends on IArray interface.
    Keep in mind that changes in IArray may require making changes here.
    """

    file_ext: str
    data_dir: str
    symlinks_dir: str
    collection_path: Path
    dirs: Tuple
    logger: Logger
    executor: ThreadPoolExecutor

    def _delete(self, symlink: Path) -> None:
        """Delete symlink and realpath files from disk.

        :param symlink: path to the symlink file
        """
        try:
            data = symlink.readlink()
            if data.exists():
                os.remove(data)

            symlink.unlink(missing_ok=True)

            for file in (data, symlink):
                if file.parent.exists():
                    for folder in file.parents:
                        subs = os.listdir(folder)
                        if subs or any(str(folder).endswith(directory) for directory in self.dirs):
                            break
                        if folder.exists():
                            os.rmdir(folder)
        except FileNotFoundError:
            pass

    @check_ctx_state
    def delete(self, array: Union[Array, VArray]) -> None:
        """Delete the existing array.

        :param array: array instance
        """
        try:
            paths = get_paths(array, self.collection_path)
            filename = array.id + self.file_ext
            self._delete(paths.symlink / filename)

        except Exception as e:
            self.logger.exception(e)
            raise e

    def is_deleted(self, array: Union[Array, VArray]) -> bool:
        """Check if the array was deleted.

        :param array: Array to check
        """
        path = get_main_path(array.id, self.collection_path / self.data_dir)
        # If it does not exist, then it's not created or deleted
        return not os.path.exists(path)

    @check_ctx_state
    def _get_main_path_to_file(self, array: Union[Array, VArray]) -> Path:
        paths = get_paths(array, self.collection_path)
        file = array.id + self.file_ext
        return paths.main / file

    def get_filenames_for_create_methods(self, array: Union[Array, VArray]) -> Tuple[Path, Path]:
        """Return paths to the main file and symlink.

        !!! DO NOT USE THIS METHOD ANYWHERE EXCEPT adapter.create !!!!

        :param array: Array or VArray
        """
        paths = get_paths(array, self.collection_path)
        paths.create()
        file = array.id + self.file_ext
        main_filename = paths.main / file
        sym_filename = paths.symlink / file
        return main_filename, sym_filename

    @staticmethod
    def check_array_file_existence(file: Path) -> None:
        """Check that (V)Array main file exists.

        :param file: Path to the main file
        """
        if file.exists():
            raise DekerArrayError(f"File {file.resolve()} already exists")

    def check_symlink_existence(self, file: Path) -> None:
        """Check that there is no symlink for a given (V)Array.

        :param file: Path to symlink
        """
        if not is_empty(file.parent):
            message = f"File {file.resolve()} already exists"
            self.logger.warning(message)
            raise DekerValidationError(message)

    def _init_array(
        self,
        path: Path,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: "BaseVArrayAdapter",
    ) -> Union[Array, VArray]:
        """Read meta and make an array/varray object from it.

        :param path: Path to meta
        :param collection: Collection instance
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        """
        meta = self.read_meta(path)  # type: ignore[attr-defined]

        if varray_adapter:
            array = VArray._create_from_meta(collection, meta, array_adapter, varray_adapter)
        else:
            array = Array._create_from_meta(collection, meta, array_adapter, varray_adapter)
        return array

    def get_by_primary_attributes(
        self,
        primary_attributes: dict,
        schema: "BaseArraysSchema",
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: "BaseVArrayAdapter",
    ) -> Optional[Union[Array, VArray]]:
        """Find (V)Array by given primary attributes.

        :param primary_attributes: Key attributes
        :param schema: (V)Array schema
        :param collection: Collection instance
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        """
        symlinks_path = get_symlink_path(
            path_to_symlink_dir=collection.path / self.symlinks_dir,  # type: ignore[operator]
            primary_attributes_schema=schema.primary_attributes,
            primary_attributes=primary_attributes,
        )

        try:
            for file in os.scandir(
                symlinks_path,
            ):  # type: ignore
                if file.is_file():
                    array_name = file.name
                    path = Path(symlinks_path) / array_name
                    self.logger.debug(f"filtered {path}")  # type: ignore[attr-defined]
                    return self._init_array(path, collection, array_adapter, varray_adapter)
                # If symlink is broken
                elif file.is_symlink() and not file.is_file():
                    raise DekerBrokenSymlinkError(f"Seems like {file} is a broken symlink.")
        except FileNotFoundError:
            self.logger.debug("nothing was filtered")  # type: ignore[attr-defined]

    def get_by_id(
        self,
        id_: str,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: "BaseVArrayAdapter",
    ) -> Optional[Union[Array, VArray]]:
        """Find (V)Array by id.

        :param id_: Array id
        :param collection: Collection
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        """
        filename = id_ + self.file_ext

        data_path = collection.path / self.data_dir  # type: ignore[operator]
        path = get_main_path(id_, data_path)
        file = path / filename
        if file.exists():
            self.logger.debug(f"filtered {file}")  # type: ignore[attr-defined]
            return self._init_array(file, collection, array_adapter, varray_adapter)

    def __iter__(self) -> Generator[ArrayMeta, None, None]:
        """Iterate over all the arrays or varrays in collection."""
        root_path = self.collection_path / self.data_dir
        for root, _, files in os.walk(root_path):
            for file in files:
                if not file.endswith(self.file_ext):
                    continue
                path = Path(os.path.join(root, file))
                meta = self.read_meta(path)  # type: ignore[attr-defined]
                yield meta
