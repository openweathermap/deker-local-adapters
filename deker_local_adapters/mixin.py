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

from abc import abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Optional, cast

from deker.ABC.base_adapters import IArrayAdapter
from deker.arrays import Array, VArray
from deker.errors import DekerArrayError, DekerLockError, DekerValidationError
from deker.flock import Flock
from deker.locks import wait_for_unlock
from deker.log import SelfLoggerMixin
from deker.tools import get_main_path, get_paths, get_symlink_path
from deker.tools.decorators import check_ctx_state
from deker.types import ArrayMeta
from deker.types.private.typings import ArrayT
from deker_tools.path import is_empty

from deker_local_adapters.errors import DekerBrokenSymlinkError

if TYPE_CHECKING:
    from deker.ABC import BaseArraysSchema
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.collection import Collection


class LocalAdapterMixin(SelfLoggerMixin, IArrayAdapter[Path, ArrayT]):
    """Common part of the local Array and VArray adapters: files, symlinks and lookups on the local disk.

    ``ArrayT`` is the kind of arrays the adapter manages. The attributes below are set by the adapters
    in their ``__init__``.
    """

    file_ext: str
    data_dir: str
    symlinks_dir: str
    dirs: tuple[str, str]
    SYMLINK_DIRECTORY_LOCK_INTERVAL = 0.05  # creation is short, so the directory is polled often

    @abstractmethod
    def read_meta(self, array: ArrayT | Path) -> ArrayMeta:
        """Read the metadata of an array by its instance or by the path to its file.

        :param array: Array or VArray instance, or the path to its main file
        :returns: array metadata
        """
        pass

    def _delete(self, symlink: Path) -> None:
        """Delete the symlink and the main file it points to, with the directories that become empty.

        The directories above both files are removed while they are empty, up to the data and symlinks
        directories of the collection, which stay. A missing file is not an error.

        :param symlink: path to the symlink file
        """
        try:
            data = symlink.readlink()
            if data.exists():
                os.remove(data)

            symlink.unlink(missing_ok=True)

            for file in (data, symlink):
                if file.parent.exists():
                    # climbs up while the directories are empty; always stops at the data/symlinks directory
                    for folder in file.parents:  # pragma: no branch
                        subs = os.listdir(folder)
                        if subs or any(str(folder).endswith(directory) for directory in self.dirs):
                            break
                        if folder.exists():  # pragma: no branch  # guard: listdir() above has just succeeded
                            os.rmdir(folder)
        except FileNotFoundError:
            pass

    @check_ctx_state
    def delete(self, array: ArrayT) -> None:
        """Delete the files of the array.

        :param array: array instance
        """
        try:
            paths = get_paths(array, self.collection_path)
            filename = array.id + self.file_ext
            self._delete(paths.symlink / filename)

        except Exception as e:
            self.logger.exception(e)
            raise e

    def is_deleted(self, array: ArrayT) -> bool:
        """Check if the array was deleted.

        :param array: Array to check
        :returns: ``True`` if the array main file does not exist
        """
        path = get_main_path(array.id, self.collection_path / self.data_dir)
        # If it does not exist, then it's not created or deleted
        return not os.path.exists(path)

    @check_ctx_state
    def _get_main_path_to_file(self, array: ArrayT) -> Path:
        """Get the path of the main file of the array.

        :param array: Array or VArray
        :returns: path of the main file
        """
        paths = get_paths(array, self.collection_path)
        file = array.id + self.file_ext
        return paths.main / file

    def get_filenames_for_create_methods(self, array: ArrayT) -> tuple[Path, Path]:
        """Return paths to the main file and symlink, creating their directories.

        !!! DO NOT USE THIS METHOD ANYWHERE EXCEPT adapter.create !!!!

        :param array: Array or VArray
        :returns: paths to the main file and to its symlink
        """
        paths = get_paths(array, self.collection_path)
        paths.create()
        file = array.id + self.file_ext
        main_filename = paths.main / file
        sym_filename = paths.symlink / file
        return main_filename, sym_filename

    def check_symlink_existence(self, file: Path) -> None:
        """Check that the symlink directory of the primary attributes of a (V)Array is empty.

        :param file: Path to symlink
        :raises DekerValidationError: if a (V)Array with the same primary attributes exists
        """
        if not is_empty(file.parent):
            message = f"File {file.resolve()} already exists"
            self.logger.warning(message)
            raise DekerValidationError(message)

    @staticmethod
    def _try_lock(flock: Flock) -> bool:
        """Try to take a flock once.

        :param flock: the flock to take
        :returns: ``True`` if the flock is held now
        """
        try:
            flock.acquire()
        except DekerLockError:
            return False
        return True

    def _lock_symlink_directory(self, directory: Path) -> Flock:
        """Take the symlink directory of some primary attributes exclusively.

        One creator of a (V)Array with these primary attributes at a time: the next one waits here and then
        finds the directory taken. The lock is an flock on the directory itself, so it works across processes
        and containers on one storage, dies with its holder and leaves no file behind.

        :param directory: symlink directory of the primary attributes
        :returns: the held flock, to be released by the caller
        :raises DekerLockError: if the directory is not released within ``write_lock_timeout``
        """
        flock = Flock(directory)
        if not wait_for_unlock(
            self._try_lock, (flock,), self.ctx.config.write_lock_timeout, self.SYMLINK_DIRECTORY_LOCK_INTERVAL
        ):
            raise DekerLockError(f"Directory {directory} is locked for the creation of an array")
        return flock

    @abstractmethod
    def _create_main_file(self, array: ArrayT, main_filename: Path) -> None:
        """Create the main file of a (V)Array in one step.

        :param array: Array or VArray
        :param main_filename: path to the main file
        :raises FileExistsError: if the file already exists
        """
        pass

    def _create_files(self, array: ArrayT, main_filename: Path, sym_filename: Path) -> None:
        """Create the main file of a (V)Array and the symlink to it.

        :param array: Array or VArray
        :param main_filename: path to the main file
        :param sym_filename: path to the symlink
        :raises DekerArrayError: if the main file already exists
        """
        try:
            self._create_main_file(array, main_filename)
        except FileExistsError:
            raise DekerArrayError(f"File {main_filename.resolve()} already exists")
        os.symlink(main_filename, sym_filename)

    def _create_on_disk(self, array: ArrayT) -> ArrayT:
        """Create the files of a (V)Array, holding the symlink directory of its primary attributes meanwhile.

        :param array: Array or VArray
        :returns: the same instance, once its files are created
        :raises DekerValidationError: if a (V)Array with the same primary attributes exists
        :raises DekerArrayError: if the main file already exists
        """
        main_filename, sym_filename = self.get_filenames_for_create_methods(array)
        if not array.primary_attributes:
            self._create_files(array, main_filename, sym_filename)
            return array
        directory_lock = self._lock_symlink_directory(sym_filename.parent)
        try:
            self.check_symlink_existence(sym_filename)
            self._create_files(array, main_filename, sym_filename)
        finally:
            directory_lock.release()
        return array

    def _init_array(
        self,
        path: Path,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> ArrayT:
        """Read meta and make an array/varray object from it.

        :param path: Path to meta
        :param collection: Collection instance
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter, passed by VArrays' adapters only
        :returns: Array or VArray built from the meta
        :raises DekerMetaDataError: if the metadata is invalid or corrupted
        """
        meta = self.read_meta(path)

        # a VArrays' adapter gets the VArrays' adapter, an Arrays' adapter does not: the kind matches ``ArrayT``
        array: Array | VArray
        if varray_adapter:
            array = VArray._create_from_meta(collection, meta, array_adapter, varray_adapter)
        else:
            array = Array._create_from_meta(collection, meta, array_adapter, varray_adapter)
        return cast("ArrayT", array)

    def get_by_primary_attributes(
        self,
        primary_attributes: dict,
        schema: "BaseArraysSchema",
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> ArrayT | None:
        """Find (V)Array by given primary attributes.

        :param primary_attributes: Key attributes
        :param schema: (V)Array schema
        :param collection: Collection instance
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        :returns: matching Array or VArray, or ``None`` if nothing was found
        :raises DekerBrokenSymlinkError: if the symlink of the attributes points to a missing file
        """
        symlinks_path = get_symlink_path(
            path_to_symlink_dir=self.collection_path / self.symlinks_dir,
            primary_attributes_schema=schema.primary_attributes,
            primary_attributes=primary_attributes,
        )

        try:
            for file in os.scandir(
                symlinks_path,
            ):
                if file.is_file():
                    array_name = file.name
                    path = Path(symlinks_path) / array_name
                    self.logger.debug("filtered %s", path)
                    return self._init_array(path, collection, array_adapter, varray_adapter)
                # If symlink is broken
                elif file.is_symlink() and not file.is_file():
                    raise DekerBrokenSymlinkError(f"Seems like {file} is a broken symlink.")
        except FileNotFoundError:
            self.logger.debug("nothing was filtered")

    def get_by_id(
        self,
        id_: str,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> ArrayT | None:
        """Find (V)Array by id.

        :param id_: Array id
        :param collection: Collection
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        :returns: Array or VArray with the id, or ``None`` if its file does not exist
        """
        filename = id_ + self.file_ext

        data_path = self.collection_path / self.data_dir
        path = get_main_path(id_, data_path)
        file = path / filename
        if file.exists():
            self.logger.debug("filtered %s", file)
            return self._init_array(file, collection, array_adapter, varray_adapter)

    def __iter__(self) -> Generator[ArrayMeta, None, None]:
        """Iterate over all the arrays or varrays in collection, walking its data directory.

        :yields: the metadata of every main file
        """
        root_path = self.collection_path / self.data_dir
        for root, _, files in os.walk(root_path):
            for file in files:
                if not file.endswith(self.file_ext):
                    continue
                path = Path(os.path.join(root, file))
                meta = self.read_meta(path)
                yield meta
