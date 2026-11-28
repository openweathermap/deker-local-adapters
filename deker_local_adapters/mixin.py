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

import errno
import os

from abc import abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Optional, cast

from cantok import AbstractToken
from deker.ABC.base_adapters import IArrayAdapter
from deker.ABC.base_locks import wait_for_unlock
from deker.arrays import Array, VArray
from deker.errors import DekerArrayError, DekerInstanceNotExistsError, DekerLockError, DekerValidationError
from deker.flock import Flock
from deker.log import SelfLoggerMixin
from deker.tools import get_main_path, get_paths, get_symlink_path
from deker.tools.decorators import check_ctx_state
from deker.tools.path import check_storage_directory
from deker.types import ArrayMeta
from deker.types.private.typings import ArrayT
from deker_tools.cancel import check, mark_started, resolve_timeout
from deker_tools.path import is_empty
from deker_tools.typings import SentinelType

from deker_local_adapters.collection_adapter import empty_trash_directory, move_to_trash
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

    def _remove_empty_parents(self, file: Path) -> None:
        """Remove the directories above a file while they are empty, up to the data or symlinks directory.

        The arrays whose ids share the first characters share the directories of the data tree, and their
        deletions may run at once in several threads: a directory that is gone by the time of its removal
        was removed by another deletion, and the climb goes on to its parent; a directory that is not empty
        anymore (something was created in it meanwhile) stays, and the climb stops there.

        :param file: the file whose parents are removed
        """
        for folder in file.parents:  # pragma: no branch  # the climb always stops at the data/symlinks directory
            if any(str(folder).endswith(directory) for directory in self.dirs):
                break
            try:
                if os.listdir(folder):
                    break
                os.rmdir(folder)
            except FileNotFoundError:
                continue
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    break
                raise

    @property
    def trash_resource(self) -> Path:
        """Return a path to the trash of the storage, next to the directory with collections."""
        return Path(self.uri.path) / self.ctx.config.trash_directory

    def _move_to_trash(self, file: Path) -> None:
        """Move a file or a symlink of an array to the trash, then remove the directories above it that become empty.

        :param file: the main file or the symlink; one that is gone already is not an error
        """
        move_to_trash(file, self.trash_resource, self.collection_path.parent)
        self._remove_empty_parents(file)

    def _delete(self, symlink: Path) -> None:
        """Move the main file the symlink points to, then the symlink, to the trash; empty directories above go too.

        The directories above both files are removed while they are empty, up to the data and symlinks
        directories of the collection, which stay. A missing symlink leaves nothing to move; a missing main file is
        not an error, the symlink is moved anyway.

        :param symlink: path to the symlink file
        """
        try:
            data = symlink.readlink()
        except FileNotFoundError:
            check_storage_directory(self.collection_path.parent)
            return  # no symlink: nothing to delete
        for file in (data, symlink):
            self._move_to_trash(file)

    @check_ctx_state
    def delete(self, array: ArrayT, *, background: bool, _empty_trash: bool, token: AbstractToken) -> None:
        """Move the main file of the array, then its symlink, to the trash, taking no lock; then empty the trash.

        The readers and the writer that hold the main file go on with it in the trash. ``LocalVArrayAdapter`` moves
        the files of a VArray its own way.

        :param array: array instance
        :param background: empty the trash in a daemon thread and return at once
        :param _empty_trash: empty the trash after the move; ``False`` - only move
        :param token: token of the write operation
        """
        check(token)
        mark_started(token)
        try:
            paths = get_paths(array, self.collection_path)
            filename = array.id + self.file_ext
            self._delete(paths.symlink / filename)

        except Exception as e:
            self.logger.exception(e)
            raise e
        if _empty_trash:
            self.empty_trash(background=background)

    @check_ctx_state
    def empty_trash(self, *, background: bool) -> None:
        """Remove everything from the trash of the storage.

        :param background: return at once and remove in a daemon thread, which nobody waits for; its error goes to the
          log
        """
        empty_trash_directory(self.trash_resource, background=background, logger=self.logger)

    def is_deleted(self, array: ArrayT) -> bool:
        """Check if the array was deleted.

        :param array: Array to check
        :returns: ``True`` if the array main file does not exist
        """
        path = get_main_path(array.id, self.collection_path / self.data_dir) / (array.id + self.file_ext)
        # If it does not exist, then it's not created or deleted
        if os.path.exists(path):
            return False
        check_storage_directory(self.collection_path.parent)
        return True

    @check_ctx_state
    def _get_main_path_to_file(self, array: ArrayT) -> Path:
        """Get the path of the main file of the array.

        :param array: Array or VArray
        :returns: path of the main file
        """
        paths = get_paths(array, self.collection_path)
        file = array.id + self.file_ext
        return paths.main / file

    def _get_filenames_for_create_methods(self, array: ArrayT) -> tuple[Path, Path]:
        """Return paths to the main file and symlink, creating their directories inside the collection.

        !!! DO NOT USE THIS METHOD ANYWHERE EXCEPT adapter.create !!!!

        :param array: Array or VArray
        :returns: paths to the main file and to its symlink
        :raises DekerInstanceNotExistsError: if the directory of the collection, or a level above, is gone
        """
        paths = get_paths(array, self.collection_path)
        paths.create(self.collection_path)
        file = array.id + self.file_ext
        main_filename = paths.main / file
        sym_filename = paths.symlink / file
        return main_filename, sym_filename

    def check_symlink_existence(self, file: Path) -> None:
        """Check that the symlink directory of the primary attributes of a (V)Array is empty.

        :param file: Path to symlink
        :raises DekerValidationError: if a (V)Array with the same primary attributes exists
        :raises DekerInstanceNotExistsError: if the symlink directory is gone
        """
        try:
            empty = is_empty(file.parent)
        except NotADirectoryError:
            check_storage_directory(self.collection_path.parent)
            raise DekerInstanceNotExistsError(f"{file.parent} doesn't exist") from None
        if not empty:
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

    def _lock_symlink_directory(self, directory: Path, *, timeout: int | SentinelType, token: AbstractToken) -> Flock:
        """Take the symlink directory of some primary attributes exclusively.

        One creator of a (V)Array with these primary attributes at a time: the next one waits here and then
        finds the directory taken. The lock is an flock on the directory itself, so it works across processes
        and containers on one storage, dies with its holder and leaves no file behind.

        :param directory: symlink directory of the primary attributes
        :param timeout: seconds to wait for the directory; ``Sentinel`` - ``write_lock_timeout`` of the client
        :param token: token of the write operation
        :returns: the held flock, to be released by the caller
        :raises DekerLockError: if the directory is not released within the timeout
        :raises DekerInstanceNotExistsError: if the directory is gone
        """
        flock = Flock(directory)
        try:
            taken = wait_for_unlock(
                self._try_lock,
                (flock,),
                timeout=resolve_timeout(timeout, self.ctx.config.write_lock_timeout),
                interval=self.SYMLINK_DIRECTORY_LOCK_INTERVAL,
                token=token,
            )
        except FileNotFoundError:
            check_storage_directory(self.collection_path.parent)
            raise DekerInstanceNotExistsError(f"{directory} doesn't exist") from None
        if not taken:
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

    def _create_files(self, array: ArrayT, main_filename: Path, sym_filename: Path, checked: tuple[Path, ...]) -> None:
        """Create the main file of a (V)Array and the symlink to it, then check that they are there.

        A path gone meanwhile means that the collection (or the VArray of an Array) was deleted or cleared:
        the created main file and symlink are removed, and the (V)Array does not exist.

        :param array: Array or VArray
        :param main_filename: path to the main file
        :param sym_filename: path to the symlink
        :param checked: other paths that shall be there after the creation: the symlink of the VArray of an Array
        :raises DekerArrayError: if the main file already exists
        :raises DekerInstanceNotExistsError: if a directory of the files or a checked path is gone
        """
        try:
            self._create_main_file(array, main_filename)
        except FileExistsError:
            raise DekerArrayError(f"File {main_filename.resolve()} already exists")
        except FileNotFoundError:
            check_storage_directory(self.collection_path.parent)
            raise DekerInstanceNotExistsError(f"{main_filename.parent} doesn't exist") from None
        try:
            os.symlink(main_filename, sym_filename)
        except FileNotFoundError:
            main_filename.unlink(missing_ok=True)
            check_storage_directory(self.collection_path.parent)
            raise DekerInstanceNotExistsError(f"{sym_filename.parent} doesn't exist") from None
        missing = [path for path in (main_filename, sym_filename, *checked) if not os.path.lexists(path)]
        if missing:
            sym_filename.unlink(missing_ok=True)
            main_filename.unlink(missing_ok=True)
            check_storage_directory(self.collection_path.parent)
            raise DekerInstanceNotExistsError(f"{missing[0]} doesn't exist")

    def _create_on_disk(
        self, array: ArrayT, *, timeout: int | SentinelType, token: AbstractToken, checked: tuple[Path, ...] = ()
    ) -> ArrayT:
        """Create the files of a (V)Array, holding the symlink directory of its primary attributes meanwhile.

        :param array: Array or VArray
        :param timeout: seconds to wait for the symlink directory; ``Sentinel`` - ``write_lock_timeout`` of the client
        :param token: token of the write operation
        :param checked: other paths that shall be there after the creation: the symlink of the VArray of an Array
        :returns: the same instance, once its files are created
        :raises DekerValidationError: if a (V)Array with the same primary attributes exists
        :raises DekerArrayError: if the main file already exists
        :raises DekerInstanceNotExistsError: if the collection (or the VArray of an Array) is deleted or cleared
          meanwhile; what was created is removed
        """
        main_filename, sym_filename = self._get_filenames_for_create_methods(array)
        if not array.primary_attributes:
            mark_started(token)
            self._create_files(array, main_filename, sym_filename, checked)
            return array
        directory_lock = self._lock_symlink_directory(sym_filename.parent, timeout=timeout, token=token)
        try:
            self.check_symlink_existence(sym_filename)
            mark_started(token)
            self._create_files(array, main_filename, sym_filename, checked)
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

    def _symlink_by_primary_attributes(self, primary_attributes: dict, schema: "BaseArraysSchema") -> Path | None:
        """Find the symlink of the array with the primary attributes.

        The symlinks of the arrays with the attributes live in one directory; the first file there is the one.

        :param primary_attributes: Key attributes
        :param schema: (V)Array schema
        :returns: the path of the symlink, or ``None`` if there is no array with the attributes
        :raises DekerBrokenSymlinkError: if the symlink of the attributes points to a missing file
        """
        symlinks_path = get_symlink_path(
            path_to_symlink_dir=self.collection_path / self.symlinks_dir,
            primary_attributes_schema=schema.primary_attributes,
            primary_attributes=primary_attributes,
        )
        try:
            with os.scandir(symlinks_path) as files:
                for file in files:
                    if file.is_file():
                        path = Path(symlinks_path) / file.name
                        self.logger.debug("filtered %s", path)
                        return path
                    # If symlink is broken
                    elif file.is_symlink() and not file.is_file():
                        raise DekerBrokenSymlinkError(f"Seems like {file} is a broken symlink.")
        except FileNotFoundError:
            check_storage_directory(self.collection_path.parent)
            self.logger.debug("nothing was filtered")
        return None

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
        :raises DekerFilterError: if the attributes are not exactly the primary attributes of the schema
        :raises DekerBrokenSymlinkError: if the symlink of the attributes points to a missing file
        """
        path = self._symlink_by_primary_attributes(self._check_primary_attributes(primary_attributes, schema), schema)
        if path is None:
            return None
        return self._init_array(path, collection, array_adapter, varray_adapter)

    def get_id_by_primary_attributes(self, primary_attributes: dict, schema: "BaseArraysSchema") -> str | None:
        """Find the id of the array with the primary attributes: the name of its symlink, without reading the file.

        :param primary_attributes: Key attributes
        :param schema: (V)Array schema
        :returns: the id, or ``None`` if there is no array with the attributes
        :raises DekerFilterError: if the attributes are not exactly the primary attributes of the schema
        :raises DekerBrokenSymlinkError: if the symlink of the attributes points to a missing file
        """
        path = self._symlink_by_primary_attributes(self._check_primary_attributes(primary_attributes, schema), schema)
        if path is None:
            return None
        return path.name.removesuffix(self.file_ext)

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
        :returns: Array or VArray with the id, or ``None`` if its file does not exist or is gone while it is read
        """
        filename = id_ + self.file_ext

        data_path = self.collection_path / self.data_dir
        path = get_main_path(id_, data_path)
        file = path / filename
        if file.exists():
            self.logger.debug("filtered %s", file)
            try:
                return self._init_array(file, collection, array_adapter, varray_adapter)
            except DekerInstanceNotExistsError:
                return None  # the file is gone since it was found
        check_storage_directory(self.collection_path.parent)
        return None

    def __iter__(self) -> Generator[ArrayMeta, None, None]:
        """Iterate over all the arrays or varrays in collection, walking its data directory.

        :yields: the metadata of every main file; a file gone since its directory was listed is skipped
        """
        root_path = self.collection_path / self.data_dir
        for root, _, files in os.walk(root_path):
            for file in files:
                if not file.endswith(self.file_ext):
                    continue
                path = Path(os.path.join(root, file))
                try:
                    meta = self.read_meta(path)
                except DekerInstanceNotExistsError:
                    continue
                yield meta
