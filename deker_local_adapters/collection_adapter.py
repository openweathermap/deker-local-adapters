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
import json
import logging
import os
import shutil
import threading

from collections.abc import Callable, Generator
from pathlib import Path
from typing import TYPE_CHECKING

from cantok import AbstractToken
from deker.ABC.base_adapters import BaseCollectionAdapter
from deker.ctx import CTX
from deker.errors import (
    DekerCollectionAlreadyExistsError,
    DekerCollectionNotExistsError,
    DekerCriticalError,
    DekerTrashError,
)
from deker.locks import CollectionLock
from deker.tools import check_memory
from deker.tools.decorators import check_ctx_state
from deker.tools.path import check_storage_directory
from deker_tools.cancel import check, mark_started
from deker_tools.typings import SentinelType

if TYPE_CHECKING:
    from deker.collection import Collection


def empty_trash_directory(trash: Path, *, background: bool, logger: logging.Logger) -> None:
    """Remove everything that lies in the trash of the storage; the trash itself stays.

    The only code of the removal of the trash. The whole trash is walked without following its symlinks: a symlink
    is removed, its target stays. Everything that can be removed is removed; what is gone already is not an error
    (another removal of the trash took it). A cancelled operation does not stop the removal.

    :param trash: the trash directory of the storage
    :param background: return at once and remove in a daemon thread of its own, not of the executor of the client;
      nobody waits for it, its error goes to the log
    :param logger: logger of the adapter, for the error of the removal in the background
    :raises DekerTrashError: if something is not removed, once at the end: the number of what is not removed and the
      unique reasons; a directory left only because something in it is left is not counted. Not with ``background``
    """
    if background:
        threading.Thread(target=_empty_trash_in_background, args=(trash, logger), daemon=True).start()
        return
    failures: list[str] = []

    def fail(error: OSError) -> None:
        """Count what is not removed or listed, unless it is gone already or keeps something not removed.

        :param error: the error of the removal or of the listing
        """
        if error.filename == str(trash):  # the trash itself is not listed
            check_storage_directory(trash)
        if error.errno not in (errno.ENOENT, errno.ENOTEMPTY):
            failures.append(str(error.strerror))

    def remove(function: Callable[[str], None], path: str) -> None:
        """Remove a file, a symlink or an empty directory.

        :param function: ``os.unlink`` or ``os.rmdir``
        :param path: what to remove
        """
        try:
            function(path)
        except OSError as error:
            fail(error)

    for root, directories, files in os.walk(str(trash), topdown=False, onerror=fail):
        for name in files:
            remove(os.unlink, os.path.join(root, name))
        for name in directories:  # a symlink to a directory is listed here and not walked
            path = os.path.join(root, name)
            remove(os.unlink if os.path.islink(path) else os.rmdir, path)
    if failures:
        raise DekerTrashError(
            f"The trash of the storage is not emptied: {len(failures)} files are not removed from {trash}. "
            f"Reasons: {'; '.join(sorted(set(failures)))}"
        )


def _empty_trash_in_background(trash: Path, logger: logging.Logger) -> None:
    """Remove everything from the trash in the thread of the background removal: its error goes to the log.

    :param trash: the trash directory of the storage
    :param logger: logger of the adapter
    """
    try:
        empty_trash_directory(trash, background=False, logger=logger)
    except DekerCriticalError:
        pass  # logged as critical where it is raised
    except Exception as e:  # noqa: BLE001  # nobody waits for the thread: the log is the only one to tell
        logger.error(e)


class LocalCollectionAdapter(BaseCollectionAdapter[Path]):
    """Adapter of the collections of a local storage.

    A collection is a directory under the collections directory of the storage, named by the collection,
    with a json file of the same name holding its metadata; ``<collection>.lock`` next to the directory is
    held while the collection is created or cleared and stays as its marker.
    """

    def __init__(self, ctx: CTX) -> None:
        """LocalCollectionAdapter constructor: creates the collections directory and the trash of the storage.

        The client builds the adapter when it opens; it is the only place the directories are created. Nobody
        removes them: one missing when it is needed is critical.

        :param ctx: context of the client
        """
        super().__init__(ctx)
        self.metadata_version = "0.2"
        self.file_ext = ".json"
        self.collections_resource.mkdir(parents=True, exist_ok=True)
        self.trash_resource.mkdir(exist_ok=True)

    def __create_path(self, name: str) -> Path:
        """Create Collection path in the file system.

        :param name: Collection name
        :returns: path to the collection directory
        :raises DekerCollectionAlreadyExistsError: if the directory exists
        """
        path = self.collections_resource / name
        try:
            os.mkdir(path)  # never the collections directory: the client creates it
            return path
        except FileExistsError:
            raise DekerCollectionAlreadyExistsError(f"Collection {name} already exists")

    def __create(self, coll_name: str, data: dict, coll_path: Path) -> None:
        """Write the metadata file of the collection into its directory.

        :param coll_name: Collection name
        :param data: metadata of the collection
        :param coll_path: directory of the collection
        """
        if coll_path.exists():
            path = coll_path / (coll_name + self.file_ext)
            with open(path, mode="w") as f:
                f.write(json.dumps(data))

    @check_ctx_state
    @CollectionLock()
    def create(self, collection: "Collection", *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Add collection to the storage and create file structure, holding the collection lock.

        :param collection: Collection to be created
        :param timeout: seconds to wait for the lock of the collection as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client. Read by the lock decorator of the method (``CollectionLock``), not by
          its body: the lock waits for another creation or clearing of the collection no longer than that
        :param token: token of the write operation
        :raises DekerMemoryError: if one array of the collection would not fit the memory limit, unless the
          check is skipped by the client
        :raises DekerCollectionAlreadyExistsError: if a collection of the name exists
        """
        check(token)
        try:
            schema = collection.array_schema
            shape = schema.arrays_shape if hasattr(schema, "arrays_shape") else schema.shape
            if not self.ctx.config.skip_collection_create_memory_check:
                check_memory(shape, schema.dtype, self.ctx.config.memory_limit)
            mark_started(token)
            coll_path = self.__create_path(collection.name)
            prepared = collection.as_dict
            self.__create(coll_name=collection.name, data=prepared, coll_path=coll_path)
        except Exception as e:
            self.logger.exception(e)
            raise e

    @check_ctx_state
    def read(self, name: str) -> dict:
        """Read collection metadata.

        :param name: Collection name
        :returns: collection metadata as a dict
        :raises DekerCollectionNotExistsError: if there is no collection of the name
        """
        coll_path = self.collections_resource / name
        path = coll_path / (name + self.file_ext)
        if path.exists():
            with open(path) as f:
                data = json.loads(f.read())
                return data
        check_storage_directory(self.collections_resource)
        raise DekerCollectionNotExistsError(f"{name} collection doesn't exist")

    @check_ctx_state
    def delete(self, collection: "Collection", *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Delete the directory of the collection with everything in it, and its lock file.

        A directory that is gone already is not an error; any other failure to remove it is raised. The deletion
        takes no lock: the timeout is not used.

        :param collection: Collection to delete
        :param timeout: not used
        :param token: token of the write operation
        """
        check(token)
        mark_started(token)
        try:
            try:
                shutil.rmtree(self.collections_resource / collection.name)
            except FileNotFoundError:
                check_storage_directory(self.collections_resource)
            Path.unlink(self.collections_resource / (collection.name + ".lock"), missing_ok=True)
        except Exception as e:
            self.logger.exception(e)
            raise e

    @check_ctx_state
    @CollectionLock()
    def clear(self, collection: "Collection", *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Remove everything from the directory of the collection but its metadata file, holding the collection lock.

        :param collection: Collection to be cleared
        :param timeout: seconds to wait for the lock of the collection as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client. Read by the lock decorator of the method (``CollectionLock``), not by
          its body: the lock waits for another creation or clearing of the collection no longer than that
        :param token: token of the write operation
        """
        check(token)
        mark_started(token)
        try:
            for root, dirs, files in os.walk(self.collections_resource / collection.name):
                for f in files:
                    save = False
                    if f == collection.name + self.file_ext:
                        save = True
                    if not save:
                        os.remove(os.path.join(root, f))
                for d in dirs:
                    shutil.rmtree(os.path.join(root, d))
        except Exception as e:
            self.logger.exception(e)
            raise e

    @property
    def collections_resource(self) -> Path:
        """Return a path to the directory with collections."""
        return Path(self.uri.path) / self.ctx.config.collections_directory

    @property
    def trash_resource(self) -> Path:
        """Return a path to the trash of the storage, next to the directory with collections."""
        return Path(self.uri.path) / self.ctx.config.trash_directory

    @check_ctx_state
    def empty_trash(self, *, background: bool) -> None:
        """Remove everything from the trash of the storage.

        :param background: return at once and remove in a daemon thread, which nobody waits for; its error goes to the
          log
        """
        empty_trash_directory(self.trash_resource, background=background, logger=self.logger)

    @check_ctx_state
    def is_deleted(self, collection: "Collection") -> bool:
        """Check if collection was deleted.

        :param collection: Collection to check
        :returns: ``True`` if the collection directory no longer exists
        """
        # If it does not exist, then it's not created or deleted
        if (self.collections_resource / collection.name).exists():
            return False
        check_storage_directory(self.collections_resource)
        return True

    @check_ctx_state
    def __iter__(self) -> Generator[dict, None, None]:
        """Iterate over the metadata of the collections: every directory under the collections directory.

        :yields: the metadata of every collection; a directory without its collection file is skipped
        :raises DekerCriticalError: if the collections directory of the storage does not exist
        """
        try:
            directories = os.scandir(self.collections_resource)
        except FileNotFoundError:
            check_storage_directory(self.collections_resource)
            raise
        with directories:
            for directory in directories:
                if os.path.isdir(directory):
                    try:
                        meta = self.read(directory.name)
                    except DekerCollectionNotExistsError:
                        continue  # no collection file
                    except FileNotFoundError:
                        check_storage_directory(self.collections_resource)
                        continue  # the collection file is gone since it was checked
                    yield meta
