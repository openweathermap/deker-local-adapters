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
import threading
import uuid

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


def move_to_trash(path: Path, trash: Path, collections: Path) -> None:
    """Move a file, a symlink or a directory to the trash of the storage in one rename, under a flat random name.

    The only code of the move to the trash. What is gone already is not an error: another operation moved it. Unless
    the trash or the collections directory of the storage is gone: that is critical.

    :param path: what to move
    :param trash: the trash directory of the storage
    :param collections: the collections directory of the storage
    """
    try:
        os.rename(path, trash / uuid.uuid4().hex)
    except FileNotFoundError:
        check_storage_directory(trash)
        check_storage_directory(collections)


class LocalCollectionAdapter(BaseCollectionAdapter[Path]):
    """Adapter of the collections of a local storage.

    A collection is a directory under the collections directory of the storage, named by the collection,
    with a json file of the same name holding its metadata. The json file is the lock of the collection while it is
    created; an empty one is a collection not created yet.
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

    @CollectionLock()
    def __write(self, collection: "Collection", *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Write the metadata file of the collection, holding the collection lock, unless it is written already.

        The lock makes the directory of the collection and its metadata file, empty, when they are not there.

        :param collection: Collection to be created
        :param timeout: seconds to wait for the lock of the collection as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client. Read by the lock decorator of the method (``CollectionLock``), not by
          its body: the lock waits for another creation of the collection no longer than that
        :param token: token of the write operation, started already; read by the lock decorator while it waits
        :raises DekerCollectionAlreadyExistsError: if the metadata file is written: the collection exists
        :raises DekerCriticalError: if the collections directory of the storage does not exist
        """
        path = self.collections_resource / collection.name / (collection.name + self.file_ext)
        try:
            if path.stat().st_size:
                raise DekerCollectionAlreadyExistsError(f"Collection {collection.name} already exists")
            with open(path, mode="w") as f:
                f.write(json.dumps(collection.as_dict))
        except FileNotFoundError:
            check_storage_directory(self.collections_resource)  # deleted under the lock: created, then deleted

    @check_ctx_state
    def create(self, collection: "Collection", *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Add collection to the storage: its directory and its metadata file, written under the collection lock.

        A deletion of the collection that comes while the lock is held takes no lock and is after the creation: the
        creation returns, and the collection does not exist.

        :param collection: Collection to be created
        :param timeout: seconds to wait for the lock of the collection as the call gave them; ``Sentinel`` -
          ``write_lock_timeout`` of the client. Passed to the method that holds the lock (``CollectionLock``): the
          lock waits for another creation of the collection no longer than that
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
            self.__write(collection, timeout=timeout, token=token)
        except Exception as e:
            self.logger.exception(e)
            raise e

    @check_ctx_state
    def read(self, name: str) -> dict:
        """Read collection metadata.

        :param name: Collection name
        :returns: collection metadata as a dict
        :raises DekerCollectionNotExistsError: if there is no collection of the name, or its metadata file is empty:
          the collection is not created yet
        """
        path = self.collections_resource / name / (name + self.file_ext)
        try:
            with open(path) as f:
                text = f.read()
        except FileNotFoundError:
            text = ""  # no file, or taken away by a deletion meanwhile
        if text:
            return json.loads(text)
        check_storage_directory(self.collections_resource)
        raise DekerCollectionNotExistsError(f"{name} collection doesn't exist")

    @check_ctx_state
    def delete(self, collection: "Collection", *, background: bool, _empty_trash: bool, token: AbstractToken) -> None:
        """Move the directory of the collection to the trash in one rename, taking no lock; then empty the trash.

        The readers and the writers of its arrays go on with their files in the trash. A directory that is gone already
        is not an error; any other failure to move it is raised.

        :param collection: Collection to delete
        :param background: empty the trash in a daemon thread and return at once
        :param _empty_trash: empty the trash after the move; ``False`` - only move
        :param token: token of the write operation
        """
        check(token)
        mark_started(token)
        move_to_trash(self.collections_resource / collection.name, self.trash_resource, self.collections_resource)
        if _empty_trash:
            self.empty_trash(background=background)

    @staticmethod
    def __remove_broken_symlinks(directory: Path) -> None:
        """Remove the symlinks under the directory that point to a missing file.

        A creator makes the file of an array before its symlink, so a symlink without its file is never an array being
        created: its file is moved away.

        :param directory: a symlinks directory of the collection; a missing one has nothing to remove
        """
        for root, _, names in os.walk(directory):
            for name in names:
                symlink = Path(root) / name
                if not symlink.exists():
                    symlink.unlink(missing_ok=True)

    @check_ctx_state
    def clear(self, collection: "Collection", *, background: bool, _empty_trash: bool, token: AbstractToken) -> None:
        """Move everything of the collection but its metadata file to the trash, taking no lock; then empty the trash.

        The directories go one by one, in one thread: the symlinks of the VArrays, the symlinks of the Arrays, the data
        of the VArrays, the data of the Arrays, then whatever else lies in the directory of the collection; a directory
        made again by a creator meanwhile stays. A creator may make its file before the data is moved and its symlink
        after the symlinks are: the symlinks left without their files are removed at the end. The readers and the
        writers of the arrays go on with their files in the trash.

        :param collection: Collection to be cleared
        :param background: empty the trash in a daemon thread and return at once
        :param _empty_trash: empty the trash after the moves; ``False`` - only move
        :param token: token of the write operation
        """
        check(token)
        mark_started(token)
        directory = self.collections_resource / collection.name
        config = self.ctx.config
        symlinks = (config.varray_symlinks_directory, config.array_symlinks_directory)
        moved = (*symlinks, config.varray_data_directory, config.array_data_directory)
        for name in moved:
            move_to_trash(directory / name, self.trash_resource, self.collections_resource)
        try:
            names = os.listdir(directory)
        except FileNotFoundError:
            check_storage_directory(self.collections_resource)
            names = []  # the collection is deleted meanwhile: nothing is left to move
        for name in names:
            if name not in moved and name != collection.name + self.file_ext:
                move_to_trash(directory / name, self.trash_resource, self.collections_resource)
        for name in symlinks:
            self.__remove_broken_symlinks(directory / name)
        if _empty_trash:
            self.empty_trash(background=background)

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
                        continue  # no collection file, or an empty one
                    yield meta
