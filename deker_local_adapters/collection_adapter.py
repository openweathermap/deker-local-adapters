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
import shutil

from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

from deker.ABC.base_adapters import BaseCollectionAdapter
from deker.ctx import CTX
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from deker.locks import CollectionLock
from deker.tools import check_memory
from deker.tools.decorators import check_ctx_state

if TYPE_CHECKING:
    from deker.collection import Collection


class LocalCollectionAdapter(BaseCollectionAdapter[Path]):
    """Adapter of the collections of a local storage.

    A collection is a directory under the collections directory of the storage, named by the collection,
    with a json file of the same name holding its metadata; ``<collection>.lock`` next to the directory is
    held while the collection is created or cleared and stays as its marker.
    """

    def __init__(self, ctx: CTX) -> None:
        """LocalCollectionAdapter constructor: creates the collections directory of the storage if it is missing.

        :param ctx: context of the client
        """
        super().__init__(ctx)
        self.metadata_version = "0.2"
        self.file_ext = ".json"
        self.collections_resource.mkdir(parents=True, exist_ok=True)

    def __create_path(self, name: str) -> Path | None:
        """Create Collection path in the file system.

        :param name: Collection name
        :returns: path to the collection directory
        :raises DekerCollectionAlreadyExistsError: if the directory exists
        """
        path = self.collections_resource / name
        try:
            os.makedirs(path)
            return path
        except FileExistsError:
            raise DekerCollectionAlreadyExistsError(f"Collection {name} already exists")

    def __create(self, coll_name: str, data: dict, coll_path: Path | None) -> None:
        """Write the metadata file of the collection into its directory.

        :param coll_name: Collection name
        :param data: metadata of the collection
        :param coll_path: directory of the collection
        """
        if coll_path and coll_path.exists():
            path = coll_path / (coll_name + self.file_ext)
            with open(path, mode="w") as f:
                f.write(json.dumps(data))

    @check_ctx_state
    @CollectionLock()
    def create(self, collection: "Collection") -> None:
        """Add collection to the storage and create file structure, holding the collection lock.

        :param collection: Collection to be created
        :raises DekerMemoryError: if one array of the collection would not fit the memory limit, unless the
          check is skipped by the client
        :raises DekerCollectionAlreadyExistsError: if a collection of the name exists
        """
        try:
            schema = collection.array_schema
            shape = schema.arrays_shape if hasattr(schema, "arrays_shape") else schema.shape
            if not self.ctx.config.skip_collection_create_memory_check:
                check_memory(shape, schema.dtype, self.ctx.config.memory_limit)
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
        raise DekerCollectionNotExistsError(f"{name} collection doesn't exist")

    @check_ctx_state
    def delete(self, collection: "Collection") -> None:
        """Delete the directory of the collection with everything in it, and its lock file.

        A directory that is gone already is not an error; any other failure to remove it is raised.

        :param collection: Collection to delete
        """
        try:
            try:
                shutil.rmtree(self.collections_resource / collection.name)
            except FileNotFoundError:
                pass
            Path.unlink(self.collections_resource / (collection.name + ".lock"), missing_ok=True)
        except Exception as e:
            self.logger.exception(e)
            raise e

    @check_ctx_state
    @CollectionLock()
    def clear(self, collection: "Collection") -> None:
        """Remove everything from the directory of the collection but its metadata file, holding the collection lock.

        :param collection: Collection to be cleared
        """
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

    @check_ctx_state
    def is_deleted(self, collection: "Collection") -> bool:
        """Check if collection was deleted.

        :param collection: Collection to check
        :returns: ``True`` if the collection directory no longer exists
        """
        # If it does not exist, then it's not created or deleted
        return not (self.collections_resource / collection.name).exists()

    @check_ctx_state
    def __iter__(self) -> Generator[dict, None, None]:
        """Iterate over the metadata of the collections: every directory under the collections directory.

        :yields: the metadata of every collection
        """
        for directory in os.scandir(self.collections_resource):
            if os.path.isdir(directory):
                meta = self.read(directory.name)
                yield meta
