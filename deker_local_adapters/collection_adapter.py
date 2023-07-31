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
import shutil

from pathlib import Path
from typing import TYPE_CHECKING, Generator, Optional

from deker.ABC.base_adapters import BaseCollectionAdapter
from deker.ctx import CTX
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from deker.locks import CollectionLock
from deker.tools import check_memory
from deker.tools.decorators import check_ctx_state


if TYPE_CHECKING:
    from deker.collection import Collection


class LocalCollectionAdapter(BaseCollectionAdapter):
    """Adapter which stores info in the file system."""

    def __init__(self, ctx: CTX) -> None:
        super().__init__(ctx)
        self.metadata_version = "0.2"
        self.file_ext = ".json"
        self.collections_resource.mkdir(parents=True, exist_ok=True)

    def __create_path(self, name: str) -> Optional[Path]:
        """Create Collection path in the file system.

        :param name: Collection name
        """
        path = self.collections_resource / name
        try:
            os.makedirs(path)
            return path
        except FileExistsError:
            raise DekerCollectionAlreadyExistsError(f"Collection {name} already exists")

    def __create(self, coll_name: str, data: dict, coll_path: Optional[Path]) -> None:
        if coll_path and coll_path.exists():
            path = coll_path / (coll_name + self.file_ext)
            with open(path, mode="w") as f:
                f.write(json.dumps(data))

    @check_ctx_state
    @CollectionLock()
    def create(self, collection: "Collection") -> None:
        """Add collection to the storage and create file structure.

        :param collection: Collection to be created
        """
        try:
            schema = collection.array_schema
            shape = schema.arrays_shape if hasattr(schema, "arrays_shape") else schema.shape
            check_memory(shape, schema.dtype, self.ctx.config.memory_limit)
            coll_path = self.__create_path(collection.name)
            prepared = collection.as_dict
            self.__create(coll_name=collection.name, data=prepared, coll_path=coll_path)
        except Exception as e:
            logging.exception(e)
            raise e

    @check_ctx_state
    def read(self, name: str) -> dict:
        """Read collection metadata.

        :param name: Collection name
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
        """Delete collection.

        :param collection: Collection to delete
        """
        try:
            shutil.rmtree(collection.path, onerror=FileNotFoundError)
            Path.unlink(collection.path.parent / (collection.name + ".lock"), missing_ok=True)
        except Exception as e:
            logging.exception(e)
            raise e

    @check_ctx_state
    @CollectionLock()
    def clear(self, collection: "Collection") -> None:
        """Clear collection directory.

        :param collection: Collection to be cleared
        """
        try:
            for root, dirs, files in os.walk(collection.path):
                for f in files:
                    save = False
                    if f == collection.name + self.file_ext:
                        save = True
                    if not save:
                        os.remove(os.path.join(root, f))
                for d in dirs:
                    shutil.rmtree(os.path.join(root, d))
        except Exception as e:
            logging.exception(e)
            raise e

    @property
    def collections_resource(self) -> Path:
        """Return a path to the directory with collections."""
        return Path(self.uri.path) / self.ctx.config.collections_directory

    def is_deleted(self, collection: "Collection") -> bool:
        """Check if collection was deleted.

        :param collection: Collection to check
        """
        # If it does not exist, then it's not created or deleted
        return not collection.path.exists()

    @check_ctx_state
    def __iter__(self) -> Generator[dict, None, None]:
        for directory in os.scandir(self.collections_resource):  # type: ignore
            if os.path.isdir(directory):
                meta = self.read(directory.name)
                yield meta
