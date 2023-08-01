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

from pathlib import Path
from typing import TYPE_CHECKING, Optional, Type

from deker.ABC import BaseCollectionAdapter
from deker.ABC.base_factory import BaseAdaptersFactory
from deker.tools.decorators import check_ctx_state

from deker_local_adapters.errors import DekerStorageError
from deker_local_adapters.array_adapter import LocalArrayAdapter
from deker_local_adapters.collection_adapter import LocalCollectionAdapter
from deker_local_adapters.storage_adapters import HDF5StorageAdapter
from deker_local_adapters.varray_adapter import LocalVArrayAdapter
from deker_local_adapters.storage_adapters.enums import StorageAdapterTypes

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter, BaseStorageAdapter
    from deker.ABC.base_collection import BaseCollectionOptions


class AdaptersFactory(BaseAdaptersFactory):
    """Adapters factory for Collections, Arrays and VArrays."""

    uri_schemes = ("file",)

    def close(self) -> None:
        """Close factory and its resources."""
        super().close()

    @check_ctx_state
    def get_array_adapter(
        self,
        collection_path: Path,
        storage_adapter: Type["BaseStorageAdapter"],
        collection_options: Optional["BaseCollectionOptions"] = None,
    ) -> "BaseArrayAdapter":
        """Create ArrayAdapter instance.

        :param collection_path: path to collection
        :param storage_adapter: storage adapter implementation
        :param collection_options: chunking and compression options
        """
        return LocalArrayAdapter(
            collection_path, self.ctx, storage_adapter, self.executor, collection_options
        )

    @check_ctx_state
    def get_varray_adapter(
        self, collection_path: Path, storage_adapter: Type["BaseStorageAdapter"]
    ) -> "BaseVArrayAdapter":
        """Create VArrayAdapter instance.

        :param collection_path: path to collection
        :param storage_adapter: storage adapter implementation
        """
        return LocalVArrayAdapter(collection_path, self.ctx, storage_adapter, self.executor)

    @check_ctx_state
    def get_collection_adapter(self) -> "BaseCollectionAdapter":
        """Create collection adapter."""
        return LocalCollectionAdapter(self.ctx)


def storage_adapter_factory(storage_adapter: Optional[str] = None) -> Type["BaseStorageAdapter"]:
    """Return class of storage adapter based on the string input.

    :param storage_adapter: string with storage adapter name.
    """
    # Currently we support only hdf5
    if storage_adapter and storage_adapter != StorageAdapterTypes.HDF5StorageAdapter.value:
        raise DekerStorageError(f"There is no adapter for {storage_adapter}")

    return HDF5StorageAdapter
