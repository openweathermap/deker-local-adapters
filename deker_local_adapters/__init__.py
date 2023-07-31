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

# nopycln: file
from .array_adapter import LocalArrayAdapter
from .collection_adapter import LocalCollectionAdapter
from .factory import AdaptersFactory, storage_adapter_factory
from .storage_adapters import (
    HDF5CompressionOpts,
    HDF5Options,
    HDF5StorageAdapter,
    HDF5BuiltinCompressionStringOptions,
    HDF5ChunksOptions
)
from .varray_adapter import LocalVArrayAdapter
