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

from enum import Enum


class StorageAdapterTypes(Enum):
    """Enum for Storage adapters."""

    HDF5StorageAdapter = "HDF5StorageAdapter"


class HDF5BuiltinCompressionStringOptions(Enum):
    """Enum for HDF5 builtin compression strings."""

    none = "none"
    gzip = "gzip"
    szip = "szip"
    lzf = "lzf"


class HDF5ChunksOptions(Enum):
    """Enum for HDF5 chunk options."""

    manual = "manual"
    true = "true"
