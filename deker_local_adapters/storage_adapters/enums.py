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
