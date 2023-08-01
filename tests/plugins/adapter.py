from concurrent.futures import ThreadPoolExecutor
from typing import Type

import pytest

from deker.ABC import BaseStorageAdapter
from deker.collection import Collection
from deker.ctx import CTX

from deker_local_adapters import (
    HDF5StorageAdapter,
    LocalArrayAdapter,
    LocalCollectionAdapter,
    LocalVArrayAdapter,
)


@pytest.fixture()
def collection_adapter(ctx) -> LocalCollectionAdapter:
    """Creates a PathCollection adapter.

    :param ctx: client context
    """
    adapter = LocalCollectionAdapter(ctx)
    return adapter


@pytest.fixture()
def storage_adapter() -> Type[BaseStorageAdapter]:
    return HDF5StorageAdapter


@pytest.fixture()
def local_array_adapter(array_collection: Collection, ctx: CTX) -> LocalArrayAdapter:
    """Instance of LocalArrayAdapter.

    Instance would be different from the one that appears in Array.
    """
    return LocalArrayAdapter(
        collection_path=array_collection.path, collection_options=array_collection.options, ctx=ctx, storage_adapter=HDF5StorageAdapter, executor=ThreadPoolExecutor(workers=1)
    )


@pytest.fixture()
def local_varray_adapter(varray_collection: Collection, ctx: CTX) -> LocalVArrayAdapter:
    return LocalVArrayAdapter(
        collection_path=varray_collection.path, ctx=ctx, executor=ThreadPoolExecutor(max_workers=1), storage_adapter=HDF5StorageAdapter
    )
