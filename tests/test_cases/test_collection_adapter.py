import os
import shutil

from pathlib import Path
from uuid import uuid4

import pytest

from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerMemoryError
from deker.schemas import ArraySchema, DimensionSchema

from tests.parameters.common import random_string

from deker_local_adapters import AdaptersFactory, LocalCollectionAdapter
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter


@pytest.mark.asyncio()
class TestCollectionAdapter:
    """Class for testing local collection adapter."""

    def test_collection_adapter_create_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        root_path: Path,
        array_schema: ArraySchema,
        factory: AdaptersFactory,
    ):
        """Tests if adapter creates array_collection in DB.

        :param collection_adapter: CollectionAdapter
        :param root_path: Path to collections directory
        :param array_schema: ArraySchema instance
        """
        name = random_string()
        collection_adapter.create(
            Collection(
                name=name,
                schema=array_schema,
                adapter=collection_adapter,
                factory=factory,
                storage_adapter=HDF5StorageAdapter,
            )
        )
        assert root_path.joinpath(factory.ctx.config.collections_directory).joinpath(name).exists()

    def test_collection_adapter_create_collection_memory_error(
        self,
        collection_adapter: LocalCollectionAdapter,
        factory: AdaptersFactory,
    ):
        """Tests if adapter raises memory error on collection creation.

        :param collection_adapter: CollectionAdapter
        :param factory: AdaptersFactory
        """
        collection_adapter.ctx.config.memory_limit = 100
        schema = ArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=10000),
                DimensionSchema(name="y", size=10000),
            ],
            dtype=float,
        )
        col_name = "memory_excess_adapter"
        with pytest.raises(DekerMemoryError):
            collection_adapter.create(
                Collection(
                    name=col_name,
                    schema=schema,
                    adapter=collection_adapter,
                    factory=factory,
                    storage_adapter=HDF5StorageAdapter,
                )
            )

    @pytest.mark.parametrize(
        ("splitter", "expected"),
        [
            ({"vgrid": [1, 3]}, (3, 1)),
            ({"arrays_shape": [1, 3]}, (3, 1)),
            ({"vgrid": [3, 1]}, (1, 3)),
            ({"arrays_shape": [3, 1]}, (1, 3)),
        ],
    )
    def test_create_from_dict_varray_collection(
        self, client: Client, varray_collection: Collection, splitter: dict, expected: tuple
    ):
        """Check if creation from dict is correct."""
        schema = {
            "metadata_version": "0.2",
            "name": "varray_from_dict_test_collection",
            "type": "varray",
            "schema": {
                "dtype": "int",
                "fill_value": -1,
                "attributes": [
                    {"name": "primary_attr_1", "dtype": "string", "primary": True},
                    {"name": "custom_attr_1", "dtype": "datetime", "primary": False},
                ],
                "dimensions": [
                    {
                        "type": "generic",
                        "name": "dimension_1",
                        "size": 3,
                        "labels": ["label_1", "label_2", "label_3"],
                    },
                    {
                        "type": "time",
                        "name": "dimension_2",
                        "size": 3,
                        "start_value": "$custom_attr_1",
                        "step": {"days": 0, "seconds": 14400, "microseconds": 0},
                    },
                ],
            },
            "options": {
                "chunks": {"mode": "manual", "size": [1, 3]},
                "compression": {"compression": "gzip", "options": [9]},
            },
        }
        col_from_dict = None

        try:
            schema["schema"].update(**splitter)
            col_from_dict = client.collection_from_dict(schema)
            assert col_from_dict
            assert col_from_dict.name == schema["name"]
            if list(splitter.keys())[0] == "vgrid":
                other_splitter = "arrays_shape"
            else:
                other_splitter = "vgrid"
            assert getattr(col_from_dict.varray_schema, other_splitter) == expected
        finally:
            if col_from_dict:
                col_from_dict.delete()

    def test_create_from_other_varray_collection_dict(self, client: Client, varray_collection: Collection):
        """Check if creation from dict is correct."""
        col_from_dict = None

        new_col_dict = varray_collection.as_dict
        new_col_dict["name"] = str(uuid4())
        try:
            col_from_dict = client.collection_from_dict(new_col_dict)
            # Compare through dicts
            col_dict = col_from_dict.as_dict

            del col_dict["name"]
            del new_col_dict["name"]

            assert col_dict == new_col_dict
        finally:
            if col_from_dict:
                col_from_dict.delete()

    def test_collection_adapter_deletes_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        array_collection: Collection,
        root_path: Path,
    ):
        """Tests if array_collection adapter deletes array_collection from db properly.

        :param collection_adapter: array_collection adapter
        :param array_collection: Pre created array_collection
        :param root_path: Path to collections directory
        """
        collection_adapter.delete(array_collection)
        assert not os.path.exists(array_collection.path)
        assert not os.path.exists(array_collection.path.parent / (array_collection.name + ".lock"))

    def test_collection_adapter_get_collection(
        self,
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
    ):
        """Tests if array_collection adapter fetch array_collection from db correctly.

        :param array_collection: Pre created array_collection
        :param collection_adapter: array_collection adapter
        """
        schema = collection_adapter.read(name=array_collection.name)
        assert schema

    def test_collection_adapter_clear(
        self,
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
        root_path: Path,
    ):
        """Tests clearing data directory.

        :param array_collection: Pre created collection
        :param collection_adapter: collection adapter
        :param root_path: Path to collections directory
        """
        collection_adapter.clear(array_collection)

        collection_dir = None
        for child in root_path.joinpath(collection_adapter.ctx.config.collections_directory).iterdir():
            if child.name == array_collection.name:
                collection_dir = child

        assert collection_dir is not None
        collection_dir = os.listdir(collection_dir)
        assert f"{array_collection.name}.json" in collection_dir

    def test_collection_adapter_iter(self, client: Client, array_schema: ArraySchema, root_path: Path, ctx):
        for root, dirs, files in os.walk(root_path / ctx.config.collections_directory):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

        names = {random_string() for _ in range(10)}
        collections = [client.create_collection(name, array_schema) for name in names]
        adapter: LocalCollectionAdapter = collections[0]._Collection__adapter  # type: ignore[attr-defined]
        coll_names = {coll["name"] for coll in adapter}
        assert names.issubset(coll_names)
        assert names - coll_names == set()


if __name__ == "__main__":
    pytest.main()
