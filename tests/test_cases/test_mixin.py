import pytest
import os
from deker_local_adapters.errors import DekerBrokenSymlinkError
from deker_local_adapters.varray_adapter import LocalVArrayAdapter 
from deker.tools.path import get_symlink_path

@pytest.mark.parametrize("adapter_", ("local_array_adapter", "local_varray_adapter"))
def test_broken_symlink(adapter_: str, request, local_array_adapter, local_varray_adapter):
    adapter = request.getfixturevalue(adapter_)
    if isinstance(adapter, LocalVArrayAdapter):
        collection = request.getfixturevalue("varray_collection")
        array = request.getfixturevalue("varray_with_attributes")
        schema = request.getfixturevalue("varray_schema_with_attributes")
        symlink_dir = "varray_symlinks"
    else:
        collection = request.getfixturevalue("array_collection")
        array = request.getfixturevalue("array_with_attributes")
        schema = request.getfixturevalue("array_schema_with_attributes")
        symlink_dir = "array_symlinks"
        
    symlinks_path = get_symlink_path(
        path_to_symlink_dir=collection.path / symlink_dir,  # type: ignore[operator]
        primary_attributes_schema=schema.primary_attributes,
        primary_attributes=array.primary_attributes,
    )
    symlinks_path.mkdir(parents=True, exist_ok=True)
    path_for_file = symlinks_path / "test.txt"
    
    path_for_file.write_text("foo")

    os.symlink(path_for_file, symlinks_path / "symlink")
    path_for_file.unlink()
    with pytest.raises(DekerBrokenSymlinkError):
        adapter.get_by_primary_attributes(
            array.primary_attributes,
            schema,
            collection,
            local_array_adapter,
            local_varray_adapter if isinstance(adapter, LocalVArrayAdapter) else None
        )

