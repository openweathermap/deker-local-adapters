import pytest

from deker.arrays import VArray
from deker.subset import VSubset


@pytest.fixture()
def varray_full_subset(inserted_varray: VArray) -> VSubset:
    return inserted_varray[:]
