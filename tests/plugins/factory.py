import pytest

from deker_local_adapters.factory import AdaptersFactory


@pytest.fixture()
def factory(ctx, uri) -> AdaptersFactory:
    """Creates ArraysAdaptersFactory instance."""
    return AdaptersFactory(ctx, uri)
