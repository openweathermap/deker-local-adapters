import string

from datetime import datetime, timedelta, timezone

import pytest

from deker.ABC.base_schemas import BaseDimensionSchema
from deker.schemas import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema, VArraySchema


@pytest.fixture
def dimensions() -> list[BaseDimensionSchema]:
    """Dimensions for array schema."""
    return [
        DimensionSchema(name="x", size=10),
        DimensionSchema(name="layers", size=10, labels=list(string.ascii_lowercase)[:10]),
        TimeDimensionSchema(
            name="forecast_dt",
            size=10,
            start_value=datetime.fromtimestamp(1663200000, tz=timezone.utc),
            step=timedelta(3),
        ),
    ]


@pytest.fixture
def dimensions_with_string_time() -> list[BaseDimensionSchema]:
    """Dimensions for array schema."""
    return [
        DimensionSchema(name="x", size=10),
        DimensionSchema(name="layers", size=10, labels=list(string.ascii_lowercase)[:10]),
        TimeDimensionSchema(
            name="forecast_dt",
            size=10,
            start_value="$time_attr_name",
            step=timedelta(hours=3),
        ),
    ]


@pytest.fixture
def array_schema(dimensions: list[DimensionSchema]) -> ArraySchema:
    """Creates a simple array schema.

    :param dimensions: Dimensions of array schema
    """
    return ArraySchema(dtype=float, dimensions=dimensions)


@pytest.fixture
def array_schema_with_attributes(dimensions_with_string_time: list[DimensionSchema]) -> ArraySchema:
    """Creates an array schema with extra primary attributes.

    :param dimensions_with_string_time: Dimensions of array schema
    """
    return ArraySchema(
        dtype=float,
        dimensions=dimensions_with_string_time,
        attributes=[
            AttributeSchema(name="primary_attribute", dtype=int, primary=True),
            AttributeSchema(name="custom_attribute", dtype=float, primary=False),
            AttributeSchema(name="time_attr_name", dtype=datetime, primary=False),
        ],
    )


@pytest.fixture
def varray_schema(dimensions: list[DimensionSchema]) -> VArraySchema:
    """Creates a VArray schema."""
    return VArraySchema(dimensions=dimensions, dtype=float, vgrid=[2] * len(dimensions))


@pytest.fixture
def varray_schema_with_attributes(
    dimensions_with_string_time: list[DimensionSchema],
) -> VArraySchema:
    """Creates an array schema with extra primary attributes.

    :param dimensions_with_string_time: Dimensions of array schema
    """
    return VArraySchema(
        dtype=float,
        dimensions=dimensions_with_string_time,
        attributes=[
            AttributeSchema(name="primary_attribute", dtype=int, primary=True),
            AttributeSchema(name="custom_attribute", dtype=float, primary=False),
            AttributeSchema(name="time_attr_name", dtype=datetime, primary=False),
        ],
        vgrid=[2] * len(dimensions_with_string_time),
    )
