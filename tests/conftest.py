from typing import Generator

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.product_category_mapper import get_product_category_pairs


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    """Create a SparkSession for testing"""
    with (
        SparkSession.builder.master("local[*]")
        .appName("tests")
        .getOrCreate() as spark
    ):
        yield spark


@pytest.fixture(scope="module")
def test_data(spark) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Create test data fixtures"""
    products_df = spark.createDataFrame(
        [
            (1, "Product1"),
            (2, "Product2"),
            (3, "NoCategory"),
        ],
        ["id", "name"],
    )

    categories_df = spark.createDataFrame(
        [
            (1, "Category1"),
            (2, "Category2"),
            (3, "EmptyCategory"),
        ],
        ["id", "name"],
    )

    product_category_df = spark.createDataFrame(
        [
            (1, 1),  # Product1 -> Category1
            (1, 2),  # Product1 -> Category2
            (2, 1),  # Product2 -> Category1
        ],
        ["product_id", "category_id"],
    )
    return products_df, categories_df, product_category_df


@pytest.fixture(scope="module")
def prod_cats_pairs(test_data) -> DataFrame:
    """Create a fixture for the query result"""
    products_df, categories_df, product_category_df = test_data
    return get_product_category_pairs(
        products_df, categories_df, product_category_df
    )
