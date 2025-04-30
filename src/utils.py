from pyspark.sql import DataFrame, SparkSession


def create_test_data(
    spark_session: SparkSession,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    This function generates sample data for products, categories,
    and product-category relationships.

    Args:
        spark_session: Spark session object.
    Returns:
        tuple: A tuple containing three DataFrames:
            - products_df ("id", "name")
            - categories_df ("id", "name")
            - product_category_df ("product_id", "category_id")
    """
    products_df = create_product_df(spark_session)
    categories_df = create_category_df(spark_session)
    product_category_df = create_product_category_df(spark_session)

    return products_df, categories_df, product_category_df


def create_product_df(spark_session: SparkSession) -> DataFrame:
    products_df = spark_session.createDataFrame(
        [
            (1, "Laptop"),
            (2, "Smartphone"),
            (3, "Headphones"),
            (4, "Keyboard"),
            (5, "Mouse"),
            (6, "Product without category"),
            (7, "Another product without category"),
        ],
        ["id", "name"],
    )
    return products_df


def create_category_df(spark_session: SparkSession) -> DataFrame:
    categories_df = spark_session.createDataFrame(
        [
            (1, "Electronics"),
            (2, "Computers"),
            (3, "Accessories"),
            (4, "Mobile"),
            (5, "Category without products"),
        ],
        ["id", "name"],
    )
    return categories_df


def create_product_category_df(spark_session: SparkSession) -> DataFrame:
    product_category_df = spark_session.createDataFrame(
        [
            (1, 1),  # Laptop - Electronics
            (1, 2),  # Laptop - Computers
            (2, 1),  # Smartphone - Electronics
            (2, 4),  # Smartphone - Mobile
            (3, 3),  # Headphones - Accessories
            (4, 2),  # Keyboard - Computers
            (4, 3),  # Keyboard - Accessories
            (5, 3),  # Mouse - Accessories
        ],
        ["product_id", "category_id"],
    )
    return product_category_df
