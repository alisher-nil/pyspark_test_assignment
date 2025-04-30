from pyspark.sql import DataFrame, SparkSession


def get_product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_category_df: DataFrame,
) -> DataFrame:
    """
    Returns a DataFrame containing:
    1. All "Product Name - Category Name" pairs
    2. All product names that have no categories

    Args:
        products_df: DataFrame with product information (must have 'id' and 'name')
        categories_df: DataFrame with category information (must have 'id' and 'name')
        product_category_df: DataFrame with product-category relationships (must have 'product_id' and 'category_id')

    Returns:
        DataFrame with columns: 'product_name', 'category_name'
    """
    # Join products with the relationship table
    products_with_relations = products_df.join(
        product_category_df,
        products_df["id"] == product_category_df["product_id"],
        how="left",
    )

    # Join with categories
    result_with_names = products_with_relations.join(
        categories_df,
        product_category_df["category_id"] == categories_df["id"],
        how="left",
    )

    # Select only product name and category name
    result = result_with_names.select(
        products_df["name"].alias("product"),
        categories_df["name"].alias("category"),
    )

    return result


def main():
    # Initialize Spark session
    with SparkSession.builder.getOrCreate() as spark_session:
        # Sample data creation
        products_df = create_product_df(spark_session)
        categories_df = create_category_df(spark_session)
        product_category_df = create_product_category_df(spark_session)

        # Get the product-category pairs
        result = get_product_category_pairs(
            products_df,
            categories_df,
            product_category_df,
        )
        result.show(truncate=False)


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


if __name__ == "__main__":
    main()
