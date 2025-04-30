from pyspark.sql import DataFrame


def get_product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_category_df: DataFrame,
) -> DataFrame:
    """
    Returns a DataFrame containing:
    1. All "Product Name - Category Name" pairs.
    2. All product names that have no categories.

    Args:
        products_df: DataFrame with product information
        (must have "id" and "name")
        categories_df: DataFrame with category information
        (must have "id" and "name")
        product_category_df: DataFrame with product-category relationships
        (must have "product_id" and "category_id")
    Returns:
        DataFrame with columns: "product_name", "category_name"
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
