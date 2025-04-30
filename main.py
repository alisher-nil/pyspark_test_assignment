from pyspark.sql import SparkSession

from src.product_category_mapper import get_product_category_pairs
from src.utils import create_test_data


def main():
    # Initialize Spark session
    with SparkSession.builder.getOrCreate() as spark_session:
        # Sample data creation
        products_df, categories_df, product_category_df = create_test_data(
            spark_session
        )

        # Get the product-category pairs
        result = get_product_category_pairs(
            products_df,
            categories_df,
            product_category_df,
        )
        # Show results
        print("Product-Category Relationships:")
        result.show(truncate=False)

        print("\nProducts without categories:")
        result.filter(result.category.isNull()).show(truncate=False)


if __name__ == "__main__":
    main()
