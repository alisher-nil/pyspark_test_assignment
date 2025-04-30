def test_product_categories_count(prod_cats_pairs):
    """Test products with categories are properly mapped"""
    prod1_cats_count = prod_cats_pairs.filter(
        prod_cats_pairs.product == "Product1"
    ).count()
    assert prod1_cats_count == 2


def test_category_products_count(prod_cats_pairs):
    cat1_prods_count = prod_cats_pairs.filter(
        prod_cats_pairs.category == "Category1"
    ).count()
    assert cat1_prods_count == 2


def test_products_without_categories(prod_cats_pairs):
    """Test products without categories appear with null category"""

    # NoCategory product should appear with null category
    no_category_product = prod_cats_pairs.filter(
        prod_cats_pairs.product == "NoCategory"
    ).collect()
    assert len(no_category_product) == 1
    assert no_category_product[0].category is None
