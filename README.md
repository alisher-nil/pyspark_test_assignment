# Product-Category Mapping in PySpark Test Assignment

This repository contains a solution for mapping products to categories using PySpark DataFrames.

## Problem Statement

Given three DataFrames representing:
1. Products (with `id` and `name`)
2. Categories (with `id` and `name`)
3. Product-Category relationships (many-to-many mappings with `product_id` and `category_id` columns)

The task is to create a PySpark method that returns a single DataFrame containing:
- All "Product Name - Category Name" pairs
- All products that have no categories (with null category name)

## Solution

The core function `get_product_category_pairs()` performs:
1. A left join between products and relationship table to ensure all products are included
2. A left join with categories to get category names
3. Selection of only the product and category names for the final result

## Prerequisites
- [Java Runtime Environment](https://www.java.com/en/download/)
- Python 3.13 or higher

## Dependencies
- PySpark 3.5.0
- Pytest 8.3.5 for testing

## Installation

Clone the repository and navigate to the project directory:
```bash
git clone https://github.com/alisher-nil/pyspark_test_assignment.git
cd pyspark_test_assignment
```
Create a virtual environment, activate it and install dependencies using pip:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
or using uv:
```bash
uv sync
```

## Usage
```python
from src.product_category_mapper import get_product_category_pairs

# Create or load your DataFrames
products_df = ...
categories_df = ...
product_category_df = ...

# Get the product-category pairs
result = get_product_category_pairs(products_df, categories_df, product_category_df)
result.show()
```

## Running an example:
```bash
python3 main.py
# or
uv run main.py # if using uv
```
## Testing
To run the tests, use the following command:
```bash
pytest
```
## Author
Created by Alisher Nildibayev. For any questions, feel free to reach out at alisher.nil@gmail.com
