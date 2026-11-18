import pandas as pd

DATASET_RULES = {
    "customers": {
        "required_columns": [
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state"
        ],
        "primary_key": ["customer_id"]
    },
    "orders": {
        "required_columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp"
        ],
        "primary_key": ["order_id"]
    },

    "order_items": {
        "required_columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value"
        ],
        "primary_key": ["order_id", "order_item_id"]
    },

    "products": {
        "required_columns": [
            "product_id",
            "product_category_name"
        ],
        "primary_key": ["product_id"]
    },

    "payments": {
        "required_columns": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_value"
        ],
        "primary_key": ["order_id", "payment_sequential"]
    }
}


def validate_data(datasets):

    for name, df in datasets.items():

        print(f"\nValidating {name}...")

        rules = DATASET_RULES[name]

        required_columns = rules["required_columns"]
        primary_key = rules["primary_key"]

        # Check required columns
        missing_columns = [
            col for col in required_columns
            if col not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"{name}: Missing columns {missing_columns}"
            )

        # Check null primary keys
        null_keys = df[primary_key].isnull().any(axis=1).sum()

        if null_keys > 0:
            raise ValueError(
                f"{name}: {null_keys} rows have null primary keys"
            )

        # Check duplicate primary keys
        duplicates = df.duplicated(
            subset=primary_key
        ).sum()

        if duplicates > 0:
            raise ValueError(
                f"{name}: {duplicates} duplicate primary keys"
            )

        print(f"{name}: Validation passed!")