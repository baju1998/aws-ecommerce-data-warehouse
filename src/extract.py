from pathlib import Path
import pandas as pd
from validate import validate_data


# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Raw data location
RAW_DATA_DIR = BASE_DIR / "data" / "raw"


# Dataset configuration
DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
}


def extract_data():
    """
    Extract CSV datasets into Pandas DataFrames.
    """

    extracted_data = {}

    for name, filename in DATASETS.items():

        file_path = RAW_DATA_DIR / filename

        if not file_path.exists():
            raise FileNotFoundError(
                f"Missing dataset: {file_path}"
            )

        df = pd.read_csv(file_path)

        extracted_data[name] = df

        print(
            f"Loaded {name}: "
            f"{df.shape[0]} rows, "
            f"{df.shape[1]} columns"
        )

    return extracted_data


if __name__ == "__main__":

    datasets = extract_data()

    validate_data(datasets)

    print("\nExtraction and validation completed successfully!")