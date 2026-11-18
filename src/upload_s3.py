import os
import boto3

from pathlib import Path
from extract import extract_data
from validate import validate_data


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


def upload_to_s3(datasets):

    # Connect to AWS using the SSO profile
    session = boto3.Session(profile_name="ecommerce-dev")

    # Create the S3 client
    s3 = session.client("s3")

    filenames = {
        "customers": "olist_customers_dataset.csv",
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "products": "olist_products_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
    }

    for name in datasets.keys():

        filename = filenames[name]

        local_path = RAW_DATA_DIR / filename

        s3_key = f"raw/olist/{filename}"

        print(f"Uploading {name}...")

        s3.upload_file(
            str(local_path),
            BUCKET_NAME,
            s3_key
        )

        print(f"Uploaded: s3://{BUCKET_NAME}/{s3_key}")


if __name__ == "__main__":

    datasets = extract_data()

    validate_data(datasets)

    upload_to_s3(datasets)

    print("All datasets uploaded successfully!")