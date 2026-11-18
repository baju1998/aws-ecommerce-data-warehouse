from pathlib import Path
import pandas as pd


#get the project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

#define the path to the raw data directory
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

customers = pd.read_csv(RAW_DATA_DIR / "olist_customers_dataset.csv")

orders = pd.read_csv(RAW_DATA_DIR / "olist_orders_dataset.csv")

order_items = pd.read_csv(RAW_DATA_DIR / "olist_order_items_dataset.csv")

products = pd.read_csv(RAW_DATA_DIR / "olist_products_dataset.csv")

payments = pd.read_csv(RAW_DATA_DIR / "olist_order_payments_dataset.csv")


print("Datasets loaded successfully")


print("Customers:", customers.shape)
print("Orders:", orders.shape)
print("Order Items:", order_items.shape)
print("Products:", products.shape)
print("Payments:", payments.shape)


print("Orders Previews:")
print(orders.head())

print("Orders information:")
print(orders.info())

print("\nMissing Values:")
print(orders.isnull().sum())

print("Duplicate orders IDs:")
print(orders["order_id"].duplicated().sum())

print("Order status counts:")
print(orders["order_status"].value_counts())
