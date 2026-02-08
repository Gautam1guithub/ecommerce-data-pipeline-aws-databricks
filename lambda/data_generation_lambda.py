# Lambda code for generating the ecommerce data

import json
import random
import boto3
import csv
from io import StringIO
from datetime import datetime, timedelta

s3_client = boto3.client("s3")

BUCKET_NAME = "glue-raw-bucket93"
S3_KEY = "ecommerce/raw/ecommerce_data_5000.csv"

def lambda_handler(event, context):

    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Beauty", "Sports"]
    payment_methods = ["Credit Card", "Debit Card", "UPI", "Net Banking", "COD"]
    order_statuses = ["Delivered", "Cancelled", "Returned", "Pending"]
    cities = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Pune"]

    start_date = datetime(2023, 1, 1)

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    # Header
    writer.writerow([
        "order_id", "order_date", "customer_id", "product_id",
        "category", "price", "quantity", "discount",
        "payment_method", "order_status", "total_amount", "city"
    ])

    for i in range(1, 5001):
        order_date = start_date + timedelta(days=random.randint(0, 365))
        price = round(random.uniform(100, 5000), 2)
        quantity = random.randint(1, 5)
        discount = round(random.uniform(0, 30), 2)
        total_amount = round((price * quantity) - discount, 2)

        writer.writerow([
            f"ORD{i:06d}",
            order_date.date(),
            f"CUST{random.randint(1000, 9999)}",
            f"PROD{random.randint(100, 999)}",
            random.choice(categories),
            price,
            quantity,
            discount,
            random.choice(payment_methods),
            random.choice(order_statuses),
            total_amount,
            random.choice(cities)
        ])

    # Upload to S3
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=S3_KEY,
        Body=csv_buffer.getvalue()
    )
    return {
        "statusCode": 200,
        "body": json.dumps("✅ 5000 rows of e-commerce data uploaded to S3 successfully")
    }
