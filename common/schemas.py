from __future__ import annotations

RAW_TLC_YELLOW_COLUMNS: dict[str, str] = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "float",
    "PULocationID": "int",
    "DOLocationID": "int",
    "RatecodeID": "int",
    "store_and_fwd_flag": "string",
    "payment_type": "int",
    "fare_amount": "float",
    "extra": "float",
    "mta_tax": "float",
    "tip_amount": "float",
    "tolls_amount": "float",
    "improvement_surcharge": "float",
    "total_amount": "float",
}

REQUIRED_TLC_MIN: list[str] = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]

TLC_URL_TEMPLATE = "https://<TLC_HOST>/trip-data/yellow_tripdata_{YYYY}-{MM}.parquet"
