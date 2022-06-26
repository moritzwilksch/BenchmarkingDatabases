#%%
import json
import os

import polars as pl
from pymongo import MongoClient

user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
passwd = os.getenv("MONGO_INITDB_ROOT_PASSWORD")

client = MongoClient(f"mongodb://{user}:{passwd}@localhost:27017", authsource="admin")
db = client["benchmarking"]
collection = db["testcollection"]

#%%
cursor = collection.find({}, {"_id": 0}, limit=10)
df = pl.from_records(
    [{**rec} | {"values": json.dumps(rec.get("values"))} for rec in cursor]
)

#%%
df.with_columns(
    [
        pl.col("values").str.json_path_match("$.*.end").alias("end"),
        pl.col("values").str.json_path_match("$.*.val").alias("val"),
        pl.col("values").str.json_path_match("$.*.frame").alias("frame"),
    ]
).drop("values")
