from pymongoarrow.api import Schema
from bson import ObjectId
from pymongo import MongoClient
from pymongoarrow.api import find_arrow_all
import polars as pl
import pandas as pd
from pyarrow import Table

schema = Schema({"_id": ObjectId, "rawTitle": str, "rawText": str})
client = MongoClient("mongodb://mongo:mongo@localhost:27020/laws?authSource=admin")
db = client.get_database("laws")

arrow_table: Table = find_arrow_all(db.law, {}, schema=schema)
arrow_table.to_pandas()