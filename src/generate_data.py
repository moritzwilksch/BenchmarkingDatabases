import os

from pymongo import MongoClient

user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
passwd = os.getenv("MONGO_INITDB_ROOT_PASSWORD")

db = MongoClient(
    f"mongodb://{user}:{passwd}@localhost:27017/edgar", authsource="admin"
)["edgar"]
coll = db["facts"]

results = coll.find({}, {"_id": 0}, limit=100_000)

import json

with open("data/dump.json", "w") as f:
    json.dump(list(results), f)
