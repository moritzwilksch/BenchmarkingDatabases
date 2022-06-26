import datetime
import json
import os
import random
import time
from http.client import ImproperConnectionState

from joblib import delayed, parallel
from pymongo import MongoClient
from rich.console import Console
from rich.markdown import Markdown
from rich.table import Table

with open("data/dump.json", "r") as f:
    data = json.load(f)


c = Console()


def printinfo(text: str) -> None:
    c.print(f"[green][INFO][/] {text}")


def timeit_decorator(func: callable):
    def wrapper(self=None):
        tic = time.perf_counter()
        func(self)
        tac = time.perf_counter()
        return tac - tic

    return wrapper


class MongoBenchmarker:
    def __init__(self) -> None:
        user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        passwd = os.getenv("MONGO_INITDB_ROOT_PASSWORD")

        self.client = MongoClient(
            f"mongodb://{user}:{passwd}@localhost:27017", authsource="admin"
        )
        self.db = self.client["benchmarking"]
        self.collection = self.db["testcollection"]
        self.collection.drop()

        # Just an experiment: Indices
        # self.collection = self.db["testcollection"]
        # self.collection.create_index([("ticker", 1), ("name", 1)])

    @timeit_decorator
    def insert_all(self) -> None:
        self.collection.insert_many(data)

    @timeit_decorator
    def insert_separately(self, N: int = 10_000) -> None:
        for doc in data[:N]:
            if "_id" in doc.keys():
                del doc["_id"]

            self.collection.insert_one(doc)

    # @timeit_decorator
    # def read_all_names(self) -> None:
    #     user_names = []
    #     for user in self.collection.find():
    #         user_names.append(user.get("name"))
    #     printinfo(f"read_all_names: {len(user_names)} users found")

    @timeit_decorator
    def query1(self) -> None:
        results = self.collection.find({"ticker": "ABBV", "name": "Revenues"})
        printinfo(f"query1: {len(list(results))} results found")

    @timeit_decorator
    def query2(self) -> None:
        results = self.collection.find({"ticker": "ABBV", "name": {"$regex": "Rev"}})
        printinfo(f"query2: {len(list(results))} results found")

    def run(self):
        c = Console()
        c.print()
        table = Table(title="MongoDB Benchmark")
        table.add_column("Benchmark", justify="left")
        table.add_column("Time", justify="center", style="cyan bold")

        table.add_row("insert_all", f"{self.insert_all():.3f}")
        table.add_row("insert_separately", f"{self.insert_separately():.3f}")
        # table.add_row("read_all_names", f"{self.read_all_names():.3f}")
        table.add_row("query1", f"{self.query1():.3f}")
        table.add_row("query2", f"{self.query2():.3f}")
        c.print(table)


if __name__ == "__main__":
    bm = MongoBenchmarker()
    bm.run()
