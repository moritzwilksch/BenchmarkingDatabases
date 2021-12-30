import random
import datetime
import time
from rich.console import Console
from rich.markdown import Markdown
from pymongo import MongoClient
from rich.table import Table

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
    def __init__(self, N=10_000) -> None:
        self.client = MongoClient("localhost", 27017)
        self.db = self.client["test"]
        self.collection = self.db["testcollection"]
        self.collection.drop()
        self.collection = self.db["testcollection"]
        self.N = N
        self.letters = "abcdefghijklmnopqrstuvwxyz"

    @timeit_decorator
    def insert_all(self) -> None:
        users = []
        for _ in range(self.N):
            name = "".join(random.sample(self.letters, random.randint(4, 25)))
            dob = datetime.datetime.now()
            users.append({"name": name, "dob": dob})
        self.collection.insert_many(users)

    @timeit_decorator
    def insert_separately(self) -> None:
        for _ in range(self.N):
            name = "".join(random.sample(self.letters, random.randint(4, 25)))
            dob = datetime.datetime.now()
            self.collection.insert_one({"name": name, "dob": dob})

    @timeit_decorator
    def read_all_names(self) -> None:
        user_names = []
        for user in self.collection.find():
            user_names.append(user.get("name"))
        printinfo(f"read_all_names: {len(user_names)} users found")

    @timeit_decorator
    def filter_names(self) -> None:
        user_names = []
        for user in self.collection.find({"name": {"$regex": "mo"}}):
            user_names.append(user.get("name"))
        printinfo(f"filter_names: {len(user_names)} users found")

    def run(self):
        c = Console()
        c.print()
        table = Table(title="MongoDB Benchmark")
        table.add_column("Benchmark", justify="left")
        table.add_column("Time", justify="center", style="cyan bold")


        table.add_row("insert_all", f"{self.insert_all():.3f}")
        table.add_row("insert_separately", f"{self.insert_separately():.3f}")
        table.add_row("read_all_names", f"{self.read_all_names():.3f}")
        table.add_row("filter_names", f"{self.filter_names():.3f}")
        c.print(table)


if __name__ == "__main__":
    bm = MongoBenchmarker()
    bm.run()
