import json
import os
import time

from peewee import FloatField, Model, PostgresqlDatabase, PrimaryKeyField, TextField
from playhouse.postgres_ext import JSONField, PostgresqlExtDatabase
from rich.console import Console
from rich.table import Table
from sqlalchemy import ForeignKey

c = Console()
with open("data/dump.json", "r") as f:
    data = json.load(f)


user = os.getenv("POSTGRES_USER")
passwd = os.getenv("POSTGRES_PASSWORD")

db = PostgresqlExtDatabase(f"postgresql://{user}:{passwd}@localhost")


class BaseModel(Model):
    class Meta:
        database = db


def printinfo(text: str) -> None:
    c.print(f"[green][INFO][/] {text}")


def timeit_decorator(func: callable):
    def wrapper(self=None):
        tic = time.perf_counter()
        func(self)
        tac = time.perf_counter()
        return tac - tic

    return wrapper


class Item(BaseModel):
    id = PrimaryKeyField()
    name = TextField()
    unit = TextField()


class Company(BaseModel):
    id = PrimaryKeyField()
    ticker = TextField()


class Fact(BaseModel):
    id = PrimaryKeyField()
    item = ForeignKey(Item)
    company = ForeignKey(Company)
    value = FloatField()


class PostgresBenchmarker:
    def __init__(self, db: PostgresqlExtDatabase) -> None:

        self.db = db
        db.connect()
        db.drop_tables([Fact, Company, Item])
        db.create_tables([Fact, Company, Item])

    @timeit_decorator
    def insert_all(self) -> None:
        Fact.insert_many(data).execute()

    @timeit_decorator
    def insert_separately(self, N: int = 10_000) -> None:
        for item in data[:N]:
            # Fact(**item).save()
            Fact.insert(item).execute()

    # @timeit_decorator
    # def read_all_names(self) -> None:
    #     user_names = []
    #     for user in self.session.query(User).all():
    #         user_names.append(user.name)

    #     printinfo(f"read_all_names: {len(user_names)} users found")

    @timeit_decorator
    def query1(self) -> None:
        results = Fact.select().where(Fact.ticker == "ABBV", Fact.name == "Revenues")
        printinfo(f"query1: {len(list(results))} results found")

    @timeit_decorator
    def query2(self) -> None:
        results = Fact.select().where(Fact.name.like("%Rev%"), Fact.ticker == "ABBV")

        printinfo(f"query2: {len(list(results))} results found")

    def run(self):
        c = Console()
        c.print()
        table = Table(title="Postgres Benchmark")
        table.add_column("Benchmark", justify="left")
        table.add_column("Time", justify="center", style="cyan bold")

        table.add_row("insert_all", f"{self.insert_all():.3f}")
        # table.add_row("insert_separately", f"{self.insert_separately():.3f}")
        # table.add_row("query1", f"{self.query1():.3f}")
        # table.add_row("query2", f"{self.query2():.3f}")
        c.print(table)


if __name__ == "__main__":
    bm = PostgresBenchmarker(db=db)
    # bm.run()
