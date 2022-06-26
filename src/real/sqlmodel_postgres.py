import json
import os
import time
from email.policy import default
from typing import Optional

from joblib import Parallel, delayed
from rich.console import Console
from rich.table import Table
from sqlalchemy import Column
from sqlmodel import ARRAY, JSON, Field, Session, SQLModel, create_engine

with open("data/dump.json", "r") as f:
    data = json.load(f)

# print(data[0]["values"])
# exit()

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


class Fact(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticker: str
    name: str
    unit: str
    values: list = Field(sa_column=Column(ARRAY(JSON)))

    # class Config:
    #     arbitrary_types_allowed = True


class PostgresBenchmarker:
    def __init__(self) -> None:
        user = os.getenv("POSTGRES_USER")
        passwd = os.getenv("POSTGRES_PASSWORD")

        self.engine = create_engine(
            f"postgresql://{user}:{passwd}@localhost/postgres"  # +psycopg2
        )

        self.session = Session(self.engine)

        SQLModel.metadata.drop_all(self.engine)
        SQLModel.metadata.create_all(self.engine)

    @timeit_decorator
    def insert_all(self) -> None:
        facts = []
        for item in data:
            facts.append(Fact(**item))
        self.session.add_all(facts)
        self.session.commit()

    @timeit_decorator
    def insert_separately(self, N: int = 10_000) -> None:
        for item in data[:N]:
            self.session.add(Fact(**item))
        self.session.commit()

    @timeit_decorator
    def insert_concurrently(self) -> None:
        def worker(item: dict):
            self.session.add(Fact(**item))

        _ = Parallel(prefer="processes", n_jobs=3)(
            delayed(worker)(item) for item in data
        )
        self.session.commit()

    # @timeit_decorator
    # def read_all_names(self) -> None:
    #     user_names = []
    #     for user in self.session.query(User).all():
    #         user_names.append(user.name)

    #     printinfo(f"read_all_names: {len(user_names)} users found")

    @timeit_decorator
    def query1(self) -> None:
        results = self.session.query(Fact).filter_by(ticker="ABBV", name="Revenues")
        printinfo(f"query1: {len(list(results))} results found")

    @timeit_decorator
    def query2(self) -> None:
        # results = self.collection.find({"ticker": "ABBV", "name": {"$regex": "Rev"}})
        results = self.session.query(Fact).filter(
            Fact.name.like("%Rev%"), Fact.ticker == "ABBV"
        )

        printinfo(f"query2: {len(list(results))} results found")

    def run(self):
        c = Console()
        c.print()
        table = Table(title="Postgres Benchmark")
        table.add_column("Benchmark", justify="left")
        table.add_column("Time", justify="center", style="cyan bold")

        table.add_row("insert_all", f"{self.insert_all():.3f}")
        table.add_row("insert_separately", f"{self.insert_separately():.3f}")
        # table.add_row("insert_concurrently", f"{self.insert_concurrently():.3f}")
        table.add_row("query1", f"{self.query1():.3f}")
        table.add_row("query2", f"{self.query2():.3f}")
        c.print(table)


if __name__ == "__main__":
    bm = PostgresBenchmarker()
    bm.run()
    # user = os.getenv("POSTGRES_USER")
    # passwd = os.getenv("POSTGRES_PASSWORD")

    # engine = create_engine(
    #     f"postgresql://{user}:{passwd}@localhost/postgres"  # +psycopg2
    # )

    # SQLModel.metadata.create_all(engine)
