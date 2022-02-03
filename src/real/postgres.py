from sqlalchemy.orm import backref, relation, sessionmaker, relationship
from sqlalchemy import JSON, create_engine, Column, Integer, String, DateTime, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import select
import random
import datetime
import time
from rich.console import Console
from rich.table import Table
import os
import json

with open("data/dump.json", "r") as f:
    data = json.load(f)


c = Console()
Base = declarative_base()


def printinfo(text: str) -> None:
    c.print(f"[green][INFO][/] {text}")


def timeit_decorator(func: callable):
    def wrapper(self=None):
        tic = time.perf_counter()
        func(self)
        tac = time.perf_counter()
        return tac - tic

    return wrapper


class Facts(Base):
    __tablename__ = "facts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String)
    name = Column(String)
    unit = Column(String)
    values = Column(JSON)


class PostgresBenchmarker:
    def __init__(self) -> None:
        user = os.getenv("POSTGRES_USER")
        passwd = os.getenv("POSTGRES_PASSWORD")

        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{passwd}@localhost/postgres"
        )
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    @timeit_decorator
    def insert_all(self) -> None:
        facts = []
        for item in data:
            facts.append(Facts(**item))
        self.session.add_all(facts)
        self.session.commit()

    @timeit_decorator
    def insert_separately(self, N: int = 10_000) -> None:
        for item in data[:N]:
            self.session.add(Facts(**item))
        self.session.commit()

    # @timeit_decorator
    # def read_all_names(self) -> None:
    #     user_names = []
    #     for user in self.session.query(User).all():
    #         user_names.append(user.name)

    #     printinfo(f"read_all_names: {len(user_names)} users found")

    @timeit_decorator
    def query1(self) -> None:
        results = self.session.query(Facts).filter_by(ticker="ABBV", name="Revenues")
        printinfo(f"query1: {len(list(results))} results found")

    @timeit_decorator
    def query2(self) -> None:
        # results = self.collection.find({"ticker": "ABBV", "name": {"$regex": "Rev"}})
        results = self.session.query(Facts).filter(Facts.name.like("%Rev%"), Facts.ticker == "ABBV")

        printinfo(f"query2: {len(list(results))} results found")


    def run(self):
        c = Console()
        c.print()
        table = Table(title="Postgres Benchmark")
        table.add_column("Benchmark", justify="left")
        table.add_column("Time", justify="center", style="cyan bold")

        table.add_row("insert_all", f"{self.insert_all():.3f}")
        table.add_row("insert_separately", f"{self.insert_separately():.3f}")
        table.add_row("query1", f"{self.query1():.3f}")
        table.add_row("query2", f"{self.query2():.3f}")
        c.print(table)



if __name__ == "__main__":
    bm = PostgresBenchmarker()
    bm.run()
