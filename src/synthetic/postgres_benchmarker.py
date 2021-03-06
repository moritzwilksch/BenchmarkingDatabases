import datetime
import random
import time

import psycopg2
from rich.console import Console
from rich.table import Table
from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relation, relationship, sessionmaker

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


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    dob = Column(DateTime)


class PostgresBenchmarker:
    def __init__(self, N=10_000) -> None:
        self.engine = create_engine(
            f"postgresql+psycopg2://postgres:postgres@localhost/postgres"
        )
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        self.N = N
        self.letters = "abcdefghijklmnopqrstuvwxyz"

    @timeit_decorator
    def insert_all(self) -> None:
        users = []
        for _ in range(self.N):
            name = "".join(random.sample(self.letters, random.randint(4, 25)))
            dob = datetime.datetime.now()
            user = User(name=name, dob=dob)
            users.append(user)
        self.session.add_all(users)
        self.session.commit()

    @timeit_decorator
    def insert_separately(self) -> None:
        for _ in range(self.N):
            name = "".join(random.sample(self.letters, random.randint(4, 25)))
            dob = datetime.datetime.now()
            user = User(name=name, dob=dob)
            self.session.add(user)
        self.session.commit()

    @timeit_decorator
    def read_all_names(self) -> None:
        user_names = []
        for user in self.session.query(User).all():
            user_names.append(user.name)

        printinfo(f"read_all_names: {len(user_names)} users found")

    @timeit_decorator
    def filter_names(self) -> None:
        user_names = []
        for user in self.session.query(User).filter(User.name.contains("mo")):
            user_names.append(user.name)
        printinfo(f"filter_names: {len(user_names)} users found")

    def run(self):
        c = Console()
        c.print()
        table = Table(title="Postgres Benchmark")
        table.add_column("Benchmark", justify="left")
        table.add_column("Time", justify="center", style="cyan bold")

        table.add_row("insert_all", f"{self.insert_all():.3f}")
        table.add_row("insert_separately", f"{self.insert_separately():.3f}")
        table.add_row("read_all_names", f"{self.read_all_names():.3f}")
        table.add_row("filter_names", f"{self.filter_names():.3f}")
        c.print(table)


# TODO: fix this class
class VanillaPostgresBenchmarker:
    """Does NOT use SQLAlchemy, only vanilla PsycoPG2"""

    def __init__(self, N=10_000) -> None:
        self.conn = psycopg2.connect(
            dbname="postgres", user="postgres", host="localhost", password="postgres"
        )

        self.N = N
        self.letters = "abcdefghijklmnopqrstuvwxyz"

        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS users")
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            dob TIMESTAMP
        )
        """
        )
        self.conn.commit()

    @timeit_decorator
    def insert_all(self) -> None:
        cur = self.conn.cursor()

        users = []
        for _ in range(self.N):
            name = "".join(random.sample(self.letters, random.randint(4, 25)))
            dob = datetime.datetime.now()
            users.append((name, dob))

        cur.executemany("INSERT INTO users (name, dob) VALUES (%s, %s)", users)
        self.conn.commit()

    @timeit_decorator
    def insert_separately(self) -> None:
        cur = self.conn.cursor()
        for _ in range(self.N):
            name = "".join(random.sample(self.letters, random.randint(4, 25)))
            dob = datetime.datetime.now()
            cur.execute("INSERT INTO users (name, dob) VALUES (%s, %s)", (name, dob))
        self.conn.commit()

    @timeit_decorator
    def read_all_names(self) -> None:
        cur = self.conn.cursor()
        user_names = []
        cur.execute("SELECT * FROM users;")
        for user in cur.fetchall():
            user_names.append(user[1])
        printinfo(f"read_all_names: {len(user_names)} users found")

    @timeit_decorator
    def filter_names(self) -> None:
        cur = self.conn.cursor()
        user_names = []
        cur.execute("SELECT * FROM users WHERE users.name LIKE '%mo%';")
        for user in cur.fetchall():
            user_names.append(user[1])
        printinfo(f"read_all_names: {len(user_names)} users found")

    def run(self):
        c = Console()
        c.print()
        table = Table(title="Vanilla Postgres Benchmark")
        table.add_column("Benchmark", justify="left")
        table.add_column("Time", justify="center", style="cyan bold")

        table.add_row("insert_all", f"{self.insert_all():.3f}")
        table.add_row("insert_separately", f"{self.insert_separately():.3f}")
        table.add_row("read_all_names", f"{self.read_all_names():.3f}")
        table.add_row("filter_names", f"{self.filter_names():.3f}")
        c.print(table)


if __name__ == "__main__":
    bm = PostgresBenchmarker()
    bm.run()

    # vbm = VanillaPostgresBenchmarker()
    # vbm.run()
