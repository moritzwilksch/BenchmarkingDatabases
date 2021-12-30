from sqlalchemy.orm.session import SessionTransaction
from sqlalchemy.orm import backref, relation, sessionmaker, relationship
from sqlalchemy import create_engine, Column, Integer, String, DateTime
import os
from sqlalchemy.ext.declarative import declarative_base
import random
import datetime
import time
from rich.console import Console
from rich.markdown import Markdown

c = Console()

Base = declarative_base()
engine = create_engine(f"postgresql+psycopg2://postgres:postgres@localhost/postgres")
Session = sessionmaker(bind=engine)
session = Session()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    dob = Column(DateTime)

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

letters = "abcdefghijklmnopqrstuvwxyz"


# --------------------------- Benchmarking -----------------------------------
N = 100_000
c.print(Markdown("# Postgres Benchmark"))
c.print(Markdown("## Inserting"))


# insert all 
tic = time.perf_counter()
users = []
for _ in range(N):
    name = "".join(random.sample(letters, random.randint(4, 25)))
    dob = datetime.datetime.now()
    user = User(name=name, dob=dob)
    users.append(user)
session.add_all(users)
session.commit()
tac = time.perf_counter()
c.print(f"{'Time to insert_all:':<30} {tac - tic:.5f}")



# insert separately
tic = time.perf_counter()
for _ in range(N):
    name = "".join(random.sample(letters, random.randint(4, 25)))
    dob = datetime.datetime.now()
    user = User(name=name, dob=dob)
    session.add(user)

session.commit()
tac = time.perf_counter()
c.print(f"{'Time to insert separately:':<30} {tac - tic:.5f}")

c.print(Markdown("---"))
c.print(Markdown("## Querying"))
# read all names
tic = time.perf_counter()
user_names = []
for user in session.query(User).all():
    user_names.append(user.name)
tac = time.perf_counter()
c.print(f"{'Time to read all names:':<30} {tac - tic:.5f}")
c.print(f"--> found {len(user_names)} users")


# filter name for pattern
tic = time.perf_counter()
user_names = []
for user in session.query(User).filter(User.name.contains("mo")):
    user_names.append(user.name)
tac = time.perf_counter()
c.print(f"{'Time to filter for pattern:':<30} {tac - tic:.5f}")
c.print(f"--> found {len(user_names)} users")
