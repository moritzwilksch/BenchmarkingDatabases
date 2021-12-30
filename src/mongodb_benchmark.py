
import random
import datetime
import time
from rich.console import Console
from rich.markdown import Markdown
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['test']
collection = db['testcollection']
collection.drop()
collection = db['testcollection']
c = Console()




letters = "abcdefghijklmnopqrstuvwxyz"


# --------------------------- Benchmarking -----------------------------------
N = 100_000
c.print(Markdown("# MongoDB Benchmark"))
c.print(Markdown("## Inserting"))


# insert all 
tic = time.perf_counter()
users = []
for _ in range(N):
    name = "".join(random.sample(letters, random.randint(4, 25)))
    dob = datetime.datetime.now()
    users.append({'name': name, 'dob': dob})
collection.insert_many(users)
tac = time.perf_counter()
c.print(f"{'Time to insert_all:':<30} {tac - tic:.5f}")



# insert separately
tic = time.perf_counter()
for _ in range(N):
    name = "".join(random.sample(letters, random.randint(4, 25)))
    dob = datetime.datetime.now()
    collection.insert_one({'name': name, 'dob': dob})
tac = time.perf_counter()
c.print(f"{'Time to separately:':<30} {tac - tic:.5f}")


c.print(Markdown("---"))
c.print(Markdown("## Querying"))

# read all names
tic = time.perf_counter()
user_names = []
for user in collection.find():
    user_names.append(user.get("name"))
tac = time.perf_counter()
c.print(f"{'Time to read all names:':<30} {tac - tic:.5f}")
c.print(f"--> found {len(user_names)} users")

# filter name for pattern
tic = time.perf_counter()
user_names = []
for user in collection.find({"name": {"$regex": "mo"}}):
    user_names.append(user.get("name"))
tac = time.perf_counter()
c.print(f"{'Time to filter for pattern:':<30} {tac - tic:.5f}")
c.print(f"--> found {len(user_names)} users")
