# %%
from surrealdb import Surreal
import asyncio

# %%
db = Surreal("ws://localhost:1943/rpc")
await db.connect()
await db.signin({"user": "moritz", "pass": "suedkreuzBhF"})
await db.use("test", "person")

# %%
for age in range(0, 10_000):
    await db.create("person", {"name": "moritz", "age": age, "interests": ['a', "b", "c", 6]})

# %%
await db.query("select id, age from person;")

# %%
await db.select("person")

# %%
await db.query("select id, age from person where age >5 ;")

#%%
await db.select('person')
