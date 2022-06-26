#%%
import json
from tkinter.tix import COLUMN

import dotenv

dotenv.load_dotenv()
import os

with open("data/large-file.json", "r") as f:
    data = json.load(f)


#%%
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Session

engine = create_engine(
    f"postgresql://{os.getenv('postgres_user')}:{os.getenv('postgres_passwd')}@localhost/benchmark",
)

metadata = MetaData(bind=engine)

table = Table(
    "datatable",
    metadata,
    Column("id", String, primary_key=True),
    Column("type", String),
    Column("actor_id", Integer),
    Column("actor_login", String),
    Column("actor_gravatar_id", String),
    Column("actor_url", String),
    Column("actor_avatar_url", String),
    Column("repo_id", Integer),
    Column("repo_name", String),
    Column("repo_url", String),
    Column("payload_ref", String),
    Column("payload_ref_type", String),
    Column("payload_master_branch", String),
    Column("payload_description", String),
    Column("payload_pusher_type", String),
    Column("public", Boolean),
    Column("created_at", DateTime),
)

metadata.create_all()
import flatdict

#%%
from sqlalchemy import insert

COLUMN_NAMES = [c.name for c in table.columns]


def prep(item: dict) -> dict:
    flat = flatdict.FlatDict(item)
    return {
        k.replace(":", "_"): v
        for k, v in flat.items()
        if k.replace(":", "_") in COLUMN_NAMES
    }


with engine.connect() as conn:
    with conn.begin():
        conn.execute(table.insert().values([prep(item) for item in data[:10]]))


prep(data[0])
