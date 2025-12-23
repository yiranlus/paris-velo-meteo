# %%
import pandas as pd
import requests
from sqlalchemy import create_engine

# %%
# Use the API
API_ENTRYPOINT = "https://opendata.paris.fr/api"
QUERY_PATH = (
    "/explore/v2.1/catalog/datasets/comptage-velo-donnees-compteurs/records?limit=100"
)

response = requests.get(API_ENTRYPOINT + QUERY_PATH)
print(response)

# %%
data_raw = response.json()
count = data_raw["total_count"]
data_json = data_raw["results"]
df = pd.DataFrame(data_json)

# %%
# Otherwise, read data from json
df = pd.read_json("../data/comptage-velo-donnees-compteurs.json")

# %%
df["id"] = df["id"].astype(pd.Int32Dtype())
df["date"] = pd.to_datetime(df["date"], utc=True)
df["installation_date"] = pd.to_datetime(df["installation_date"])
df["longitude"] = df["coordinates"].apply(
    lambda x: x["lon"] if x and "lon" in x else None
)
df["latitude"] = df["coordinates"].apply(
    lambda x: x["lat"] if x and "lat" in x else None
)
df = df.loc[
    :,
    ["id", "name", "sum_counts", "date", "longitude", "latitude"],
]
print(df)

# %%
df = (
    df[["id", "name", "sum_counts", "longitude", "latitude"]]
    .groupby(["id", df["date"].dt.date])
    .agg(
        {
            "name": "first",
            "sum_counts": "sum",
            "longitude": "first",
            "latitude": "first",
        }
    )
)

# %%
df.reset_index(inplace=True)

# %%
df.info()

# %%
df.describe()

# %% [markdown]
# ## Save to SQLite2
engine = create_engine("sqlite:///../data/comptage_velo.db", echo=False)


# %%
df.to_sql(name="comptage_velo", con=engine, if_exists="replace")


# %%
df = pd.read_sql("SELECT * FROM comptage_velo", con=engine)


# %%
pd.to_datetime(df["date"]).dt.date
