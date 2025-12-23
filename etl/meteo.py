# %%
import csv
import pickle
from io import StringIO
from string import Template

import numpy as np
import pandas as pd
import requests
from _duckdb import fetch_arrow_table
from bs4 import BeautifulSoup
from IPython.display import display
from sqlalchemy import create_engine

TEMPLATE_URL = Template(
    "https://www.meteociel.fr/cartes_obs/climato2v4.php?code=$station&mois=$month&annee=$year&print=1"
)


# %%
def read_station_info():
    df = pd.read_csv("../data/meteo_station_info.csv", quoting=csv.QUOTE_NONE)
    return df


# %%
meteo_station_info = read_station_info()
meteo_station_info["Latitude"] = (
    meteo_station_info["Latitude"]
    .str.split(r"\D")
    .apply(lambda x: float(x[0]) + (float(x[1]) + float(x[2]) / 60) / 60)
)
meteo_station_info["Longitude"] = (
    meteo_station_info["Longitude"]
    .str.split(r"\D")
    .apply(lambda x: float(x[0]) + (float(x[1]) + float(x[2]) / 60) / 60)
)
display(meteo_station_info)


# %%
def table_to_df(soup: BeautifulSoup):
    return pd.DataFrame(
        [[td.string for td in tr.find_all("td")] for tr in soup.find_all("tr")]
    )


# %%
def fetch_meteo(station, month, year):
    url = TEMPLATE_URL.substitute(station=station, month=month, year=year)
    print(f"fetching {url}")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from {url}")

    soup = BeautifulSoup(response.content, "html.parser")

    # Drop the header
    _df = table_to_df(soup.find_all("table")[1]).iloc[1:]
    # Drop the last row
    _df = _df.iloc[:-1]
    COLUMN_NAMES = [
        "Jour",
        "Température max.",
        "Température min.",
        "Précipitations 24h",
        "Ensoleillement",
        "Enneigement",
    ]
    _df.columns = COLUMN_NAMES[: len(_df.columns)]

    _df.replace(r"\s*---\s*", None, regex=True, inplace=True)

    _df["Jour"] = _df["Jour"].str.split().apply(lambda x: int(x[1]) if x else np.nan)
    _df["Température max."] = (
        _df["Température max."]
        .str.split()
        .apply(lambda x: float(x[0]) if x else np.nan)
    )
    _df["Température min."] = (
        _df["Température min."]
        .str.split()
        .apply(lambda x: float(x[0]) if x else np.nan)
    )
    _df["Précipitations 24h"] = (
        _df["Précipitations 24h"]
        .str.split()
        .apply(lambda x: float(x[0]) if x else np.nan)
    )

    if "Ensoleillement" in _df.columns:
        _df["Ensoleillement"] = (
            _df["Ensoleillement"]
            .str.split()
            .apply(lambda x: float(x[0]) if x else np.nan)
        )

    if "Enneigement" in _df.columns:
        _df["Enneigement"] = (
            _df["Enneigement"].str.split().apply(lambda x: float(x[0]) if x else np.nan)
        )

    _df["Jour"] = _df["Jour"].astype(int)
    _df["Date"] = pd.to_datetime(
        _df["Jour"].apply(lambda d: f"{year}-{month:02}-{d:02}")
    )
    _df.drop(columns=["Jour"], inplace=True)
    _df.set_index("Date", inplace=True)

    return _df


# %% Fetch all the weather info from stations in Paris
# from 2024-11 to 2025-12
START_YEAR_MONTH = (2024, 11)
END_YEAR_MONTH = (2025, 12)

year_months = []
current_year_month = START_YEAR_MONTH
year_months.append(current_year_month)

while current_year_month != END_YEAR_MONTH:
    year, month = current_year_month
    new_month = month + 1
    if new_month == 13:
        current_year_month = (year + 1, 1)
    else:
        current_year_month = (year, new_month)

    year_months.append(current_year_month)

print(year_months)

# %%
station_meteos = []
for i, station in meteo_station_info.iterrows():
    station_name, code, latitude, longitude, altitude = station
    print(f"fetching meteo from {station_name} ({code})")
    station_meteos.append(
        pd.concat([fetch_meteo(code, month, year) for year, month in year_months])
    )

# %% Save all the data to pickls
with open("../data/meteo-paris.pkl", "wb") as f:
    pickle.dump(station_meteos, f)

# %%
with open("../data/meteo-paris.pkl", "rb") as f:
    station_meteos = pickle.load(f)

# %%
for df in station_meteos:
    print(df.columns)

# %% concatenate all the data frames
for i, station in meteo_station_info.iterrows():
    station_name, code, latitude, longitude, altitude = station
    # add the station code to each dataframe
    station_meteos[i] = station_meteos[i].assign(station=code)
    station_meteos[i].reset_index(inplace=True)

station_meteos = pd.concat(station_meteos)

# %%
engine = create_engine("sqlite:///../data/paris_meteo.db", echo=False)

# %%
meteo_station_info.to_sql(name="paris_station", con=engine, if_exists="replace")
station_meteos.to_sql(name="paris_meteo", con=engine, if_exists="replace")


# %% load from SQLite database
# meteo_station_info = pd.read_sql("SELECT * FROM paris_station", con=engine)
# station_meteos = pd.read_sql("SELECT * FROM paris_meteo", con=engine)
# station_meteos["station"].unique()
