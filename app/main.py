# %%
from datetime import timedelta

import folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster
from sqlalchemy import create_engine
from streamlit_folium import st_folium

# %%
CENTER_START = [48.851140449664435, 2.343732422119687]
ZOOM_START = 12


def init_session_state():
    # Create SQLite Engines
    if "station_engine" not in st.session_state:
        st.session_state["station_engine"] = create_engine(
            "sqlite:///../data/paris_meteo.db", echo=False
        )
    if "velo_comptage_engine" not in st.session_state:
        st.session_state["velo_comptage_engine"] = create_engine(
            "sqlite:///../data/comptage_velo.db", echo=False
        )

    # Folium Center and Zoom
    if "center" not in st.session_state:
        st.session_state["center"] = CENTER_START

    if "zoom" not in st.session_state:
        st.session_state["zoom"] = ZOOM_START


init_session_state()

# %%
meteo_station_info = pd.read_sql(
    """
    SELECT
        StationName, Code, Latitude, Longitude
    FROM
        paris_station
    """,
    con=st.session_state["station_engine"],
)

# %% Read velo_comptage

# %% Calculate Min and Max Date
min_max_date = pd.read_sql(
    """
    SELECT
        min(date), max(date)
    FROM
        comptage_velo
    """,
    con=st.session_state["velo_comptage_engine"],
    dtype="datetime64[ns]",
)

min_date = min_max_date.loc[0, "min(date)"].date()
max_date = min_max_date.loc[0, "max(date)"].date()
print("Min date:", min_date)
print("Max date:", max_date)

diff_days = (max_date - min_date) + timedelta(days=1)
print("Diff days:", diff_days)

# %%
mean_lat_lng = pd.read_sql(
    """
    SELECT
        avg(latitude), avg(longitude)
    FROM
        comptage_velo
    """,
    con=st.session_state["velo_comptage_engine"],
)
mean_lat = mean_lat_lng.loc[0, "avg(latitude)"]
mean_lng = mean_lat_lng.loc[0, "avg(longitude)"]

print("Mean Lat:", mean_lat)
print("Mean Long:", mean_lng)


# %% Begin of Streamlit application
# Date slider
st_date = st.slider(
    "Day",
    min_value=min_date,
    max_value=max_date,
    value=min_date,
    format="MM/DD/YY",
)


# %%
# get the count of bikes
df_bikes = pd.read_sql(
    f"""
    SELECT
        date, id, name, sum_counts, longitude, latitude
    FROM
        comptage_velo
    WHERE
        date(date) BETWEEN date("{st_date}") and date("{st_date}")
    """,
    con=st.session_state["velo_comptage_engine"],
)

# %%
# get the weather from different stations
df_weather = pd.read_sql(
    f"""
    SELECT
        Date, station, StationName, Longitude , Latitude, "Température max.", "Température min.", "Précipitations 24h"
    FROM
        paris_meteo
    LEFT JOIN paris_station
    ON paris_meteo.station = paris_station.code
    WHERE
        date("Date") = "{st_date}"
    """,
    con=st.session_state["station_engine"],
)


# %%
m = folium.Map(location=st.session_state["center"], zoom_start=st.session_state["zoom"])

marker_cluster = MarkerCluster().add_to(m)

for i, compteur in df_bikes.iterrows():
    folium.Marker(
        location=(compteur["latitude"], compteur["longitude"]),
        popup=f"{compteur['sum_counts']} bikes",
    ).add_to(marker_cluster)

map_data = st_folium(m, width=700, height=300)
if map_data:
    st.session_state["zoom"] = map_data.get("zoom", ZOOM_START)
    if "center" in map_data:
        st.session_state["center"] = (
            map_data["center"]["lat"],
            map_data["center"]["lng"],
        )
# %%
df_bikes
