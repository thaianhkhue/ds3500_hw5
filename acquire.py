"""
acquire.py
----------
Fetch and clean LAMP subway performance data
for the Red Line in February 2026. Returns cleaned, concatenated
February DataFrame filtered to a given trunk_route_id and ordered list of
station names for a given line.
"""

import pandas as pd
from pathlib import Path

ROUTE_ID = "Red"
CACHE_PATH = Path("february_red_data.parquet")

BASE_URL = (
    "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/"
    "{year}-{month:02d}-{day:02d}-subway-on-time-performance-v1.parquet"
)

#geographic stop order for the red line (Alewife -> branches)
RED_LINE_STOPS = [
    "place-alfcl",
    "place-davis",
    "place-portr",
    "place-harsq",
    "place-cntsq",
    "place-knncl",
    "place-chmnl",
    "place-pktrm",
    "place-dwnxg",
    "place-sstat",
    "place-brdwy",
    "place-andrw",
    "place-jfk",
    "place-smmnl",
    "place-fldcr",
    "place-shmnl",
    "place-asmnl",
    "place-nqncy",
    "place-wlsta",
    "place-qnctr",
    "place-qamnl",
    "place-brntn",
]

#Data fetching
def fetch_february(use_cache: bool = True) -> pd.DataFrame:
    """
    Fetches all 28 days of February 2026 for the red line route.
    Filters to trunk_route_id == 'Red' after each fetch. Also, caches the result locally.
    Args: use_cache (bool) -> if True, loads from the local parquet cache if it exists
    Returns: DataFrame
    """

    if use_cache and CACHE_PATH.exists():
        print(f"Loading from cache: {CACHE_PATH}")
        return pd.read_parquet(CACHE_PATH)

    frames = []
    for day in range(1, 29):
        url = BASE_URL.format(year=2026, month=2, day=day)
        print(f"Fetching {url} ...")
        try:
            df = pd.read_parquet(url)
            df = df[df["trunk_route_id"] == ROUTE_ID]
            frames.append(df)
        except Exception as e:
            print(f"  Warning: could not fetch day {day}: {e}")

    combined = pd.concat(frames, ignore_index=True)
    combined.to_parquet(CACHE_PATH, index=False)
    print(f"Saved cache to {CACHE_PATH}")

    return combined


#Data cleaning
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw LAMP DataFrame and achieves the following by deduplicating trip_id/stop_id pairs, keeping
    earliest per pair, drops rows missing required fields for actual analysis, sums segment times into full
    trip durations, computes scheduled full trip durations only where the scheduled data is, and tracks
    stop count per trip to identify coverage gaps.
    Args: DataFram — raw concatenated LAMP data for the red line
    Returns: cleaned DataFrame
    """
    df = df.copy()

    #deduplicate
    if "stop_timestamp" in df.columns:
        df = df.sort_values("stop_timestamp")
    df = df.drop_duplicates(subset=["trip_id", "stop_id"], keep="first")

    #drop rows missing required fields
    required_cols = [
        "trip_id", "parent_station",
        "travel_time_seconds", "service_date",
    ]
    df = df.dropna(subset=[c for c in required_cols if c in df.columns])

    #sums segment times into full trip durations
    trip_actual = (
        df.groupby(["trip_id", "service_date"])["travel_time_seconds"]
        .sum()
        .reset_index()
        .rename(columns={"travel_time_seconds": "trip_actual_seconds"})
    )
    scheduled_df = df.dropna(subset=["scheduled_travel_time"])

    trip_scheduled = (
        scheduled_df.groupby(["trip_id", "service_date"])["scheduled_travel_time"]
        .sum()
        .reset_index()
        .rename(columns={"scheduled_travel_time": "trip_scheduled_seconds"})
    )

    #count stops per trip to identify trips with large coverage gaps
    trip_stop_counts = (
        df.groupby(["trip_id", "service_date"])["parent_station"]
        .count()
        .reset_index()
        .rename(columns={"parent_station": "stop_count"})
    )

    #keep only stop level columns
    stop_level = df[[
        "trip_id", "service_date", "parent_station",
        "travel_time_seconds", "scheduled_travel_time",
    ]].copy()

    #merge trip level aggregates back onto stop level rows
    stop_level = stop_level.merge(trip_actual, on=["trip_id", "service_date"], how="left")
    stop_level = stop_level.merge(trip_scheduled, on=["trip_id", "service_date"], how="left")
    stop_level = stop_level.merge(trip_stop_counts, on=["trip_id", "service_date"], how="left")

    #Convert service_date to correct string format
    if pd.api.types.is_integer_dtype(stop_level["service_date"]):
        stop_level["service_date"] = pd.to_datetime(
            stop_level["service_date"].astype(str), format="%Y%m%d"
        ).dt.strftime("%Y-%m-%d")
    else:
        stop_level["service_date"] = (
            pd.to_datetime(stop_level["service_date"]).dt.strftime("%Y-%m-%d")
        )

    return stop_level

def get_clean_dataframe(route_id: str = ROUTE_ID, use_cache: bool = True) -> pd.DataFrame:
    """
    Runs the full fetch and clean pipeline and returns a complete
    stop level DataFrame ready for analysis.
    Args: route_id (str), use_cache (bool)
    Returns: DataFrame with columns trip_id, service_date, parent_station,
             travel_time_seconds, scheduled_travel_time, trip_actual_seconds,
             trip_scheduled_seconds, stop_count
    """
    raw = fetch_february(use_cache=use_cache)
    return clean(raw)

def get_stop_order(route_id: str = ROUTE_ID) -> list[str]:
    """
    Returns the hardcoded geographic stop order for the Red Line,
    ordered inbound from Alewife through the different branches.
    Args: route_id (str)
    Returns: list of parent_station ID strings
    """
    return RED_LINE_STOPS