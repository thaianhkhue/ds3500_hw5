"""
acquire.py
----------
Single responsibility: fetch and clean LAMP subway performance data
for the Red Line in February 2026. Returns analysis-ready DataFrames.
No plotting, no modeling.
"""

import pandas as pd
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────
ROUTE_ID = "Red"
CACHE_PATH = Path("february_red_data.parquet")

BASE_URL = (
    "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/"
    "{year}-{month:02d}-{day:02d}-subway-on-time-performance-v1.parquet"
)

# Geographic stop order for the Red Line (inbound: Alewife → branches)
RED_LINE_STOPS = [
    "place-alfcl",  # Alewife
    "place-davis",  # Davis
    "place-portr",  # Porter
    "place-harsq",  # Harvard
    "place-cntsq",  # Central
    "place-knncl",  # Kendall/MIT
    "place-chmnl",  # Charles/MGH
    "place-pktrm",  # Park Street
    "place-dwnxg",  # Downtown Crossing
    "place-sstat",  # South Station
    "place-brdwy",  # Broadway
    "place-andrw",  # Andrew
    "place-jfk",    # JFK/UMass (branch split)
    # Ashmont branch
    "place-smmnl",  # Savin Hill
    "place-fldcr",  # Fields Corner
    "place-shmnl",  # Shawmut
    "place-asmnl",  # Ashmont
    # Braintree branch
    "place-nqncy",  # North Quincy
    "place-wlsta",  # Wollaston
    "place-qnctr",  # Quincy Center
    "place-qamnl",  # Quincy Adams
    "place-brntn",  # Braintree
]


# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch_february(use_cache: bool = True) -> pd.DataFrame:
    """
    Fetch all 28 days of February 2026 for the Red Line.
    Filters to trunk_route_id == 'Red' immediately after each fetch.
    Caches the concatenated result locally to avoid repeated downloads.
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


# ── Clean ──────────────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw LAMP DataFrame:
      1. Deduplicate trip_id/parent_station pairs — keep earliest per pair
      2. Drop rows missing required fields
      3. Drop rows with null scheduled_travel_time (unmatched/added trips)
      4. Sum segment times into full end-to-end trip durations
      5. Track stop count per trip to flag coverage gaps
    Returns a stop-level DataFrame with both segment and full-trip columns.
    """
    df = df.copy()

    # 1. Deduplicate — keep earliest record per (trip_id, parent_station)
    if "stop_timestamp" in df.columns:
        df = df.sort_values("stop_timestamp")
    df = df.drop_duplicates(subset=["trip_id", "parent_station"], keep="first")

    # 2. Drop rows missing required fields
    required_cols = [
        "trip_id", "parent_station",
        "travel_time_seconds", "scheduled_travel_time", "service_date",
    ]
    df = df.dropna(subset=[c for c in required_cols if c in df.columns])

    # 3. Drop rows with null scheduled_travel_time
    #    These are unplanned or added trips that didn't match the GTFS schedule.
    #    This subset will be smaller on storm days (Feb 23-24) when the MBTA
    #    operated non-standard service — noted in reflection.md.
    df = df.dropna(subset=["scheduled_travel_time"])

    # 4. Sum segment times into full end-to-end trip durations
    trip_actual = (
        df.groupby(["trip_id", "service_date"])["travel_time_seconds"]
        .sum()
        .reset_index()
        .rename(columns={"travel_time_seconds": "trip_actual_seconds"})
    )
    trip_scheduled = (
        df.groupby(["trip_id", "service_date"])["scheduled_travel_time"]
        .sum()
        .reset_index()
        .rename(columns={"scheduled_travel_time": "trip_scheduled_seconds"})
    )

    # 5. Count stops per trip to identify trips with large coverage gaps
    trip_stop_counts = (
        df.groupby(["trip_id", "service_date"])["parent_station"]
        .count()
        .reset_index()
        .rename(columns={"parent_station": "stop_count"})
    )

    # Keep only stop-level columns the model needs
    stop_level = df[[
        "trip_id", "service_date", "parent_station",
        "travel_time_seconds", "scheduled_travel_time",
    ]].copy()

    # Merge trip-level aggregates back onto stop-level rows
    stop_level = stop_level.merge(trip_actual, on=["trip_id", "service_date"])
    stop_level = stop_level.merge(trip_scheduled, on=["trip_id", "service_date"])
    stop_level = stop_level.merge(trip_stop_counts, on=["trip_id", "service_date"])

    # Normalize service_date to YYYY-MM-DD strings
    # Handle integer format (YYYYMMDD) or date/datetime format
    if pd.api.types.is_integer_dtype(stop_level["service_date"]):
        stop_level["service_date"] = pd.to_datetime(
            stop_level["service_date"].astype(str), format="%Y%m%d"
        ).dt.strftime("%Y-%m-%d")
    else:
        stop_level["service_date"] = (
            pd.to_datetime(stop_level["service_date"]).dt.strftime("%Y-%m-%d")
        )

    return stop_level


# ── Public API ─────────────────────────────────────────────────────────────────

def get_clean_dataframe(route_id: str = ROUTE_ID, use_cache: bool = True) -> pd.DataFrame:
    """
    Full pipeline: fetch → clean → return.
    Primary entry point for the model layer.

    Returns a stop-level DataFrame with columns:
        trip_id, service_date, parent_station,
        travel_time_seconds       (segment actual)
        scheduled_travel_time     (segment scheduled)
        trip_actual_seconds       (full trip actual)
        trip_scheduled_seconds    (full trip scheduled)
        stop_count                (stops recorded per trip)
    """
    raw = fetch_february(use_cache=use_cache)
    return clean(raw)


def get_stop_order(route_id: str = ROUTE_ID) -> list[str]:
    """
    Return the hardcoded geographic stop order for the Red Line.
    Only parent_station IDs that appear in the cleaned data will be
    used by the model layer — extras are harmless.
    """
    return RED_LINE_STOPS