"""
model.py
----------
NOTES:
Columns and what they mean

travel_time_seconds and scheduled_travel_time are segment-level values
(previous stop -> this stop). Animation B (heatmap) should use these
grouped by parent_station and service_date.

trip_actual_seconds and trip_scheduled_seconds are full-trip totals
(summed across all recorded stops in a trip). Animation A (line plot)
should use these for daily averages.

stop_count is the number of stops recorded per trip. It can be useful
for identifying incomplete trips or trips with large coverage gaps.

Red Line branch note
The stop order includes both branches after JFK/UMass:
- Ashmont branch: place-smmnl through place-asmnl
- Braintree branch: place-nqncy through place-brntn

For the heatmap, rows follow this hardcoded geographic order. Because the
Red Line branches, the lower portion of the heatmap represents two diverging
paths after place-jfk.

Storm days
Feb 23–24 have noticeably fewer rows than many other days in the month.
On storm days, scheduled data may be missing more often because the MBTA
ran non-standard service that did not always match the planned GTFS
schedule. These rows are still kept in the cleaned dataset for actual
travel analysis, but they are excluded when computing scheduled trip totals.
This means daily_avg_scheduled may be based on a smaller subset of trips
around the storm, which is worth noting in the reflection.

Cache
If february_red_data.parquet already exists in the project folder,
get_clean_dataframe() will load from cache instead of re-fetching the
28 daily parquet files.
"""

from pydantic import BaseModel, computed_field
import pandas as pd

class SubwayLine(BaseModel):
    """
    Pydantic model for one MBTA subway line in February 2026.
    Stores the cleaned DataFrame and exposes computed summaries
    for the animation layer.
    """

    route_name: str
    route_id: str
    df: pd.DataFrame
    stop_order: list[str]

    model_config = {
        "arbitrary_types_allowed": True
    }

    @computed_field
    @property
    def stops(self) -> list[str]:
        """
        Ordered list of stop IDs along the line.
        """
        return self.stop_order

    @computed_field
    @property
    def dates(self) -> list[str]:
        """
        Sorted list of service dates in February as strings.
        """
        dates = self.df["service_date"].astype(str).unique().tolist()
        dates.sort()
        return dates

    @computed_field
    @property
    def daily_avg_travel(self) -> dict[str, float]:
        """
        Date string -> mean actual full-trip travel time (seconds)
        across all trips that day.
        """
        trip_df = (
            self.df[["service_date", "trip_id", "trip_actual_seconds"]]
            .drop_duplicates()
        )

        daily = (
            trip_df.groupby("service_date")["trip_actual_seconds"]
            .mean()
            .sort_index()
        )

        return daily.to_dict()

    @computed_field
    @property
    def daily_avg_scheduled(self) -> dict[str, float]:
        """
        Date string -> mean scheduled full-trip travel time (seconds)
        across trips with valid scheduled data that day.
        """
        trip_df = (
            self.df[["service_date", "trip_id", "trip_scheduled_seconds"]]
            .drop_duplicates()
            .dropna(subset=["trip_scheduled_seconds"])
        )

        daily = (
            trip_df.groupby("service_date")["trip_scheduled_seconds"]
            .mean()
            .sort_index()
        )

        return daily.to_dict()

    @computed_field
    @property
    def travel_by_stop_and_day(self) -> pd.DataFrame:
        """
        Pivot table where:
        - rows are parent stations in geographic order
        - columns are service dates
        - values are mean segment travel time in seconds

        This is the 2D table used by the heatmap animation.
        """
        pivot = self.df.pivot_table(
            index="parent_station",
            columns="service_date",
            values="travel_time_seconds",
            aggfunc="mean"
        )

        pivot = pivot.reindex(self.stop_order)

        ordered_dates = sorted(pivot.columns.astype(str))
        pivot = pivot[ordered_dates]

        return pivot