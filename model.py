"""
model.py
----------
Pydantic model representing the MBTA Red Line in February 2026.
Accepts the cleaned DataFrame from the acquisition layer and exposes
aggregated, animation ready data as computed fields.
"""

from pydantic import BaseModel, computed_field
import pandas as pd

class SubwayLine(BaseModel):
    """
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
        Date string -> mean scheduled full trip travel time (seconds)
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
        Pivot table with rows as parent stations in geographic order, columns are service dates,
        and values are mean segment travel time in seconds

        This is the table used by the heatmap.
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