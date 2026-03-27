"""
model.py
----------
NOTES:
Columns and what they mean

travel_time_seconds and scheduled_travel_time are segment-level (stop-to-stop). Animation B (heatmap) should use these grouped by parent_station.
trip_actual_seconds and trip_scheduled_seconds are full trip totals (summed across all stops). Animation A (line plot) should use these for daily averages.
stop_count is how many stops were recorded per trip — useful if she wants to filter out trips with large coverage gaps (e.g. stop_count < 5 being suspicious).

Red Line branch note
The stop order handles both branches — Ashmont (place-smmnl through place-asmnl) and Braintree (place-nqncy through place-brntn) are both in get_stop_order(). For the heatmap rows she'll need to decide how to handle the branch split visually since they share the trunk but diverge after place-jfk.
Storm days
Feb 23–24 have significantly fewer rows (~1,600 vs ~2,600 on a normal day) because the MBTA ran non-standard service that didn't match the GTFS schedule — those rows were dropped in cleaning. Daily averages around the storm will be from a smaller sample, which is worth calling out in the reflection.
Cache
The file february_red_data.parquet is already saved in the project folder — she doesn't need to re-fetch anything, just call get_clean_dataframe() and it loads from cache instantly.
"""

from acquire import get_clean_dataframe, get_stop_order