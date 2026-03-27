from acquire import get_clean_dataframe, get_stop_order
from model import SubwayLine

df = get_clean_dataframe()
stop_order = get_stop_order()

line = SubwayLine(
    route_name="Red Line",
    route_id="Red",
    df=df,
    stop_order=stop_order
)

print("=== BASIC ===")
print("Stops:", line.stops[:5])
print("Dates:", line.dates[:5])

print("\n=== DAILY AVG TRAVEL ===")
print(list(line.daily_avg_travel.items())[:5])

print("\n=== DAILY AVG SCHEDULED ===")
print(list(line.daily_avg_scheduled.items())[:5])

print("\n=== HEATMAP SHAPE ===")
print(line.travel_by_stop_and_day.shape)

print("\n=== HEATMAP SAMPLE ===")
print(line.travel_by_stop_and_day.head())