from acquire import get_clean_dataframe, get_stop_order

df = get_clean_dataframe()
stop_order = get_stop_order()

print("Shape:", df.shape)
print("\nColumns:")
print(df.columns.tolist())

print("\nUnique dates:", len(df["service_date"].unique()))
print(sorted(df["service_date"].unique()))

print("\nMissing values:")
print(df.isna().sum())

print("\nRows per day:")
print(df.groupby("service_date").size())

print("\nStops in data but not in stop_order:")
print(set(df["parent_station"].unique()) - set(stop_order))

print("\nSample:")
print(df.head())