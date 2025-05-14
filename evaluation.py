import pandas as pd
import matplotlib.pyplot as plt


charging_points_commercial = pd.read_csv("data/test/commercial_simulierte_ladeevents_kumuliert.csv")

charging_points_commercial["time_step"] = charging_points_commercial.index * 4
charging_points_commercial = charging_points_commercial.loc[charging_points_commercial["time_step"] <= 7*24*4]

charging_points_commercial.plot(kind="area", stacked=True, figsize=(10, 6))

plt.title("Stacked Time Series Plot")
plt.xlabel("Time Step")
plt.ylabel("Values")
plt.grid(True)
plt.tight_layout()
plt.show()

charging_points_private = pd.read_csv("data/test/private_occupation_2035_first.csv")
charging_points_private["time_step"] = charging_points_private.index
charging_points_private = charging_points_private.loc[charging_points_private["time_step"] <= (7*24*4)*2]
charging_points_private = charging_points_private.loc[charging_points_private["time_step"] >= (7*24*4)]
charging_points_private["time_step"] = charging_points_private["time_step"] - (7*24*4)

charging_points_private.plot(kind="area", stacked=True, figsize=(10, 6))

plt.title("Stacked Time Series Plot")
plt.xlabel("Time Step")
plt.ylabel("Values")
plt.grid(True)
plt.tight_layout()
plt.show()

merged = pd.merge(charging_points_private, charging_points_commercial, on="time_step", how="outer", suffixes=('_df1', '_df2'))

merged = merged.ffill()

merged["home"] = merged["home_detached"] + merged["home_apartment"] + merged ["home"]
merged["retail"] = merged["retail_df1"] + merged["retail_df2"]

merged = merged.drop(columns=["home_detached", "home_apartment", "home", "retail_df1", "retail_df2", "Datum_Uhrzeit", "Wochentag_Uhrzeit"])

# Mit vorherigen Werten auffüllen
merged = merged.ffill()

merged_for_plot = merged.drop(columns=["time_step"])
merged_for_plot = merged_for_plot.set_index("timestamp")

merged.plot(kind="area", stacked=True, figsize=(10, 6))

plt.title("Stacked Time Series Plot")
plt.xlabel("Time Step")
plt.ylabel("Values")
plt.grid(True)
plt.tight_layout()
plt.show()

print("data loaded")

