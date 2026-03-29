"""
animate_a.py
------------
Single responsibility: consume SubwayLine computed fields and produce
an animated line plot of actual vs. scheduled daily mean travel time
across February 2026. No data fetching, no aggregation.
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.animation import FuncAnimation
import pandas as pd

from acquire import get_clean_dataframe, get_stop_order
from model import SubwayLine


def main():
    df = get_clean_dataframe()
    stops = get_stop_order()

    subway_line = SubwayLine(
        route_name="Red Line",
        route_id="Red",
        df=df,
        stop_order=stops
    )

    dates = [pd.to_datetime(d) for d in subway_line.dates]
    actual_vals    = [subway_line.daily_avg_travel.get(d, float("nan"))    for d in subway_line.dates]
    scheduled_vals = [subway_line.daily_avg_scheduled.get(d, float("nan")) for d in subway_line.dates]

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.set_xlim(dates[0], dates[-1])
    ymin = min(v for v in actual_vals + scheduled_vals if not pd.isna(v)) * 0.9
    ymax = max(v for v in actual_vals + scheduled_vals if not pd.isna(v)) * 1.1
    ax.set_ylim(ymin, ymax)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    ax.set_xlabel("Date")
    ax.set_ylabel("Mean Travel Time (seconds)")
    ax.set_title("Red Line — Actual vs. Scheduled Daily Mean Travel Time\nFebruary 2026")

    ax.axvspan(
        pd.to_datetime("2026-02-23"),
        pd.to_datetime("2026-02-24"),
        color="steelblue", alpha=0.15, label="Blizzard (Feb 23–24)"
    )

    line_actual,    = ax.plot([], [], color="crimson",    linewidth=2,
                              marker="o", markersize=4, label="Actual")
    line_scheduled, = ax.plot([], [], color="steelblue",  linewidth=2,
                              linestyle="--", marker="o", markersize=4,
                              label="Scheduled")

    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    def animate(frame, dates, actual_vals, scheduled_vals):
        x = dates[:frame + 1]
        line_actual.set_data(x, actual_vals[:frame + 1])
        line_scheduled.set_data(x, scheduled_vals[:frame + 1])
        return line_actual, line_scheduled

    anim = FuncAnimation(
        fig,
        animate,
        frames=len(dates),
        fargs=(dates, actual_vals, scheduled_vals),
        interval=200,
        blit=True
    )

    anim.save("mbta_red_animation_a.mp4", writer="ffmpeg", fps=5, dpi=150)
    print("Saved mbta_red_animation_a.mp4")
    plt.show()

if __name__ == "__main__":
    main()