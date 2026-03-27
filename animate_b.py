"""
animate_b.py
------------
Animation B: Stop x Day Travel Time Heatmap
"""
STATION_NAMES = {
    "place-alfcl": "Alewife",
    "place-davis": "Davis",
    "place-portr": "Porter",
    "place-harsq": "Harvard",
    "place-cntsq": "Central",
    "place-knncl": "Kendall/MIT",
    "place-chmnl": "Charles/MGH",
    "place-pktrm": "Park Street",
    "place-dwnxg": "Downtown Crossing",
    "place-sstat": "South Station",
    "place-brdwy": "Broadway",
    "place-andrw": "Andrew",
    "place-jfk": "JFK/UMass",
    "place-smmnl": "Savin Hill",
    "place-fldcr": "Fields Corner",
    "place-shmnl": "Shawmut",
    "place-asmnl": "Ashmont",
    "place-nqncy": "North Quincy",
    "place-wlsta": "Wollaston",
    "place-qnctr": "Quincy Center",
    "place-qamnl": "Quincy Adams",
    "place-brntn": "Braintree",
}

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, FFMpegWriter

from acquire import get_clean_dataframe, get_stop_order
from model import SubwayLine


def update(frame, full_matrix, current_matrix, im):
    """
    Frame i reveals column i (day i of February)
    """
    current_matrix[:, frame] = full_matrix[:, frame]
    im.set_array(current_matrix)
    return [im]


def main():
    # Load model (NO raw processing here)
    df = get_clean_dataframe()
    stop_order = get_stop_order()

    line = SubwayLine(
        route_name="Red Line",
        route_id="Red",
        df=df,
        stop_order=stop_order
    )

    # Use computed field ONLY
    heatmap_df = line.travel_by_stop_and_day

    # Convert to numpy
    full_matrix = heatmap_df.to_numpy()

    # Initialize with NaN
    current_matrix = np.full(full_matrix.shape, np.nan)

    # Fixed color scale (important)
    vmin = np.nanpercentile(full_matrix, 5)
    vmax = np.nanpercentile(full_matrix, 95)

    # Plot
    fig, ax = plt.subplots(figsize=(12, 8))

    im = ax.imshow(
        current_matrix,
        aspect="auto",
        cmap="viridis",
        vmin=vmin,
        vmax=vmax
    )

    im.cmap.set_bad(color="lightgray")

    # Labels
    ax.set_title("MBTA Red Line: Travel Time by Stop (Feb 2026)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Station")

    # Reduce clutter on x-axis (every 4 days)
    ax.set_xticks(range(0, len(line.dates), 4))
    ax.set_xticklabels(line.dates[::4], rotation=90)

    station_labels = [STATION_NAMES[stop] for stop in line.stops]
    ax.set_yticks(range(len(line.stops)))
    ax.set_yticklabels(station_labels)

    # Colorbar
    plt.colorbar(im, ax=ax, label="Travel Time (seconds)")

    plt.tight_layout()

    # Animation
    anim = FuncAnimation(
        fig,
        update,
        frames=len(line.dates),
        fargs=(full_matrix, current_matrix, im),
        interval=300,
        blit=True
    )

    # Save (REQUIRES ffmpeg installed)
    writer = FFMpegWriter(fps=2)
    anim.save("mbta_red_animation_b.mp4", writer=writer)

    plt.show()


if __name__ == "__main__":
    main()