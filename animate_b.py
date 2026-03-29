"""
animate_b.py
------------
Take SubwayLine computed fields and produce
an animated heatmap of mean segment travel time by stop and day
across February 2026, revealing one column per frame.
"""

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, FFMpegWriter

from acquire import get_clean_dataframe, get_stop_order
from model import SubwayLine


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

def update(frame, full_matrix, current_matrix, im):
    """
    Reveals column i of the heatmap by copying day i from the full matrix
    into the current matrix and calling set_array() on the image artist.
    Args: frame (int), full_matrix (ndarray), current_matrix (ndarray), im (AxesImage)
    Returns: list containing the updated AxesImage artist
    """
    current_matrix[:, frame] = full_matrix[:, frame]
    im.set_array(current_matrix)
    return [im]


def main():
    """
    Builds the SubwayLine model, extracts the travel_by_stop_and_day pivot
    table, sets up the heatmap figure with fixed color scale and station
    labels, and runs FuncAnimation to produce the animated mp4
    """

    df = get_clean_dataframe()
    stop_order = get_stop_order()

    line = SubwayLine(
        route_name="Red Line",
        route_id="Red",
        df=df,
        stop_order=stop_order
    )

    heatmap_df = line.travel_by_stop_and_day

    full_matrix = heatmap_df.to_numpy()

    current_matrix = np.full(full_matrix.shape, np.nan)

    vmin = np.nanpercentile(full_matrix, 5)
    vmax = np.nanpercentile(full_matrix, 95)

    fig, ax = plt.subplots(figsize=(12, 8))

    im = ax.imshow(
        current_matrix,
        aspect="auto",
        cmap="viridis",
        vmin=vmin,
        vmax=vmax
    )

    im.cmap.set_bad(color="lightgray")

    ax.set_title("MBTA Red Line: Travel Time by Stop (Feb 2026)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Station")

    ax.set_xticks(range(0, len(line.dates), 4))
    ax.set_xticklabels(line.dates[::4], rotation=90)

    station_labels = [STATION_NAMES[stop] for stop in line.stops]
    ax.set_yticks(range(len(line.stops)))
    ax.set_yticklabels(station_labels)

    plt.colorbar(im, ax=ax, label="Travel Time (seconds)")

    plt.tight_layout()

    anim = FuncAnimation(
        fig,
        update,
        frames=len(line.dates),
        fargs=(full_matrix, current_matrix, im),
        interval=300,
        blit=True
    )

    writer = FFMpegWriter(fps=2)
    anim.save("mbta_red_animation_b.mp4", writer=writer)
    print("mbta_red_animation_b.mp4 saved to folder")

    plt.show()


if __name__ == "__main__":
    main()