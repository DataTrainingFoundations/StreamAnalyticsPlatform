from pathlib import Path
import math
import pandas as pd
import numpy as np
import plotly.graph_objects as go

MAX_SITES = 18

MONITOR_FILE = Path("Monitoring_Site_Locations_V2.dat")
ACTIVE_SITES_FILE = Path("active_sites_union.txt")

OUT_PY = Path("regenerated_bbox_list_active_4.py")
OUT_TXT = Path("regenerated_bbox_list_active_4.txt")
OUT_CSV = Path("regenerated_bbox_list_active_4.csv")
OUT_MAP = Path("regenerated_bbox_list_active_map_4.html")


def load_monitoring_sites() -> pd.DataFrame:
    df = pd.read_csv(MONITOR_FILE, sep="|", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

    df = df[
        df["CountryFIPS"].astype(str).str.upper().eq("US")
        & df["Latitude"].between(18, 72)
        & df["Longitude"].between(-170, -65)
        & ~((df["Latitude"] == 0) & (df["Longitude"] == 0))
    ].drop_duplicates(subset=["FullAQSID"]).copy()

    return df


def load_active_site_ids() -> set[str]:
    with ACTIVE_SITES_FILE.open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def assign_region(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Geographic isolation to avoid ocean-spanning boxes
    df["region"] = np.select(
        [
            df["Latitude"] >= 50,  # Alaska-like
            (df["Latitude"] < 23) & (df["Longitude"] < -154),  # Hawaii-like
            (df["Latitude"] < 19.5) & (df["Longitude"] > -68.5),  # PR-like
        ],
        ["AK", "HI", "PR"],
        default="MAIN",
    )

    return df


def partition_balanced(df: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Recursively split into non-overlapping geographic groups with:
    - <= MAX_SITES sites
    - bounded geographic span

    A group is accepted only if it satisfies both:
    1. site count <= MAX_SITES
    2. lat/lon span <= configured max span
    """

    MAX_LAT_SPAN = 6.0
    MAX_LON_SPAN = 6.0

    leaves: list[pd.DataFrame] = []

    def recurse(chunk: pd.DataFrame) -> None:
        n = len(chunk)

        lat_span = float(chunk["Latitude"].max() - chunk["Latitude"].min())
        lon_span = float(chunk["Longitude"].max() - chunk["Longitude"].min())

        # Accept only if both site count and geographic spread are safe
        if n <= MAX_SITES and lat_span <= MAX_LAT_SPAN and lon_span <= MAX_LON_SPAN:
            leaves.append(chunk.copy())
            return

        # Decide preferred split axis:
        # prioritize whichever geographic span is worse relative to its threshold
        lat_ratio = lat_span / MAX_LAT_SPAN if MAX_LAT_SPAN > 0 else 0
        lon_ratio = lon_span / MAX_LON_SPAN if MAX_LON_SPAN > 0 else 0

        if lon_ratio >= lat_ratio:
            axis_order = ["Longitude", "Latitude"]
        else:
            axis_order = ["Latitude", "Longitude"]

        # If too many sites, target a balanced count split
        groups = math.ceil(n / MAX_SITES)
        target = math.ceil(n / groups) if groups > 1 else max(1, n // 2)

        for axis in axis_order:
            other = "Latitude" if axis == "Longitude" else "Longitude"
            ordered = chunk.sort_values([axis, other, "FullAQSID"]).reset_index(drop=True)
            vals = ordered[axis].tolist()

            # If geographic span is too large, prefer a midpoint-ish split
            cut = None
            midpoint = len(ordered) // 2

            # Try around midpoint first if span is the reason we need splitting
            for dist in range(0, len(vals)):
                for idx in (midpoint - dist, midpoint + dist):
                    if 1 <= idx < len(vals) and vals[idx - 1] != vals[idx]:
                        cut = idx
                        break
                if cut is not None:
                    break

            # If midpoint split failed, try around target site count
            if cut is None:
                for dist in range(0, len(vals)):
                    for idx in (target - dist, target + dist):
                        if 1 <= idx < len(vals) and vals[idx - 1] != vals[idx]:
                            cut = idx
                            break
                    if cut is not None:
                        break

            if cut is not None:
                left = ordered.iloc[:cut].copy()
                right = ordered.iloc[cut:].copy()

                if len(left) > 0 and len(right) > 0:
                    recurse(left)
                    recurse(right)
                    return

        # Fallback: hard split if all coordinates are too tied
        ordered = chunk.sort_values(["Longitude", "Latitude", "FullAQSID"]).reset_index(drop=True)
        midpoint = max(1, len(ordered) // 2)

        left = ordered.iloc[:midpoint].copy()
        right = ordered.iloc[midpoint:].copy()

        if len(left) == 0 or len(right) == 0:
            leaves.append(chunk.copy())
            return

        recurse(left)
        recurse(right)

    recurse(df)
    return leaves   


def boxes_from_leaves(leaves: list[pd.DataFrame]) -> pd.DataFrame:
    rows = []

    for i, leaf in enumerate(leaves, start=1):
        min_lon = float(leaf["Longitude"].min())
        min_lat = float(leaf["Latitude"].min())
        max_lon = float(leaf["Longitude"].max())
        max_lat = float(leaf["Latitude"].max())

        bbox = f"{min_lon:g},{min_lat:g},{max_lon:g},{max_lat:g}"

        rows.append(
            {
                "box_id": i,
                "bbox_string": bbox,
                "site_count": int(len(leaf)),
                "region": str(leaf["region"].iloc[0]),
            }
        )

    return pd.DataFrame(rows)


def write_outputs(boxes: pd.DataFrame) -> None:
    with OUT_PY.open("w", encoding="utf-8") as f:
        f.write("BBOXES = [\n")
        for bbox in boxes["bbox_string"]:
            f.write(f'    "{bbox}",\n')
        f.write("]\n")

    with OUT_TXT.open("w", encoding="utf-8") as f:
        for bbox in boxes["bbox_string"]:
            f.write(f"{bbox}\n")

    boxes.to_csv(OUT_CSV, index=False)


def build_map(sites: pd.DataFrame, boxes: pd.DataFrame) -> None:
    fig = go.Figure()

    fig.add_trace(
        go.Scattergeo(
            lon=sites["Longitude"],
            lat=sites["Latitude"],
            mode="markers",
            marker=dict(size=3),
            text=sites["FullAQSID"].astype(str),
            hovertemplate="Site %{text}<br>Lat %{lat:.3f} Lon %{lon:.3f}<extra></extra>",
            name="Active monitoring sites",
            opacity=0.65,
        )
    )

    for i, bbox in enumerate(boxes["bbox_string"], start=1):
        min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(","))
        lons = [min_lon, max_lon, max_lon, min_lon, min_lon]
        lats = [min_lat, min_lat, max_lat, max_lat, min_lat]

        fig.add_trace(
            go.Scattergeo(
                lon=lons,
                lat=lats,
                mode="lines",
                line=dict(width=1),
                hovertemplate=f"Box {i}<br>{bbox}<extra></extra>",
                showlegend=False,
            )
        )

    fig.update_layout(
        title="Regenerated Non-overlapping BBoxes from Active AirNow Sites",
        geo=dict(
            scope="usa",
            projection_type="albers usa",
            showland=True,
            showlakes=True,
            showsubunits=True,
            fitbounds="locations",
        ),
        height=900,
    )

    fig.write_html(OUT_MAP, include_plotlyjs="cdn")


def main() -> None:
    sites = load_monitoring_sites()
    active_ids = load_active_site_ids()

    # Match actual API load, not every possible metadata site
    sites = sites[sites["FullAQSID"].isin(active_ids)].copy()
    sites = assign_region(sites)

    leaves: list[pd.DataFrame] = []
    for _, grp in sites.groupby("region"):
        leaves.extend(partition_balanced(grp))

    boxes = boxes_from_leaves(leaves)
    print(f"Max latitude span: {(boxes['bbox_string'].apply(lambda b: float(b.split(',')[3]) - float(b.split(',')[1]))).max():.3f}")
    print(f"Max longitude span: {(boxes['bbox_string'].apply(lambda b: float(b.split(',')[2]) - float(b.split(',')[0]))).max():.3f}")
    write_outputs(boxes)
    build_map(sites, boxes)

    print(f"Active sites used: {len(sites)}")
    print(f"Boxes generated: {len(boxes)}")
    print("Site count distribution:")
    print(boxes["site_count"].value_counts().sort_index())
    print(f"Python list: {OUT_PY}")
    print(f"Text list: {OUT_TXT}")
    print(f"CSV: {OUT_CSV}")
    print(f"Map: {OUT_MAP}")


if __name__ == "__main__":
    main()