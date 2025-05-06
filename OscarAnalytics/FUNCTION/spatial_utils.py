# FUNCTION/spatial_utils.py

import os
import geopandas as gpd
import pandas as pd


def compute_weighted_income(
    lea_shp_path: str,
    tract_shp_path: str,
    tract_csv_path: str,
    lea_id_shp_col: str = "GEOID",
    tract_id_shp_col: str = "GEOID",
    tract_geo_id: str = "GEOID",
    income_col: str = "median_income",
    buffer_miles: float = 0,
    csv_header: int = 0
) -> pd.DataFrame:
    """
    Computes an area‐weighted average income per LEA by intersecting
    LEA polygons with census tracts—using a single vectorized overlay.

    Returns a DataFrame with columns ["lea_id", "weighted_income"].
    """

    # 1) check files
    for path in (lea_shp_path, tract_shp_path, tract_csv_path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
    print("✅ Files found.")

    # 2) load LEA polygons
    print(f"📥 Loading LEAs from {lea_shp_path}")
    leagdf = gpd.read_file(lea_shp_path)
    if lea_id_shp_col not in leagdf.columns:
        raise KeyError(f"LEA .shp has no '{lea_id_shp_col}'. Columns: {list(leagdf.columns)}")
    leagdf = leagdf[[lea_id_shp_col, "geometry"]].rename(columns={lea_id_shp_col: "lea_id"})
    print(f"   → {len(leagdf)} LEA polygons loaded.")

    # 3) load tract polygons
    print(f"📥 Loading tracts from {tract_shp_path}")
    tractgdf = gpd.read_file(tract_shp_path)
    if tract_id_shp_col not in tractgdf.columns:
        raise KeyError(f"Tract .shp has no '{tract_id_shp_col}'. Columns: {list(tractgdf.columns)}")
    tractgdf = tractgdf[[tract_id_shp_col, "geometry"]].rename(columns={tract_id_shp_col: tract_geo_id})
    print(f"   → {len(tractgdf)} tract polygons loaded.")

    # 4) load income CSV
    print(f"📥 Reading CSV {tract_csv_path} (header row = {csv_header})")
    income_df = pd.read_csv(tract_csv_path, header=csv_header, dtype={tract_geo_id: str})
    for col in (tract_geo_id, income_col):
        if col not in income_df.columns:
            raise KeyError(f"CSV missing '{col}'. Columns: {list(income_df.columns)}")
    print(f"   → {len(income_df)} CSV rows read. Merging…")
    tractgdf = tractgdf.merge(income_df[[tract_geo_id, income_col]], on=tract_geo_id, how="inner")
    print(f"   → {len(tractgdf)} tracts after merge.")

    # 5) reproject to meters for accurate areas
    print("🔄 Reprojecting to EPSG:3857")
    leagdf = leagdf.to_crs(epsg=3857)
    tractgdf = tractgdf.to_crs(epsg=3857)

    # 6) optional buffer
    if buffer_miles > 0:
        meters = buffer_miles * 1609.34
        print(f"🔧 Buffering LEAs by {buffer_miles} mi ({meters:.0f} m)")
        leagdf["geometry"] = leagdf.geometry.buffer(meters)

    # 7) compute intersections via overlay
    print("🔀 Performing spatial overlay (intersection)…")
    overlay = gpd.overlay(
        leagdf,
        tractgdf,
        how="intersection"
    ).rename(columns={income_col: "inc_value"})
    print(f"   → {len(overlay)} intersected pieces")

    # 8) compute areas and weighted incomes
    print("⚖️  Computing intersection areas and weights…")
    overlay["area"] = overlay.geometry.area
    overlay["weighted_inc"] = overlay.inc_value * overlay.area

    # 9) aggregate per LEA
    print("📊 Aggregating to area‐weighted average per LEA…")
    agg = (
        overlay.groupby("lea_id")
               .agg(total_area=("area","sum"),
                    total_income=("weighted_inc","sum"))
               .reset_index()
    )
    agg["weighted_income"] = agg["total_income"] / agg["total_area"]

    # 10) fill in LEAs with no overlaps
    missing = set(leagdf["lea_id"]) - set(agg["lea_id"])
    if missing:
        print(f"⚠️  {len(missing)} LEAs had no intersecting tracts; filling with NaN")
        df_missing = pd.DataFrame({"lea_id": list(missing), "weighted_income": [pd.NA]*len(missing)})
        result = pd.concat([agg[["lea_id","weighted_income"]], df_missing], ignore_index=True)
    else:
        result = agg[["lea_id","weighted_income"]]

    print("✅ Done computing weighted incomes.")
    return result


if __name__ == "__main__":
    print("▶️ Running compute_weighted_income and exporting results…")
    df = compute_weighted_income(
        lea_shp_path    = r"C:\Users\oscar\Documents\UTSA\Data Analytics\EDGE_SCHOOLDISTRICT_TL18_SY1718\schooldistrict_sy1718_tl18.shp",
        tract_shp_path  = r"C:\Users\oscar\Documents\UTSA\Data Analytics\cb_2022_us_tract_500k\cb_2022_us_tract_500k.shp",
        tract_csv_path  = r"C:\Users\oscar\Documents\UTSA\Data Analytics\Project\data\tract_kfr_rP_gP_pall.csv",
        lea_id_shp_col   = "GEOID",
        tract_id_shp_col = "GEOID",
        tract_geo_id     = "tract",
        income_col       = "Household_Income_at_Age_35_rP_gP_pall",
        buffer_miles     = 0.5,
        csv_header       = 0
    )
    print(df.head())

    out_csv = os.path.join(os.getcwd(), "weighted_income_by_lea.csv")
    df.to_csv(out_csv, index=False)
    print(f"📁 Exported full results to {out_csv}")
