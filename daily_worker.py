import os
import json
import ee
from supabase import create_client

# =========================
# ENV VALIDATION
# =========================
REQUIRED_ENV = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "EE_SERVICE_ACCOUNT_JSON",
]

missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if missing:
    for k in missing:
        print(f"❌ Missing env: {k}")
    raise RuntimeError("Environment validation failed")

# =========================
# INIT SUPABASE
# =========================
supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_ROLE_KEY"]
)

# =========================
# INIT GEE
# =========================
print("🚀 Initializing Google Earth Engine...")
creds = json.loads(os.environ["EE_SERVICE_ACCOUNT_JSON"])

ee.Initialize(
    ee.ServiceAccountCredentials(
        creds["client_email"],
        key_data=json.dumps(creds)
    )
)
print("✅ GEE initialized successfully")

# =========================
# MAIN WORKER
# =========================
def run():
    print("🛰 Fetching plots directly from Supabase...")

    plots = (
        supabase
        .table("plots")
        .select("id, plot_name, geometry")
        .execute()
        .data
    )

    print(f"📍 Found {len(plots)} plots")

    for plot in plots:
        plot_id = plot["id"]
        plot_name = plot["plot_name"]
        geometry = plot["geometry"]

        print(f"\n🌱 Processing plot: {plot_name}")

        if not geometry:
            print("⚠️ No geometry, skipping")
            continue

        try:
            # ✅ Convert GeoJSON → GEE Geometry
            ee_geom = ee.Geometry(geometry)

            # ✅ Area (required by your earlier failures)
            area_ha = ee_geom.area(maxError=1).divide(10000).getInfo()
            print(f"📐 Area: {area_ha:.2f} ha")

            # ✅ Fast, safe Sentinel-2 fetch
            img = (
                ee.ImageCollection("COPERNICUS/S2_SR")
                .filterBounds(ee_geom)
                .filterDate("2024-01-01", "2024-12-31")
                .sort("CLOUDY_PIXEL_PERCENTAGE")
                .first()
            )

            if img is None:
                print("⚠️ No imagery, skipping")
                continue

            ndvi = img.normalizedDifference(["B8", "B4"]).rename("NDVI")

            mean_ndvi = ndvi.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=ee_geom,
                scale=10,
                maxPixels=1e9
            ).get("NDVI").getInfo()

            print(f"🌿 NDVI: {mean_ndvi}")

            # ✅ Store result (idempotent-safe)
            supabase.table("plot_metrics").insert({
                "plot_id": plot_id,
                "plot_name": plot_name,
                "area_ha": area_ha,
                "ndvi": mean_ndvi
            }).execute()

            print("✅ Stored successfully")

        except ee.EEException as e:
            print(f"❌ GEE error (skipped): {e}")

        except Exception as e:
            print(f"❌ Unexpected error (skipped): {e}")

# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    run()
