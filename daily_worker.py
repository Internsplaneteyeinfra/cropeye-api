import os
import json
import requests
import ee
from datetime import date
from supabase import create_client
from gee_growth import run_growth_analysis_by_plot

# =====================================================
# REQUIRED ENV
# =====================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FASTAPI_PLOTS_URL = os.getenv("FASTAPI_PLOTS_URL")  # ❗ MUST be JSON endpoint, NOT /docs
WORKER_TOKEN = os.getenv("WORKER_TOKEN")
EE_JSON = os.getenv("EE_SERVICE_ACCOUNT_JSON")

required = {
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_SERVICE_ROLE_KEY": SUPABASE_SERVICE_ROLE_KEY,
    "FASTAPI_PLOTS_URL": FASTAPI_PLOTS_URL,
    "WORKER_TOKEN": WORKER_TOKEN,
    "EE_SERVICE_ACCOUNT_JSON": EE_JSON,
}

missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"❌ Missing env vars: {missing}")

# =====================================================
# SUPABASE
# =====================================================
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# =====================================================
# GOOGLE EARTH ENGINE (SERVICE ACCOUNT)
# =====================================================
print("🚀 Initializing Google Earth Engine...")

sa = json.loads(EE_JSON)

credentials = ee.ServiceAccountCredentials(
    sa["client_email"],
    key_data=json.dumps(sa)
)

ee.Initialize(credentials, project=sa["project_id"])
print("✅ GEE initialized successfully")

# =====================================================
# MAIN WORKER
# =====================================================
def run():
    print("🛰 Fetching plots from API...")

    res = requests.get(
        FASTAPI_PLOTS_URL,
        headers={"x-worker-token": WORKER_TOKEN},
        timeout=30
    )
    res.raise_for_status()

    plots = res.json()

    # ---------- HARD VALIDATION ----------
    if not isinstance(plots, list):
        raise RuntimeError(
            f"❌ Expected list from API, got {type(plots)}.\n"
            f"Check FASTAPI_PLOTS_URL: {FASTAPI_PLOTS_URL}"
        )

    print(f"📍 Found {len(plots)} plots")

    # =================================================
    # LOOP OVER LIST OF PLOTS
    # =================================================
    for plot in plots:
        if not isinstance(plot, dict):
            print("⚠️ Skipping invalid plot:", plot)
            continue

        plot_name = plot.get("plot_name")
        geometry = plot.get("geometry")

        if not plot_name or not geometry:
            print("⚠️ Missing plot_name or geometry, skipping:", plot)
            continue

        print(f"\n🌱 Processing plot: {plot_name}")

        # ---------------- Supabase plot_id ----------------
        db_plot = (
            supabase
            .table("plots")
            .select("id")
            .eq("plot_name", plot_name)
            .execute()
        )

        if not db_plot.data:
            print("❌ Plot not found in Supabase:", plot_name)
            continue

        plot_id = db_plot.data[0]["id"]

        # ---------------- GEE ANALYSIS ----------------
        try:
            result = run_growth_analysis_by_plot(
                plot_data=plot,
                start_date="2025-01-01",
                end_date=str(date.today())
            )
        except Exception as e:
            print("❌ GEE failed for", plot_name, ":", e)
            continue

        analysis_date = result["analysis_date"]

        # ---------------- SKIP IF EXISTS ----------------
        cached = (
            supabase
            .table("analysis_results")
            .select("id")
            .eq("plot_id", plot_id)
            .eq("analysis_type", "growth")
            .eq("analysis_date", analysis_date)
            .execute()
        )

        if cached.data:
            print("⏭ Already cached:", plot_name, analysis_date)
            continue

        # ---------------- SATELLITE IMAGE ----------------
        sat = supabase.table("satellite_images").insert({
            "plot_id": plot_id,
            "satellite": result["sensor"],
            "satellite_date": analysis_date
        }).execute()

        sat_id = sat.data[0]["id"]

        # ---------------- STORE RESULT ----------------
        supabase.table("analysis_results").insert({
            "plot_id": plot_id,
            "satellite_image_id": sat_id,
            "analysis_type": "growth",
            "analysis_date": analysis_date,
            "sensor_used": result["sensor"],
            "tile_url": result["tile_url"],
            "response_json": result["response_json"]
        }).execute()

        print("✅ Stored growth for", plot_name, analysis_date)

# =====================================================
# ENTRYPOINT
# =====================================================
if __name__ == "__main__":
    run()
