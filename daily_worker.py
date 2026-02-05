import os
import json
import requests
import ee
from datetime import date
from supabase import create_client
from gee_growth import run_growth_analysis_by_plot

# =====================================================
# ENVIRONMENT VARIABLES (REQUIRED)
# =====================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FASTAPI_PLOTS_URL = os.getenv("FASTAPI_PLOTS_URL")   # e.g. https://cropeye-api.onrender.com/plots
WORKER_TOKEN = os.getenv("WORKER_TOKEN")
EE_JSON = os.getenv("EE_SERVICE_ACCOUNT_JSON")

missing = [
    name for name, value in {
        "SUPABASE_URL": SUPABASE_URL,
        "SUPABASE_SERVICE_ROLE_KEY": SUPABASE_SERVICE_ROLE_KEY,
        "FASTAPI_PLOTS_URL": FASTAPI_PLOTS_URL,
        "WORKER_TOKEN": WORKER_TOKEN,
        "EE_SERVICE_ACCOUNT_JSON": EE_JSON,
    }.items() if not value
]

if missing:
    raise RuntimeError(f"❌ Missing environment variables: {missing}")

# =====================================================
# SUPABASE CLIENT
# =====================================================
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# =====================================================
# GOOGLE EARTH ENGINE INIT (SERVICE ACCOUNT – NO BROWSER)
# =====================================================
print("🚀 Initializing Google Earth Engine...")

service_account_info = json.loads(EE_JSON)

credentials = ee.ServiceAccountCredentials(
    service_account_info["client_email"],
    key_data=json.dumps(service_account_info)
)

ee.Initialize(credentials, project=service_account_info["project_id"])

print("✅ GEE initialized successfully")

# =====================================================
# MAIN WORKER
# =====================================================
def run():
    print("🛰 Fetching plots from API...")

    response = requests.get(
        FASTAPI_PLOTS_URL,
        headers={"x-worker-token": WORKER_TOKEN},
        timeout=30
    )

    response.raise_for_status()
    plots = response.json()   # 🔑 THIS IS A DICT

    print(f"📍 Found {len(plots)} plots")

    # -------------------------------------------------
    # plots = { "999_11": {...}, "1001_5": {...} }
    # -------------------------------------------------
    for plot_name, plot_data in plots.items():
        print(f"\n🌱 Processing plot: {plot_name}")

        # ---------------- Get plot_id from Supabase ----------------
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
                plot_data=plot_data,   # ✅ geometry comes from API
                start_date="2025-01-01",
                end_date=str(date.today())
            )
        except Exception as e:
            print("❌ GEE failed:", e)
            continue

        analysis_date = result["analysis_date"]

        # ---------------- SKIP IF ALREADY EXISTS ----------------
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
            print("⏭ Already cached for", analysis_date)
            continue

        # ---------------- SATELLITE IMAGE ROW ----------------
        sat = (
            supabase
            .table("satellite_images")
            .insert({
                "plot_id": plot_id,
                "satellite": result["sensor"],
                "satellite_date": analysis_date
            })
            .execute()
        )

        sat_id = sat.data[0]["id"]

        # ---------------- STORE ANALYSIS ----------------
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
