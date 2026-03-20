import os
import time
import json
import threading
import pandas as pd
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timedelta

from scraper_tech import search_maps_all_pages, flatten_result
from clay_webhook import send_to_clay
from apify_actor import run_actor_with_csv, upload_to_kv_store

# --- Config ---
ZIP_CSV = Path(__file__).parent / "us_zip_codes.csv"
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)
APIFY_LOG_FILE = RESULTS_DIR / "apify_log.json"
JOB_STATUS_FILE = RESULTS_DIR / "scrape_job.json"


def load_apify_log():
    if APIFY_LOG_FILE.exists():
        return json.loads(APIFY_LOG_FILE.read_text())
    return {}


def save_apify_log(log: dict):
    APIFY_LOG_FILE.write_text(json.dumps(log, indent=2))


def build_csv_filename(keyword, market, scope, states=None, cities=None, count=0):
    parts = ["gmaps", keyword.replace(" ", "-")]

    if market == "Europe":
        parts.append("EU")
    else:
        if scope == "Entire US":
            parts.append("US-all")
        elif scope == "Specific States" and states:
            state_str = "-".join(s.replace(" ", "") for s in states[:5])
            parts.append(state_str)
            if len(states) > 5:
                parts.append(f"+{len(states)-5}more")
        elif scope == "Specific Cities" and cities:
            city_str = "-".join(c.replace(" ", "") for c in cities[:3])
            parts.append(city_str)
            if len(cities) > 3:
                parts.append(f"+{len(cities)-3}more")

    parts.append(f"{count}results")
    parts.append(datetime.now().strftime("%Y%m%d_%H%M%S"))

    return "_".join(parts) + ".csv"


# --- Job status ---
def save_job_status(status: dict):
    status["updated_at"] = datetime.now().isoformat()
    JOB_STATUS_FILE.write_text(json.dumps(status, indent=2))


def load_job_status() -> dict | None:
    if JOB_STATUS_FILE.exists():
        try:
            return json.loads(JOB_STATUS_FILE.read_text())
        except (json.JSONDecodeError, IOError):
            return None
    return None


def clear_job_status():
    if JOB_STATUS_FILE.exists():
        JOB_STATUS_FILE.unlink()


def is_job_stale(job: dict, max_age_minutes: int = 5) -> bool:
    """Check if a 'running' job is stale (thread died from app sleep/restart)."""
    updated = job.get("updated_at")
    if not updated:
        return True
    try:
        last_update = datetime.fromisoformat(updated)
        return datetime.now() - last_update > timedelta(minutes=max_age_minutes)
    except (ValueError, TypeError):
        return True


def upload_result_to_cloud(filepath: Path):
    """Upload a CSV result to Apify KV store for backup."""
    try:
        csv_data = filepath.read_text()
        upload_to_kv_store(filepath.name, csv_data)
    except Exception:
        pass


# --- Password Protection ---
def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    password = st.secrets.get("APP_PASSWORD", "")
    if not password:
        return True

    st.markdown("### Please enter the password to continue")
    entered = st.text_input("Password", type="password")
    if st.button("Login", use_container_width=True):
        if entered == password:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")
    return False


@st.cache_data
def load_zip_codes():
    df = pd.read_csv(ZIP_CSV, dtype={"zipcode": str})
    return df


def get_states(df):
    return sorted(df["state"].unique().tolist())


def get_cities(df, states=None):
    if states:
        df = df[df["state"].isin(states)]
    cities = df["city"].dropna().unique().tolist()
    return sorted([str(c) for c in cities])


def filter_zip_codes(df, scope, selected_states=None, selected_cities=None,
                     excluded_states=None, excluded_cities=None):
    filtered = df.copy()

    if scope == "Specific States" and selected_states:
        filtered = filtered[filtered["state"].isin(selected_states)]
    elif scope == "Specific Cities" and selected_cities:
        filtered = filtered[filtered["city"].isin(selected_cities)]

    if excluded_states:
        filtered = filtered[~filtered["state"].isin(excluded_states)]
    if excluded_cities:
        filtered = filtered[~filtered["city"].isin(excluded_cities)]

    return filtered


def load_past_scraped_data(keyword: str = None) -> tuple[set, set]:
    """Load place_ids and scraped zip codes from past CSVs."""
    place_ids = set()
    scraped_zips = set()
    for csv_file in RESULTS_DIR.glob("*.csv"):
        try:
            cols_to_load = ["place_id"]
            header = pd.read_csv(csv_file, nrows=0).columns.tolist()
            has_keyword = "search_keyword" in header
            has_zip = "source_zip" in header

            if has_zip:
                cols_to_load.append("source_zip")
            if has_keyword:
                cols_to_load.append("search_keyword")

            df = pd.read_csv(csv_file, usecols=cols_to_load, dtype=str)
            place_ids.update(df["place_id"].dropna().tolist())

            if has_zip and keyword:
                if has_keyword:
                    matched = df[df["search_keyword"].str.lower() == keyword.lower()]
                    scraped_zips.update(matched["source_zip"].dropna().tolist())
        except (ValueError, KeyError):
            pass
    return place_ids, scraped_zips


# --- Background scraper ---
_scrape_lock = threading.Lock()
_stop_event = threading.Event()


def _fetch_zip(keyword, zip_code, country):
    query = f"{keyword} in {zip_code}"
    return search_maps_all_pages(query=query, country=country)


def _run_scrape_background(keyword, zip_rows, country, excluded_categories,
                           initial_seen_ids, output_filepath, delay, max_results,
                           max_workers):
    if not _scrape_lock.acquire(blocking=False):
        save_job_status({"status": "error", "message": "Another scrape is already running."})
        return

    _stop_event.clear()

    try:
        seen_place_ids = set(initial_seen_ids) if initial_seen_ids else set()
        lock = threading.Lock()
        total = len(zip_rows)
        completed = 0
        errors = 0
        found = 0
        header_written = False

        save_job_status({
            "status": "running",
            "completed": 0,
            "total": total,
            "found": 0,
            "errors": 0,
            "output_file": str(output_filepath),
            "started_at": datetime.now().isoformat(),
        })

        def save_batch_to_disk(batch_results):
            nonlocal header_written
            if not batch_results:
                return
            df_batch = pd.DataFrame(batch_results)
            if not header_written:
                df_batch.to_csv(output_filepath, index=False, mode="w")
                header_written = True
            else:
                df_batch.to_csv(output_filepath, index=False, mode="a", header=False)

        def process_result(r, zip_code, city, state):
            place_id = r.get("place_id", r.get("business_id", ""))
            with lock:
                if place_id and place_id in seen_place_ids:
                    return None
                if place_id:
                    seen_place_ids.add(place_id)

            types = r.get("types", [])
            if isinstance(types, str):
                types = [t.strip() for t in types.split(",")]
            if excluded_categories:
                excluded_lower = [c.lower().strip() for c in excluded_categories]
                if any(t.lower() in excluded_lower for t in types):
                    return None

            flat = flatten_result(r)
            flat["search_keyword"] = keyword
            flat["source_zip"] = zip_code
            flat["source_city"] = city
            flat["source_state"] = state
            return flat

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            batch_start = 0
            batch_size = max_workers

            while batch_start < total:
                if _stop_event.is_set():
                    save_job_status({
                        "status": "stopped",
                        "completed": completed,
                        "total": total,
                        "found": found,
                        "errors": errors,
                        "output_file": str(output_filepath),
                        "message": "Stopped by user.",
                    })
                    # Upload partial results to cloud
                    upload_result_to_cloud(output_filepath)
                    return

                if max_results > 0 and found >= max_results:
                    break

                batch_end = min(batch_start + batch_size, total)
                futures = {}
                for row_data in zip_rows[batch_start:batch_end]:
                    zip_code, city, state = row_data
                    future = executor.submit(_fetch_zip, keyword, zip_code, country)
                    futures[future] = (zip_code, city, state)

                batch_new = []
                for future in as_completed(futures):
                    zip_code, city, state = futures[future]
                    completed += 1

                    try:
                        results = future.result()
                        for r in results:
                            flat = process_result(r, zip_code, city, state)
                            if flat:
                                batch_new.append(flat)
                                found += 1
                                if max_results > 0 and found >= max_results:
                                    break
                    except Exception:
                        errors += 1

                    save_job_status({
                        "status": "running",
                        "completed": completed,
                        "total": total,
                        "found": found,
                        "errors": errors,
                        "output_file": str(output_filepath),
                    })

                save_batch_to_disk(batch_new)

                if delay > 0:
                    time.sleep(delay)

                batch_start = batch_end

        # Upload completed results to cloud
        upload_result_to_cloud(output_filepath)

        save_job_status({
            "status": "completed",
            "completed": completed,
            "total": total,
            "found": found,
            "errors": errors,
            "output_file": str(output_filepath),
            "finished_at": datetime.now().isoformat(),
        })
    except Exception as e:
        save_job_status({
            "status": "error",
            "message": str(e),
            "output_file": str(output_filepath),
        })
        # Try to upload whatever we have
        if output_filepath.exists():
            upload_result_to_cloud(output_filepath)
    finally:
        _scrape_lock.release()


def start_scrape_job(keyword, zip_rows, country, excluded_categories,
                     initial_seen_ids, output_filepath, delay, max_results,
                     max_workers):
    t = threading.Thread(
        target=_run_scrape_background,
        args=(keyword, zip_rows, country, excluded_categories,
              initial_seen_ids, output_filepath, delay, max_results,
              max_workers),
        daemon=True,
    )
    t.start()


def stop_scrape_job():
    _stop_event.set()


# --- Streamlit UI ---
st.set_page_config(page_title="Google Maps Scraper", page_icon="🗺️", layout="wide")

if not check_password():
    st.stop()

st.markdown("""
<style>
    .stApp { max-width: 1200px; margin: 0 auto; }
    div[data-testid="stForm"] {
        border: 2px solid #5B5FC7;
        border-radius: 12px;
        padding: 24px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🗺️ Google Maps Scraper")
st.caption("Search for businesses across the United States or Europe")

# --- Sidebar: View Past Results (always rendered) ---
with st.sidebar:
    st.header("Past Scrapes")
    result_files = sorted(RESULTS_DIR.glob("*.csv"), reverse=True)
    if result_files:
        selected_file = st.selectbox(
            "Select a past scrape",
            result_files,
            format_func=lambda f: f"{f.name} ({f.stat().st_size/1024:.0f} KB)",
        )

        if selected_file:
            past_df = pd.read_csv(selected_file)
            st.metric("Rows", len(past_df))
            st.dataframe(past_df, use_container_width=True, height=300)

            st.download_button(
                "Download CSV",
                past_df.to_csv(index=False),
                file_name=selected_file.name,
                mime="text/csv",
                use_container_width=True,
                key="sidebar_download",
            )

            st.divider()
            past_clay_url = st.text_input(
                "Clay Webhook URL",
                value=os.getenv("CLAY_WEBHOOK_URL", ""),
                key="sidebar_clay_url",
            )
            if st.button("Send to Clay", use_container_width=True, key="sidebar_clay_btn"):
                if past_clay_url:
                    past_clay_progress = st.progress(0)
                    past_clay_status = st.empty()

                    def on_past_clay_progress(current, total, sent, errors):
                        past_clay_progress.progress(current / total)
                        past_clay_status.text(f"Row {current}/{total} — Sent: {sent} | Errors: {errors}")

                    try:
                        result = send_to_clay(
                            past_df.to_dict("records"),
                            webhook_url=past_clay_url,
                            on_progress=on_past_clay_progress,
                        )
                        past_clay_progress.progress(1.0)
                        st.success(f"Sent {result['sent']}/{result['total']} rows. Errors: {result['errors']}")
                    except Exception as e:
                        st.error(f"Failed: {e}")
                else:
                    st.warning("Enter a Clay webhook URL")

            st.divider()
            st.markdown("**Apify Actor**")

            sidebar_apify_log = load_apify_log()
            past_fname = selected_file.name
            if past_fname in sidebar_apify_log:
                prev = sidebar_apify_log[past_fname]
                st.success("Already sent to Apify")
                st.code(f"Run ID: {prev.get('run_id', '?')}\nSent: {prev.get('sent_at', '?')}")
                st.markdown(f"[View on Apify](https://console.apify.com/actors/runs/{prev.get('run_id', '')})")

            past_apify_actor = st.text_input(
                "Actor Name",
                value=os.getenv("APIFY_ACTOR_NAME", ""),
                placeholder="e.g., creator-account/csv---webhook",
                key="sidebar_apify_actor",
            )
            past_apify_wait = st.checkbox(
                "Wait for completion",
                value=False,
                key="sidebar_apify_wait",
            )

            btn_label = "Re-send to Apify" if past_fname in sidebar_apify_log else "Send to Apify"
            if st.button(btn_label, use_container_width=True, key="sidebar_apify_btn"):
                if past_apify_actor:
                    past_apify_progress = st.progress(0)
                    past_apify_status = st.empty()

                    try:
                        csv_content = past_df.to_csv(index=False)
                        past_apify_status.text("Starting Apify actor run...")

                        def on_past_apify_progress(msg):
                            past_apify_status.text(f"Status: {msg}")

                        max_wait = 300 if past_apify_wait else 0
                        result = run_actor_with_csv(
                            past_apify_actor,
                            csv_content,
                            file_key_name=past_fname,
                            poll_interval=5,
                            max_wait=max_wait,
                            on_progress=on_past_apify_progress,
                        )

                        if "error" in result:
                            past_apify_status.error(f"Error: {result['error']}")
                        else:
                            past_apify_progress.progress(1.0)
                            run_id = result.get("run_id", "unknown")
                            run_status = result.get("run_status", "")
                            file_key = result.get("file_key", "")

                            sidebar_apify_log[past_fname] = {
                                "run_id": run_id,
                                "file_key": file_key,
                                "run_status": run_status,
                                "sent_at": datetime.now().isoformat(),
                            }
                            save_apify_log(sidebar_apify_log)

                            if run_status == "SUCCEEDED":
                                st.success("Actor completed!")
                            elif run_status == "STARTED":
                                st.info("Actor running in background.")
                            else:
                                past_apify_status.info(f"Status: {run_status}")

                            st.code(f"Run ID: {run_id}\nFile: {file_key}\nStatus: {run_status}")
                            st.markdown(f"[View on Apify](https://console.apify.com/actors/runs/{run_id})")
                    except Exception as e:
                        past_apify_status.error(f"Failed: {e}")
                else:
                    st.warning("Enter an Apify actor name")
    else:
        st.caption("No scrapes yet. Run a search to get started.")

# --- Check for running/completed job ---
job = load_job_status()

# Detect stale "running" jobs (thread died from app sleep/restart)
if job and job.get("status") == "running" and is_job_stale(job):
    output_file = Path(job.get("output_file", ""))
    st.warning(f"**Previous scrape was interrupted** (app went to sleep). "
               f"Found {job.get('found', 0):,} businesses before interruption.")

    if output_file.exists():
        st.info("Partial results were saved. You can download them or re-run to continue "
                "(auto-exclude will skip already-scraped zip codes).")
        df_partial = pd.read_csv(output_file)
        st.dataframe(df_partial, use_container_width=True, height=300)
        st.download_button(
            "Download Partial CSV",
            df_partial.to_csv(index=False),
            file_name=output_file.name,
            mime="text/csv",
            use_container_width=True,
        )

    if st.button("Dismiss & Continue", use_container_width=True):
        clear_job_status()
        st.rerun()
    st.stop()

elif job and job.get("status") == "running":
    st.info(f"**Scrape in progress** — {job['completed']}/{job['total']} zips | "
            f"Found: {job['found']:,} businesses | Errors: {job['errors']}")
    progress = job["completed"] / job["total"] if job["total"] > 0 else 0
    st.progress(min(progress, 1.0))

    if st.button("Stop Scraping", use_container_width=True, type="secondary"):
        stop_scrape_job()
        st.warning("Stop signal sent. Waiting for current batch to finish...")

    time.sleep(2)
    st.rerun()

elif job and job.get("status") in ("completed", "stopped"):
    output_file = Path(job.get("output_file", ""))
    status_label = "Scrape completed" if job["status"] == "completed" else "Scrape stopped"
    st.success(f"**{status_label}** — Found **{job.get('found', 0):,}** businesses "
               f"({job.get('completed', 0)}/{job.get('total', 0)} zips, {job.get('errors', 0)} errors)")

    if output_file.exists():
        df_results = pd.read_csv(output_file)
        if "place_id" in df_results.columns:
            before = len(df_results)
            df_results = df_results.drop_duplicates(subset=["place_id"], keep="first")
            dupes = before - len(df_results)
            if dupes:
                df_results.to_csv(output_file, index=False)
                st.info(f"Removed {dupes} duplicates")

        st.dataframe(df_results, use_container_width=True, height=400)
        st.download_button(
            "Download CSV",
            df_results.to_csv(index=False),
            file_name=output_file.name,
            mime="text/csv",
            use_container_width=True,
        )

        # Clay webhook
        st.divider()
        st.subheader("Send to Clay")
        clay_url = st.text_input(
            "Clay Webhook URL",
            value=os.getenv("CLAY_WEBHOOK_URL", ""),
            help="Paste your Clay webhook URL to send results",
        )
        if st.button("Send to Clay", use_container_width=True):
            if clay_url:
                clay_progress = st.progress(0)
                clay_status = st.empty()

                def on_clay_progress(current, total, sent, errors):
                    clay_progress.progress(current / total)
                    clay_status.text(f"Sending row {current}/{total} — Sent: {sent} | Errors: {errors}")

                try:
                    result = send_to_clay(
                        df_results.to_dict("records"),
                        webhook_url=clay_url,
                        on_progress=on_clay_progress,
                    )
                    clay_progress.progress(1.0)
                    st.success(f"Done! Sent {result['sent']}/{result['total']} rows to Clay. Errors: {result['errors']}")
                except Exception as e:
                    st.error(f"Failed to send to Clay: {e}")
            else:
                st.warning("Enter a Clay webhook URL first")

        # Apify actor integration
        st.divider()
        st.subheader("Process with Apify Actor")
        apify_actor = st.text_input(
            "Apify Actor Name",
            value=os.getenv("APIFY_ACTOR_NAME", ""),
            placeholder="e.g., creator-account/csv---webhook",
            help="Full actor identifier to process the CSV data",
        )
        apify_wait = st.checkbox("Wait for actor to complete", value=False, help="Poll for run completion")

        filename = output_file.name
        apify_log = load_apify_log()
        if filename in apify_log:
            prev = apify_log[filename]
            st.warning(f"Already sent to Apify on {prev.get('sent_at', '?')} — Run ID: `{prev.get('run_id', '?')}`")

        if st.button("Send to Apify Actor", use_container_width=True):
            if apify_actor:
                apify_progress = st.progress(0)
                apify_status = st.empty()

                try:
                    csv_content = df_results.to_csv(index=False)
                    apify_status.text("Uploading CSV to Apify key-value store and starting actor...")

                    def on_apify_progress(msg):
                        apify_status.text(f"Status: {msg}")

                    max_wait = 300 if apify_wait else 0
                    result = run_actor_with_csv(
                        apify_actor,
                        csv_content,
                        file_key_name=filename,
                        poll_interval=5,
                        max_wait=max_wait,
                        on_progress=on_apify_progress,
                    )

                    if "error" in result:
                        apify_status.error(f"Error: {result['error']}")
                    else:
                        apify_progress.progress(1.0)
                        run_id = result.get("run_id", "unknown")
                        run_status = result.get("run_status", "")
                        file_key = result.get("file_key", "")

                        apify_log[filename] = {
                            "run_id": run_id,
                            "file_key": file_key,
                            "run_status": run_status,
                            "sent_at": datetime.now().isoformat(),
                        }
                        save_apify_log(apify_log)

                        if run_status == "SUCCEEDED":
                            st.success("Actor completed successfully!")
                        elif run_status == "FAILED":
                            st.error("Actor run failed.")
                        elif run_status == "STARTED":
                            st.info("Actor started — running in background.")
                        else:
                            apify_status.info(f"Actor status: {run_status}")

                        st.markdown(f"""
**Run ID:** `{run_id}`
**File Key:** `{file_key}`
**Status:** `{run_status}`

[View run on Apify](https://console.apify.com/actors/runs/{run_id})
""")
                except Exception as e:
                    apify_status.error(f"Failed to run actor: {e}")
            else:
                st.warning("Enter an Apify actor name first")

        st.caption(f"Results saved to `{output_file}`")

    if st.button("Dismiss & Start New Scrape", use_container_width=True):
        clear_job_status()
        st.rerun()
    st.stop()

elif job and job.get("status") == "error":
    st.error(f"Last scrape failed: {job.get('message', 'Unknown error')}")
    output_file = Path(job.get("output_file", ""))
    if output_file.exists():
        st.info("Partial results were saved.")
        df_partial = pd.read_csv(output_file)
        st.dataframe(df_partial, use_container_width=True, height=300)
        st.download_button(
            "Download Partial CSV",
            df_partial.to_csv(index=False),
            file_name=output_file.name,
            mime="text/csv",
            use_container_width=True,
        )
    if st.button("Dismiss & Start New Scrape", use_container_width=True):
        clear_job_status()
        st.rerun()
    st.stop()


# --- Main search form (only shown when no job is active) ---
zips_df = load_zip_codes()
all_states = get_states(zips_df)

EUROPEAN_COUNTRIES = [
    "United Kingdom", "Germany", "France", "Spain", "Italy", "Netherlands",
    "Belgium", "Switzerland", "Austria", "Sweden", "Norway", "Denmark",
    "Finland", "Ireland", "Portugal", "Poland", "Czech Republic", "Greece",
    "Romania", "Hungary", "Croatia", "Bulgaria", "Slovakia", "Slovenia",
    "Estonia", "Latvia", "Lithuania", "Luxembourg", "Malta", "Cyprus",
]
EU_COUNTRY_CODES = {
    "United Kingdom": "gb", "Germany": "de", "France": "fr", "Spain": "es",
    "Italy": "it", "Netherlands": "nl", "Belgium": "be", "Switzerland": "ch",
    "Austria": "at", "Sweden": "se", "Norway": "no", "Denmark": "dk",
    "Finland": "fi", "Ireland": "ie", "Portugal": "pt", "Poland": "pl",
    "Czech Republic": "cz", "Greece": "gr", "Romania": "ro", "Hungary": "hu",
    "Croatia": "hr", "Bulgaria": "bg", "Slovakia": "sk", "Slovenia": "si",
    "Estonia": "ee", "Latvia": "lv", "Lithuania": "lt", "Luxembourg": "lu",
    "Malta": "mt", "Cyprus": "cy",
}

keyword = st.text_input(
    "What are you searching for?",
    placeholder="e.g., hotel, restaurant, cafe, lawyer",
    help="Enter the type of business you want to find",
)

col1, col2 = st.columns(2)
with col1:
    market = st.radio("Market", ["United States", "Europe"], horizontal=True)

selected_states = []
selected_cities = []
excluded_states = []
excluded_cities = []
selected_eu_countries = []
eu_city_input = ""

if market == "United States":
    with col2:
        scope = st.radio("Search scope", ["Entire US", "Specific States", "Specific Cities"], horizontal=True)

    if scope == "Specific States":
        selected_states = st.multiselect("Select states", all_states)
    elif scope == "Specific Cities":
        selected_cities = st.multiselect("Select cities", get_cities(zips_df), help="Type to search")

    if scope != "Specific Cities":
        excluded_cities = st.multiselect("Exclude cities (optional)", get_cities(zips_df))
    if scope != "Specific States":
        excluded_states = st.multiselect("Exclude states (optional)", all_states)

else:
    with col2:
        scope = st.radio("Search scope", ["Specific Countries", "Specific Cities"], horizontal=True)

    if scope == "Specific Countries":
        selected_eu_countries = st.multiselect("Select countries", EUROPEAN_COUNTRIES)
    else:
        eu_city_input = st.text_input(
            "Cities to search (comma-separated)",
            placeholder="e.g., London, Berlin, Paris",
        )

st.divider()

max_results = st.number_input(
    "Max results (0 = unlimited)", min_value=0, value=0, step=10,
    help="Stop scraping after reaching this many businesses.",
)

auto_exclude = st.checkbox(
    "Auto-exclude already scraped businesses",
    value=True,
    help="Skips entire zip codes already scraped for the same keyword (saves API credits) and deduplicates by place_id",
)

with st.expander("Advanced options"):
    excluded_categories = st.text_input(
        "Exclude business categories (comma-separated)",
        placeholder="e.g., gas_station, atm, parking",
    )
    additional_keywords = st.text_input(
        "Additional search keywords (comma-separated)",
        placeholder="e.g., motel, bed and breakfast",
        help="Run multiple category searches in one run",
    )
    delay = st.slider("Delay between batches (seconds)", 0.0, 2.0, 0.3, 0.1)
    max_workers = st.slider("Concurrent requests", 1, 20, 5, 1,
                            help="Number of zip codes to scrape in parallel. Higher = faster but more API load.")

submitted = st.button("Start Scraping", use_container_width=True, type="primary")

if submitted and keyword:
    keywords = [keyword.strip()]
    if additional_keywords:
        keywords += [k.strip() for k in additional_keywords.split(",") if k.strip()]

    excluded_cats = [c.strip() for c in excluded_categories.split(",") if c.strip()] if excluded_categories else []

    zip_rows = []

    if market == "United States":
        filtered_zips = filter_zip_codes(
            zips_df, scope,
            selected_states=selected_states,
            selected_cities=selected_cities,
            excluded_states=excluded_states,
            excluded_cities=excluded_cities,
        )

        if filtered_zips.empty:
            st.error("No zip codes match your filters. Adjust your selection.")
            st.stop()

        for _, row in filtered_zips.iterrows():
            zip_code = row["zipcode"]
            city = row["city"]
            state = row["state_abbr"] if "state_abbr" in row else row.get("state", "")
            zip_rows.append((zip_code, city, state))

    else:
        cities_to_search = []
        if scope == "Specific Cities" and eu_city_input:
            cities_to_search = [c.strip() for c in eu_city_input.split(",") if c.strip()]
        elif scope == "Specific Countries" and selected_eu_countries:
            cities_to_search = list(selected_eu_countries)

        if not cities_to_search:
            st.error("Please select countries or enter cities to search.")
            st.stop()

        for loc in cities_to_search:
            cc = EU_COUNTRY_CODES.get(loc, "")
            zip_rows.append((loc, loc, cc))

    pre_seen = set()
    if auto_exclude:
        for kw in keywords:
            place_ids, scraped_zips = load_past_scraped_data(keyword=kw)
            pre_seen.update(place_ids)
            before = len(zip_rows)
            if market == "United States":
                zip_rows = [(z, c, s) for z, c, s in zip_rows if z not in scraped_zips]
            skipped = before - len(zip_rows)
            if skipped or pre_seen:
                st.info(f"Auto-excluding **{len(pre_seen):,}** businesses, "
                        f"skipping **{skipped:,}** already-scraped zip codes")

    if not zip_rows:
        st.success("All zip codes already scraped. Nothing to do.")
        st.stop()

    filename = build_csv_filename(
        keyword, market, scope,
        states=selected_states or None,
        cities=selected_cities or (eu_city_input.split(",") if eu_city_input else None),
        count=0,
    )
    filepath = RESULTS_DIR / filename

    if market == "United States":
        api_country = "us"
    else:
        api_country = EU_COUNTRY_CODES.get(zip_rows[0][2], "gb") if zip_rows else "gb"

    kw = keywords[0]
    start_scrape_job(
        keyword=kw,
        zip_rows=zip_rows,
        country=api_country if market == "United States" else "",
        excluded_categories=excluded_cats,
        initial_seen_ids=pre_seen,
        output_filepath=filepath,
        delay=delay,
        max_results=max_results,
        max_workers=max_workers,
    )

    st.success(f"Scrape started! Searching **{len(zip_rows):,}** locations for '{kw}'")
    time.sleep(1)
    st.rerun()

elif submitted and not keyword:
    st.warning("Please enter a search keyword")
