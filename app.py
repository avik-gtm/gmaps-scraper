import os
import time
import json
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime

from scraper_tech import search_maps, flatten_result
from clay_webhook import send_to_clay
from apify_actor import run_actor_with_csv

# --- Config ---
ZIP_CSV = Path(__file__).parent / "us_zip_codes.csv"
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)
APIFY_LOG_FILE = RESULTS_DIR / "apify_log.json"


def load_apify_log():
    """Load log of CSVs that have been sent to Apify."""
    if APIFY_LOG_FILE.exists():
        return json.loads(APIFY_LOG_FILE.read_text())
    return {}


def save_apify_log(log: dict):
    APIFY_LOG_FILE.write_text(json.dumps(log, indent=2))


def build_csv_filename(keyword, market, scope, states=None, cities=None, count=0):
    """Build a descriptive CSV filename."""
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


# --- Password Protection ---
def check_password():
    """Returns True if the user has entered the correct password."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    password = st.secrets.get("APP_PASSWORD", "")
    if not password:
        return True  # No password set, skip auth

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
    """Filter zip codes based on scope and include/exclude filters."""
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


def run_scrape(keyword, zip_codes_df, country, excluded_categories,
               progress_bar, status_text, results_container, delay=0.5,
               max_results=0):
    """Run the scrape across all zip codes."""
    all_results = []
    seen_place_ids = set()
    total = len(zip_codes_df)
    errors = 0

    for i, (_, row) in enumerate(zip_codes_df.iterrows()):
        # Check max results limit before making another API call
        if max_results > 0 and len(all_results) >= max_results:
            progress_bar.progress(1.0)
            status_text.text(f"Reached max results limit ({max_results}). Stopping. Found {len(all_results)} businesses.")
            break

        zip_code = row["zipcode"]
        city = row["city"]
        state = row["state_abbr"] if "state_abbr" in row else row.get("state", "")

        progress = (i + 1) / total
        progress_bar.progress(progress)
        status_text.text(
            f"Searching zip {zip_code} ({city}, {state}) — "
            f"{i+1}/{total} | Found: {len(all_results)} businesses | Errors: {errors}"
        )

        try:
            query = f"{keyword} in {zip_code}"
            results = search_maps(query=query, country=country, limit=20)

            for r in results:
                place_id = r.get("place_id", r.get("business_id", ""))
                if place_id and place_id in seen_place_ids:
                    continue
                if place_id:
                    seen_place_ids.add(place_id)

                # Filter out excluded categories
                types = r.get("types", [])
                if isinstance(types, str):
                    types = [t.strip() for t in types.split(",")]
                if excluded_categories:
                    excluded_lower = [c.lower().strip() for c in excluded_categories]
                    if any(t.lower() in excluded_lower for t in types):
                        continue

                flat = flatten_result(r)
                flat["source_zip"] = zip_code
                flat["source_city"] = city
                flat["source_state"] = state
                all_results.append(flat)

                # Also check mid-zip so we don't overshoot
                if max_results > 0 and len(all_results) >= max_results:
                    break

        except Exception as e:
            errors += 1
            if errors <= 5:
                st.warning(f"Error on zip {zip_code}: {e}")

        # Rate limiting
        if delay > 0:
            time.sleep(delay)

        # Update live results every 50 zips
        if (i + 1) % 50 == 0 and all_results:
            with results_container:
                st.metric("Businesses found so far", len(all_results))

    return all_results


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

# Load zip codes
zips_df = load_zip_codes()
all_states = get_states(zips_df)

# --- Constants ---
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

# --- Search Config (no form, just regular widgets for conditional rendering) ---
keyword = st.text_input(
    "What are you searching for?",
    placeholder="e.g., hotel, restaurant, cafe, lawyer",
    help="Enter the type of business you want to find",
)

col1, col2 = st.columns(2)
with col1:
    market = st.radio("Market", ["United States", "Europe"], horizontal=True)

# --- Scope + filters based on market ---
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
    delay = st.slider("Delay between requests (seconds)", 0.0, 2.0, 0.3, 0.1)

# --- Start button ---
submitted = st.button("Start Scraping", use_container_width=True, type="primary")

# --- Run Scraper ---
if submitted and keyword:
    keywords = [keyword.strip()]
    if additional_keywords:
        keywords += [k.strip() for k in additional_keywords.split(",") if k.strip()]

    excluded_cats = [c.strip() for c in excluded_categories.split(",") if c.strip()] if excluded_categories else []

    all_results = []

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
        else:
            st.info(f"Searching **{len(keywords)} keyword(s)** across **{len(filtered_zips):,} zip codes**")

            for kw in keywords:
                st.subheader(f"Searching: {kw}")
                progress_bar = st.progress(0)
                status_text = st.empty()
                results_container = st.empty()

                results = run_scrape(
                    kw, filtered_zips, "us", excluded_cats,
                    progress_bar, status_text, results_container, delay=delay,
                    max_results=max_results,
                )
                all_results.extend(results)
                st.success(f"Found **{len(results)}** businesses for '{kw}'")

    else:
        cities_to_search = []
        if scope == "Specific Cities" and eu_city_input:
            cities_to_search = [c.strip() for c in eu_city_input.split(",") if c.strip()]
        elif scope == "Specific Countries" and selected_eu_countries:
            for country_name in selected_eu_countries:
                cities_to_search.append(country_name)

        if not cities_to_search:
            st.error("Please select countries or enter cities to search.")
        else:
            st.info(f"Searching **{len(keywords)} keyword(s)** across **{len(cities_to_search)} locations**")

            for kw in keywords:
                st.subheader(f"Searching: {kw}")
                progress_bar = st.progress(0)
                status_text = st.empty()
                seen_place_ids = set()

                for j, location in enumerate(cities_to_search):
                    progress_bar.progress((j + 1) / len(cities_to_search))
                    status_text.text(f"Searching '{kw}' in {location} — {j+1}/{len(cities_to_search)}")

                    cc = EU_COUNTRY_CODES.get(location, "")
                    query = f"{kw} in {location}"

                    try:
                        results = search_maps(query=query, country=cc or "gb", limit=20)
                        for r in results:
                            place_id = r.get("place_id", r.get("business_id", ""))
                            if place_id and place_id in seen_place_ids:
                                continue
                            if place_id:
                                seen_place_ids.add(place_id)

                            types = r.get("types", [])
                            if isinstance(types, str):
                                types = [t.strip() for t in types.split(",")]
                            if excluded_cats and any(t.lower() in [c.lower() for c in excluded_cats] for t in types):
                                continue

                            flat = flatten_result(r)
                            flat["source_location"] = location
                            all_results.append(flat)
                    except Exception as e:
                        st.warning(f"Error searching {location}: {e}")

                    if delay > 0:
                        time.sleep(delay)

                st.success(f"Found **{len(all_results)}** businesses for '{kw}'")

    # --- Display & Export Results ---
    if all_results:
        st.divider()
        st.header(f"Results: {len(all_results):,} businesses")

        df_results = pd.DataFrame(all_results)

        if "place_id" in df_results.columns:
            before = len(df_results)
            df_results = df_results.drop_duplicates(subset=["place_id"], keep="first")
            dupes = before - len(df_results)
            if dupes:
                st.info(f"Removed {dupes} duplicate businesses")

        st.dataframe(df_results, use_container_width=True, height=400)

        # Save to CSV with descriptive name
        filename = build_csv_filename(
            keyword, market, scope,
            states=selected_states or None,
            cities=selected_cities or (eu_city_input.split(",") if eu_city_input else None),
            count=len(df_results),
        )
        filepath = RESULTS_DIR / filename
        df_results.to_csv(filepath, index=False)

        st.download_button(
            "Download CSV",
            df_results.to_csv(index=False),
            file_name=filename,
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

        # Check if this file was already sent
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

                        # Log this send
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

        st.caption(f"Results saved to `{filepath}`")

elif submitted and not keyword:
    st.warning("Please enter a search keyword")

# --- Sidebar: View Past Results ---
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

            # Download
            st.download_button(
                "Download CSV",
                past_df.to_csv(index=False),
                file_name=selected_file.name,
                mime="text/csv",
                use_container_width=True,
                key="sidebar_download",
            )

            # Send to Clay
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

            # Apify actor for past scrapes
            st.divider()
            st.markdown("**Apify Actor**")

            # Show if already sent
            sidebar_apify_log = load_apify_log()
            past_fname = selected_file.name
            if past_fname in sidebar_apify_log:
                prev = sidebar_apify_log[past_fname]
                st.success(f"Already sent to Apify")
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

                            # Log it
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
