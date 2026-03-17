import os
import time
import json
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime

from scraper_tech import search_maps, flatten_result
from clay_webhook import send_to_clay

# --- Config ---
ZIP_CSV = Path(__file__).parent / "us_zip_codes.csv"
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)


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

        except Exception as e:
            errors += 1
            if errors <= 5:
                st.warning(f"Error on zip {zip_code}: {e}")

        # Check max results limit
        if max_results > 0 and len(all_results) >= max_results:
            status_text.text(f"Reached max results limit ({max_results}). Stopping.")
            break

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

# --- Advanced options in expander ---
with st.expander("Advanced options"):
    max_results = st.number_input(
        "Max results (0 = unlimited)", min_value=0, value=0, step=500,
        help="Stop scraping after reaching this many businesses.",
    )
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

        # Save to CSV
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"gmaps_{keyword.replace(' ', '_')}_{ts}.csv"
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
    else:
        st.caption("No scrapes yet. Run a search to get started.")
