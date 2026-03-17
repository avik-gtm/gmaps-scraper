import os
import httpx

try:
    import streamlit as st
    API_KEY = st.secrets.get("SCRAPER_TECH_API_KEY", os.getenv("SCRAPER_TECH_API_KEY", ""))
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    API_KEY = os.getenv("SCRAPER_TECH_API_KEY", "")
BASE_URL = "https://api.scraper.tech"
SEARCH_ENDPOINT = "/searchmaps.php"


def search_maps(query: str, country: str = "us", lang: str = "en",
                lat: float = None, lng: float = None, limit: int = 20,
                offset: int = 0, zoom: int = 13) -> list[dict]:
    """Search Google Maps via scraper.tech API."""
    headers = {
        "Scraper-Key": API_KEY,
        "Content-Type": "application/json",
    }
    params = {
        "query": query,
        "country": country,
        "lang": lang,
        "limit": limit,
        "offset": offset,
        "zoom": zoom,
    }
    if lat is not None and lng is not None:
        params["lat"] = lat
        params["lng"] = lng

    with httpx.Client(timeout=30) as client:
        resp = client.get(f"{BASE_URL}{SEARCH_ENDPOINT}", headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

    # API may return results under "data" key or directly as a list
    if isinstance(data, dict):
        return data.get("data", data.get("results", []))
    return data if isinstance(data, list) else []


def search_by_zip(keyword: str, zip_code: str, country: str = "us",
                  limit: int = 20) -> list[dict]:
    """Search Google Maps for a keyword in a specific zip code."""
    query = f"{keyword} in {zip_code}"
    return search_maps(query=query, country=country, limit=limit)


def search_by_city(keyword: str, city: str, state: str = "",
                   country: str = "us", limit: int = 20) -> list[dict]:
    """Search Google Maps for a keyword in a specific city."""
    location = f"{city}, {state}" if state else city
    query = f"{keyword} in {location}"
    return search_maps(query=query, country=country, limit=limit)


def flatten_result(r: dict) -> dict:
    """Flatten a scraper.tech result into a clean row for CSV/display."""
    return {
        "business_id": r.get("business_id", ""),
        "place_id": r.get("place_id", ""),
        "name": r.get("name", ""),
        "full_address": r.get("full_address", ""),
        "city": r.get("city", ""),
        "phone": r.get("phone_number", ""),
        "website": r.get("website", ""),
        "rating": r.get("rating", ""),
        "review_count": r.get("review_count", ""),
        "latitude": r.get("latitude", ""),
        "longitude": r.get("longitude", ""),
        "types": ", ".join(r.get("types", [])) if isinstance(r.get("types"), list) else r.get("types", ""),
        "place_link": r.get("place_link", ""),
        "verified": r.get("verified", ""),
        "is_claimed": r.get("is_claimed", ""),
    }
