import os
import httpx

try:
    import streamlit as st
    CLAY_WEBHOOK_URL = st.secrets.get("CLAY_WEBHOOK_URL", os.getenv("CLAY_WEBHOOK_URL", ""))
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    CLAY_WEBHOOK_URL = os.getenv("CLAY_WEBHOOK_URL", "")


def send_to_clay(results: list[dict], webhook_url: str = None,
                 on_progress=None) -> dict:
    """Send scraped results to a Clay webhook, one row per request."""
    url = webhook_url or CLAY_WEBHOOK_URL
    if not url:
        return {"error": "No Clay webhook URL configured."}

    sent = 0
    errors = 0
    with httpx.Client(timeout=30) as client:
        for i, row in enumerate(results):
            try:
                resp = client.post(url, json=row)
                resp.raise_for_status()
                sent += 1
            except Exception:
                errors += 1

            if on_progress:
                on_progress(i + 1, len(results), sent, errors)

    return {"status": "done", "sent": sent, "errors": errors, "total": len(results)}
