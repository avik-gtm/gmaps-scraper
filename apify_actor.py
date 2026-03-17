import os
import time
import httpx
from datetime import datetime

try:
    import streamlit as st
    APIFY_API_KEY = st.secrets.get("APIFY_API_KEY", os.getenv("APIFY_API_KEY", ""))
    APIFY_KV_STORE_ID = st.secrets.get("APIFY_KV_STORE_ID", os.getenv("APIFY_KV_STORE_ID", ""))
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
    APIFY_KV_STORE_ID = os.getenv("APIFY_KV_STORE_ID", "")


def run_actor_with_csv(actor_name: str, csv_data: str, api_key: str = None,
                       store_id: str = None, batch_size: int = 500,
                       max_concurrency: int = 3, poll_interval: int = 5,
                       max_wait: int = 300, on_progress=None) -> dict:
    """Upload CSV to key-value store, then run actor pointing to it."""
    key = api_key or APIFY_API_KEY
    sid = store_id or APIFY_KV_STORE_ID
    if not key:
        return {"error": "No Apify API key configured."}
    if not sid:
        return {"error": "No Apify key-value store ID configured."}

    # Step 1: Upload CSV to the named store
    file_key = f"gmaps-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
    if on_progress:
        on_progress(f"Uploading CSV as '{file_key}'...")

    try:
        upload_url = f"https://api.apify.com/v2/key-value-stores/{sid}/records/{file_key}"
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "text/csv"}

        with httpx.Client(timeout=60) as client:
            resp = client.put(upload_url, content=csv_data, headers=headers)
            resp.raise_for_status()

        if on_progress:
            on_progress("CSV uploaded. Starting actor...")
    except Exception as e:
        return {"error": f"Failed to upload CSV: {str(e)}"}

    # Step 2: Run actor with input pointing to the store/file
    safe_name = actor_name.replace("/", "~")
    run_url = f"https://api.apify.com/v2/acts/{safe_name}/runs"

    actor_input = {
        "fileKey": file_key,
        "storeId": sid,
        "batchSize": batch_size,
        "maxConcurrency": max_concurrency,
    }

    try:
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        with httpx.Client(timeout=60) as client:
            resp = client.post(run_url, json=actor_input, headers=headers)
            resp.raise_for_status()
            result = resp.json()

        run_id = result.get("data", {}).get("id")
        if on_progress:
            on_progress(f"Actor run started: {run_id}")
    except Exception as e:
        return {"error": f"Failed to run actor: {str(e)}"}

    # Step 3: Poll for completion if requested
    if max_wait > 0 and run_id:
        elapsed = 0
        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval

            try:
                status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
                with httpx.Client(timeout=30) as client:
                    resp = client.get(status_url, headers={"Authorization": f"Bearer {key}"})
                    resp.raise_for_status()
                    run_status = resp.json().get("data", {}).get("status", "")

                if on_progress:
                    on_progress(f"Run status: {run_status}")

                if run_status in ["SUCCEEDED", "FAILED", "ABORTED"]:
                    return {"run_id": run_id, "run_status": run_status, "file_key": file_key}
            except Exception:
                pass

        return {"run_id": run_id, "run_status": "TIMEOUT", "file_key": file_key,
                "message": f"Did not complete within {max_wait}s"}

    return {"run_id": run_id, "run_status": "STARTED", "file_key": file_key}
