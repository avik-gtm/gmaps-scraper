import os
import json
import time
import httpx

try:
    import streamlit as st
    APIFY_API_KEY = st.secrets.get("APIFY_API_KEY", os.getenv("APIFY_API_KEY", ""))
    APIFY_KV_STORE_ID = st.secrets.get("APIFY_KV_STORE_ID", os.getenv("APIFY_KV_STORE_ID", ""))
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
    APIFY_KV_STORE_ID = os.getenv("APIFY_KV_STORE_ID", "")


def upload_csv_to_keyvalue_store(csv_data: str, actor_name: str, api_key: str = None,
                                  store_id: str = None, record_key: str = "input.csv",
                                  on_progress=None) -> dict:
    """Upload CSV data to a named Apify key-value store."""
    key = api_key or APIFY_API_KEY
    sid = store_id or APIFY_KV_STORE_ID
    if not key:
        return {"error": "No Apify API key configured."}
    if not sid:
        return {"error": "No Apify key-value store ID configured. Set APIFY_KV_STORE_ID."}

    try:
        store_url = f"https://api.apify.com/v2/key-value-stores/{sid}/records/{record_key}"

        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "text/csv",
        }

        with httpx.Client(timeout=60) as client:
            resp = client.put(store_url, content=csv_data, headers=headers)
            resp.raise_for_status()

        if on_progress:
            on_progress("Uploaded CSV to key-value store")

        return {"status": "uploaded", "store_id": sid, "records": len(csv_data.split('\n'))}

    except Exception as e:
        return {"error": f"Failed to upload to key-value store: {str(e)}"}


def run_actor(actor_name: str, input_data: dict = None, api_key: str = None,
              on_progress=None) -> dict:
    """
    Trigger/run an Apify actor.

    Args:
        actor_name: Actor identifier (e.g., "creator-account/csv---webhook")
        input_data: Input data for the actor (optional)
        api_key: Apify API key (defaults to env var)
        on_progress: Callback function for progress updates

    Returns:
        dict with run_id, status, and actor info
    """
    key = api_key or APIFY_API_KEY
    if not key:
        return {"error": "No Apify API key configured."}

    # Endpoint: POST /v2/acts/{actorId}/runs
    # Or for named actors: POST /v2/acts/{username}/{actorName}/runs

    # Apify uses ~ instead of / in actor names for URLs
    safe_name = actor_name.replace("/", "~")
    url = f"https://api.apify.com/v2/acts/{safe_name}/runs"

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    payload = input_data or {}

    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                url,
                json=payload,
                headers=headers,
            )
            resp.raise_for_status()
            result = resp.json()

        run_id = result.get("data", {}).get("id")
        if on_progress:
            on_progress(f"Actor run started: {run_id}")

        return {
            "status": "running",
            "run_id": run_id,
            "actor": actor_name,
            "api_response": result,
        }

    except Exception as e:
        return {"error": f"Failed to run actor: {str(e)}"}


def get_run_status(run_id: str, api_key: str = None, on_progress=None) -> dict:
    """
    Get the status of an actor run.

    Args:
        run_id: The run ID from run_actor()
        api_key: Apify API key (defaults to env var)
        on_progress: Callback function for progress updates

    Returns:
        dict with run status and metadata
    """
    key = api_key or APIFY_API_KEY
    if not key:
        return {"error": "No Apify API key configured."}

    # Endpoint: GET /v2/actor-runs/{runId}
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"

    headers = {
        "Authorization": f"Bearer {key}",
    }

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
            result = resp.json()

        status = result.get("data", {}).get("status")
        if on_progress:
            on_progress(f"Run status: {status}")

        return {
            "status": "fetched",
            "run_id": run_id,
            "run_status": status,
            "api_response": result,
        }

    except Exception as e:
        return {"error": f"Failed to get run status: {str(e)}"}


def run_actor_with_csv(actor_name: str, csv_data: str, api_key: str = None,
                       poll_interval: int = 5, max_wait: int = 300,
                       on_progress=None) -> dict:
    """
    Convenience function: Upload CSV to key-value store and run the actor.

    Args:
        actor_name: Actor identifier
        csv_data: CSV content as string
        api_key: Apify API key (defaults to env var)
        poll_interval: Seconds to wait between status checks
        max_wait: Maximum seconds to wait for completion (0 = don't wait)
        on_progress: Callback function for progress updates

    Returns:
        dict with final status and results
    """
    key = api_key or APIFY_API_KEY
    if not key:
        return {"error": "No Apify API key configured."}

    # Step 1: Upload CSV
    if on_progress:
        on_progress("Uploading CSV to key-value store...")

    upload_result = upload_csv_to_keyvalue_store(csv_data, actor_name, key, on_progress=on_progress)
    if "error" in upload_result:
        return upload_result

    # Step 2: Run the actor
    if on_progress:
        on_progress("Triggering actor run...")

    run_result = run_actor(actor_name, api_key=key, on_progress=on_progress)
    if "error" in run_result:
        return run_result

    run_id = run_result.get("run_id")

    # Step 3: Poll for completion (if max_wait > 0)
    if max_wait > 0 and run_id:
        elapsed = 0
        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval

            status_result = get_run_status(run_id, api_key=key, on_progress=on_progress)
            if "error" in status_result:
                return status_result

            run_status = status_result.get("run_status", "")
            if run_status in ["SUCCEEDED", "FAILED", "ABORTED"]:
                return {
                    "status": "completed",
                    "run_id": run_id,
                    "run_status": run_status,
                    "upload": upload_result,
                    "run": run_result,
                }

        return {
            "status": "timeout",
            "run_id": run_id,
            "message": f"Actor run did not complete within {max_wait} seconds",
            "upload": upload_result,
            "run": run_result,
        }
    else:
        return {
            "status": "queued",
            "run_id": run_id,
            "message": "Actor run started (not waiting for completion)",
            "upload": upload_result,
            "run": run_result,
        }
