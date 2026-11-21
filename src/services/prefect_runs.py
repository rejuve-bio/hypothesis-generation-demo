import os

import requests
from loguru import logger


def cancel_flow_run(flow_run_id):
    """Ask Prefect to cancel a flow run so its worker tears it down; best effort, never raises.

    Returns whether Prefect accepted the transition.
    """
    prefect_url = os.getenv("PREFECT_API_URL", "http://prefect-service:4200/api")
    try:
        resp = requests.post(
            f"{prefect_url}/flow_runs/{flow_run_id}/set_state",
            json={"state": {"type": "CANCELLING"}},
            timeout=3,
        )
        resp.raise_for_status()
        result = resp.json()
    except Exception as cancel_e:
        logger.warning(f"Failed to cancel Prefect flow run {flow_run_id}: {cancel_e}")
        return False
    if result.get("status") == "ACCEPT":
        logger.info(f"Requested cancellation of Prefect flow run {flow_run_id}")
        return True
    reason = (result.get("details") or {}).get("reason", "")
    logger.info(f"Prefect did not cancel flow run {flow_run_id}: {result.get('status')} {reason}".rstrip())
    return False
