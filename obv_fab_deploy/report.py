"""
Report deployment module.
Deploys Fabric reports from a source to a target workspace
using direct Fabric REST API calls.
"""

import requests
import time
import json
import base64
from typing import Optional

from .utils import (
    FABRIC_API,
    _fabric_headers,
    get_workspace_id_by_name,
    get_item_id_by_name,
    get_dataset_id_by_name,
    rebind_report,
    list_items,
)


# =============================================================================
# LRO Helper
# =============================================================================

def _poll_lro(
    url: str,
    label: str,
    creds: Optional[dict] = None,
    max_attempts: int = 15,
    interval: int = 5,
    fetch_result: bool = False,
):
    """
    Poll a long-running operation until it succeeds, fails, or times out.

    Args:
        url: The Location URL from the initial 202 response.
        label: Human-readable label for logging.
        creds: Optional credentials dict.
        max_attempts: Max polling iterations.
        interval: Seconds between polls.
        fetch_result: If True, fetch the /result endpoint on success.

    Returns:
        The result JSON (if fetch_result), True (if not), or None on failure/timeout.
    """
    headers = _fabric_headers(creds)
    for _ in range(max_attempts):
        time.sleep(interval)
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        body = resp.json()

        if body.get("status") == "Succeeded":
            if fetch_result:
                result_resp = requests.get(url + "/result", headers=headers)
                result_resp.raise_for_status()
                return result_resp.json()
            return True
        elif body.get("status") == "Failed":
            print(f"❌ {label} failed: {body}")
            return None
        else:
            print(f"⏳ {label} running...")

    print(f"❌ Timed out waiting for {label}")
    return None


# =============================================================================
# Report Definition Helper
# =============================================================================

def _get_report_definition(
    workspace_id: str, report_id: str, creds: Optional[dict] = None
) -> Optional[dict]:
    """
    Fetch the full definition of a report via REST API.
    Handles both synchronous (200) and long-running (202) responses.

    Args:
        workspace_id: The workspace ID (GUID).
        report_id: The report item ID (GUID).
        creds: Optional credentials dict.

    Returns:
        The definition dict (with 'parts' list), or None on failure.
    """
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/reports/{report_id}/getDefinition"

    resp = requests.post(url, headers=headers)

    if resp.status_code == 202:
        result = _poll_lro(resp.headers["Location"], "getDefinition", creds, fetch_result=True)
        return result.get("definition") if result else None
    elif resp.status_code == 200:
        return resp.json().get("definition")
    else:
        print(f"❌ getDefinition failed: {resp.status_code} {resp.text}")
        return None


# =============================================================================
# Deploy Report
# =============================================================================

def deploy_report(
    source_workspace_name: str,
    source_report_name: str,
    target_workspace_name: str,
    target_report_name: str,
    target_dataset_name: str,
    creds: Optional[dict] = None,
):
    """
    Deploy a report from a source workspace to a target workspace,
    and rebind it to a semantic model in the target workspace.

    If the target report already exists it is updated in-place;
    otherwise a new report is created.

    Args:
        source_workspace_name: Name of the source workspace.
        source_report_name: Name of the source report.
        target_workspace_name: Name of the target workspace.
        target_report_name: Display name for the target report.
        target_dataset_name: Name of the semantic model to rebind to in the target workspace.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
    """
    # --- resolve workspace IDs --------------------------------------------
    try:
        source_ws = get_workspace_id_by_name(source_workspace_name, creds)
        target_ws = get_workspace_id_by_name(target_workspace_name, creds)
    except Exception as e:
        print(f"❌ Workspace resolution failed: {e}")
        return

    # --- get source report definition -------------------------------------
    source_report_id = get_item_id_by_name(
        source_workspace_name, source_report_name, "Report", creds
    )
    definition = _get_report_definition(source_ws, source_report_id, creds)
    if definition is None:
        print(f"❌ Could not retrieve definition for '{source_report_name}'")
        return

    # --- check if target report already exists ----------------------------
    existing_reports = list_items(target_ws, "Report", creds)
    matches = [r for r in existing_reports if r["displayName"] == target_report_name]
    headers = _fabric_headers(creds)

    if matches:
        # --- update existing report ---------------------------------------
        report_id = matches[0]["id"]
        print(f"🛠️ Report '{target_report_name}' exists. Updating definition...")

        # Get .platform from TARGET report (required for updateMetadata)
        target_def = _get_report_definition(target_ws, report_id, creds)
        if target_def:
            platform_payload = next(
                (p["payload"] for p in target_def["parts"] if p["path"] == ".platform"),
                None,
            )
            if platform_payload:
                # Replace source .platform with target .platform
                has_platform = False
                for part in definition["parts"]:
                    if part["path"] == ".platform":
                        part["payload"] = platform_payload
                        has_platform = True
                        break
                if not has_platform:
                    definition["parts"].append({
                        "path": ".platform",
                        "payload": platform_payload,
                        "payloadType": "InlineBase64",
                    })

        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/reports/{report_id}/updateDefinition?updateMetadata=true",
            headers=headers,
            json={"definition": definition},
        )
        if resp.status_code == 202:
            result = _poll_lro(resp.headers["Location"], "Report update", creds)
            if result is None:
                return
        elif resp.status_code != 200:
            print(f"❌ Update failed: {resp.status_code} {resp.text}")
            return
        print(f"✅ Updated report '{target_report_name}'")
    else:
        # --- create new report --------------------------------------------
        print(f"📄 Report '{target_report_name}' not found. Creating...")

        # Patch .platform displayName for the new report
        for part in definition.get("parts", []):
            if part["path"] == ".platform":
                try:
                    content = base64.b64decode(part["payload"]).decode("utf-8")
                    platform_obj = json.loads(content)
                    platform_obj["metadata"]["displayName"] = target_report_name
                    part["payload"] = base64.b64encode(
                        json.dumps(platform_obj, indent=2).encode()
                    ).decode()
                except Exception:
                    pass

        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/items",
            headers=headers,
            json={
                "displayName": target_report_name,
                "type": "Report",
                "definition": definition,
            },
        )
        if resp.status_code == 202:
            result = _poll_lro(
                resp.headers["Location"], "Report creation", creds, fetch_result=True
            )
            if result is None:
                return
        elif resp.status_code in (200, 201):
            pass
        else:
            print(f"❌ Create failed: {resp.status_code} {resp.text}")
            return
        print(f"✅ Created report '{target_report_name}'")

    # --- rebind to target dataset -----------------------------------------
    try:
        rebind_report(
            report=target_report_name,
            dataset=target_dataset_name,
            report_workspace=target_ws,
            dataset_workspace=target_ws,
            creds=creds,
        )
    except Exception as e:
        print(f"⚠️ Rebind failed: {e}")

    print(f"✅ Finished '{source_report_name}' → '{target_report_name}'")
