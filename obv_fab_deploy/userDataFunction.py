"""
(UDF) User Data Function deployment module.
Deploys Fabric UDF from a source to a target workspace
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
    get_lakehouse_id_by_name,
    get_item_id_by_name,
    list_items,
)

# =============================================================================
# LRO Helper
# =============================================================================

def _poll_lro(
    url: str,
    label: str,
    creds: Optional[dict] = None,
    max_attempts: int = 50, #bump this up udf take long time to publish
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
# UDF Definition Helper
# =============================================================================

def _get_udf_definition(workspace_id: str, udf_id: str, creds: Optional[dict] = None) -> Optional[dict]:
    """
    Fetch the full definition of a udf via REST API.
    Handles both synchronous (200) and long-running (202) responses.

    Args:
        workspace_id: The workspace ID (GUID).
        udf_id: The notebook item ID (GUID).
        creds: Optional credentials dict.

    Returns:
        The definition dict (with 'parts' list), or None on failure.
    """
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/userDataFunctions/{udf_id}/getDefinition"

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
# Deploy userDataFunction
# =============================================================================
def deploy_udf(
    source_workspace_name: str,
    source_udf_name: str,
    target_workspace_name: str,
    target_udf_name: Optional[str] = None,
    target_database_workspace_name: Optional[str] = None,
    target_sql_database_name: Optional[str] = None,
    creds: Optional[dict] = None,
):
    """
    Deploy a udf from a source workspace to a target workspace,
    patching the connection to the correct datebase.

    Args:
        source_workspace_name: Name of the source workspace.
        source_udf_name: Name of the source udf.
        target_workspace_name: Name of the target workspace.
        target_udf_name: Display name for the target udf.
        target_database_workspace_name: Name of workspace where database lives
        target_sql_database_name: Name of the database that udf will write too 
        creds: Optional credentials dict. Not needed in Fabric notebooks.

    Returns:
        The notebook ID on success, or None on failure.
    """

    headers = _fabric_headers(creds)


    if target_database_workspace_name is not None and target_sql_database_name is not None:
        has_connection = True
    elif target_database_workspace_name is None and target_sql_database_name is not None:
        print(f'Missing target_database_workspace_name')
        return None
    elif target_database_workspace_name is not None and target_sql_database_name is None:
        print(f'Missing target_sql_database_name')
        return None
    else:
        has_connection = False

    if target_udf_name is None:
        target_udf_name = source_udf_name

    # --- set all workspace, udf, and datbase IDs ------------------------------
    try:
        target_ws = get_workspace_id_by_name(workspace_name=target_workspace_name)
        source_ws = get_workspace_id_by_name(workspace_name=source_workspace_name)
        source_udf_id = get_item_id_by_name(
            workspace=source_workspace_name,
            item_name=source_udf_name,
            item_type="userDataFunction"
        )
        if has_connection:
            target_database_ws = get_workspace_id_by_name(workspace_name=target_database_workspace_name)
            target_sql_database_id = get_item_id_by_name(
                workspace=target_database_workspace_name,
                item_name=target_sql_database_name,
                item_type="SQLDatabase"
            )
    except Exception as e:
        print(f"❌ Could not resolve IDs: {e}")
        return None

    # --- get source udf definition -----------------------------------
    definition_raw = _get_udf_definition(source_ws, source_udf_id)
    if definition_raw is None:
        print('definition_raw is None')
        return

    try:
        part_paths = [p["path"] for p in definition_raw["parts"]]
        payload_part = next(
            (p for p in definition_raw["parts"] if p["path"].endswith("definition.json")),
            None,
        )
        if payload_part is None:
            print(f"❌ No definition.json part found in udf definition. Available parts: {part_paths}")
            return None

        decoded_payload_json = json.loads(base64.b64decode(payload_part["payload"]).decode("utf-8"))
        
        if has_connection:
            new_data_source = decoded_payload_json.get("connectedDataSources", [])[0]
            new_data_source['artifactId'] = target_sql_database_id 
            new_data_source['workspaceId'] = target_database_ws


            # Re-encode the patched content
            payload_part["payload"] = base64.b64encode(
                json.dumps(decoded_payload_json).encode("utf-8")
            ).decode()

    except Exception as e:
        print(f"⚠️ Could not patch udf: {type(e).__name__}: {e}")
        return None

    #alter the source defintion with changed database connection
    if has_connection:
        definition_raw['parts'][0]['payload'] = payload_part["payload"]

    # --- check if target udf already exists --------------------------
    existing_udfs = list_items(target_ws, "userDataFunction", creds)
    matches = [udf for udf in existing_udfs if udf["displayName"] == target_udf_name]

    if not matches:
        # --- create new udf ------------------------------------------
        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/userDataFunctions",
            headers=headers,
            json={
                "displayName": target_udf_name,
                "definition": definition_raw
            },
        )
        if resp.status_code == 202:
            udf_id = _poll_lro(resp.headers["Location"], 
                                "userDataFunction creation",
                                creds,
                                max_attempts=50,
                                interval=5,
                                fetch_result=True)['id']
        elif resp.status_code in (200, 201):
            udf_id = resp.json().get("id")
        else:
            print(f"❌ Create failed: {resp.status_code} {resp.text}")
        print(f"✅ Created udf '{target_udf_name}'")
    else:
        # --- update existing udf -------------------------------------
        udf_id = matches[0]["id"]
        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/userDataFunctions/{udf_id}/updateDefinition",
            headers=headers,
            json={
                "definition": definition_raw
            },
        )
        if resp.status_code == 202:
            result = _poll_lro(resp.headers["Location"], "userDataFunction update",creds,max_attempts=50,interval=5)
            if result is None:
                return None
        elif resp.status_code != 200:
            print(f"❌ Update failed: {resp.status_code} {resp.text}")
            return None
        print(f"✅ Updated udf '{target_udf_name}'")

    print(f"✅ Finished '{source_udf_name}' → '{target_udf_name}'")
    return udf_id