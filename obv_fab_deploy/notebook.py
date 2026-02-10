"""
Notebook deployment module.
Deploys Fabric notebooks from a source to a target workspace
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

def _poll_lro(url: str, label: str, creds: Optional[dict] = None, max_attempts: int = 15, interval: int = 5):
    """
    Poll a long-running operation (LRO) until it succeeds, fails, or times out.

    Args:
        url: The Location URL returned from the initial 202 response.
        label: A human-readable label for logging (e.g. "Notebook creation").
        creds: Optional credentials dict.
        max_attempts: Max polling iterations.
        interval: Seconds between polls.

    Returns:
        The item ID on success, or None on failure/timeout.
    """
    headers = _fabric_headers(creds)
    for _ in range(max_attempts):
        time.sleep(interval)
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        status_body = resp.json()

        if status_body.get("status") == "Succeeded":
            result_resp = requests.get(url + "/result", headers=headers)
            result_resp.raise_for_status()
            return result_resp.json().get("id")
        elif status_body.get("status") == "Failed":
            print(f"❌ {label} failed: {status_body}")
            return None
        else:
            print(f"⏳ {label} running...")

    print(f"❌ Timed out waiting for {label}")
    return None


# =============================================================================
# Notebook Definition Helper
# =============================================================================

def _get_notebook_definition(workspace_id: str, notebook_id: str, creds: Optional[dict] = None) -> Optional[dict]:
    """
    Fetch the full definition of a notebook (ipynb format) via REST API.
    Handles both synchronous (200) and long-running (202) responses.

    Args:
        workspace_id: The workspace ID (GUID).
        notebook_id: The notebook item ID (GUID).
        creds: Optional credentials dict.

    Returns:
        The definition dict (with 'parts' list), or None on failure.
    """
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition?format=ipynb"

    resp = requests.post(url, headers=headers)

    if resp.status_code == 202:
        op_url = resp.headers["Location"]
        for _ in range(10):
            time.sleep(5)
            poll = requests.get(op_url, headers=headers)
            poll.raise_for_status()
            poll_body = poll.json()
            if poll_body.get("status") == "Succeeded":
                result = requests.get(op_url + "/result", headers=headers)
                result.raise_for_status()
                return result.json().get("definition")
            elif poll_body.get("status") == "Failed":
                print("❌ getDefinition LRO failed")
                return None
        print("❌ Timed out waiting for notebook definition")
        return None
    elif resp.status_code == 200:
        return resp.json().get("definition")
    else:
        print(f"❌ getDefinition failed: {resp.status_code} {resp.text}")
        return None


# =============================================================================
# Delete Notebook
# =============================================================================

def delete_notebook(workspace_name: str, notebook_name: str, creds: Optional[dict] = None):
    """
    Delete a Fabric notebook by name from a workspace.

    Args:
        workspace_name: The Fabric workspace name.
        notebook_name: The notebook display name.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
    """
    workspace_id = get_workspace_id_by_name(workspace_name, creds)
    notebooks = list_items(workspace_id, "Notebook", creds)
    matches = [nb for nb in notebooks if nb["displayName"] == notebook_name]

    if not matches:
        print(f"❌ Notebook '{notebook_name}' not found in workspace '{workspace_name}'.")
        return
    if len(matches) > 1:
        print(f"❌ Multiple notebooks named '{notebook_name}' found. Cannot safely delete.")
        return

    notebook_id = matches[0]["id"]
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks/{notebook_id}"

    response = requests.delete(url, headers=headers)
    if response.status_code == 200:
        print(f"✅ Deleted notebook '{notebook_name}' from workspace '{workspace_name}'.")
    elif response.status_code == 404:
        print(f"❌ Notebook not found: '{notebook_name}' in workspace '{workspace_name}'.")
    else:
        print(f"❌ Failed to delete notebook: {response.status_code} {response.text}")


# =============================================================================
# Deploy Notebook
# =============================================================================

def deploy_notebook(
    source_workspace_name: str,
    source_notebook_name: str,
    target_workspace_name: str,
    target_notebook_name: str,
    target_lakehouse_name: str,
    creds: Optional[dict] = None,
):
    """
    Deploy a notebook from a source workspace to a target workspace,
    patching the default lakehouse connection.

    If the target notebook already exists it is updated in-place;
    otherwise a new notebook is created.

    Args:
        source_workspace_name: Name of the source workspace.
        source_notebook_name: Name of the source notebook.
        target_workspace_name: Name of the target workspace.
        target_notebook_name: Display name for the target notebook.
        target_lakehouse_name: Name of the default lakehouse in the target workspace.
        creds: Optional credentials dict. Not needed in Fabric notebooks.

    Returns:
        The notebook ID on success, or None on failure.
    """
    source_ws = get_workspace_id_by_name(source_workspace_name, creds)
    target_ws = get_workspace_id_by_name(target_workspace_name, creds)
    headers = _fabric_headers(creds)

    # --- resolve lakehouse ID in target workspace -------------------------
    try:
        lakehouse_id = get_lakehouse_id_by_name(target_workspace_name, target_lakehouse_name, creds)
    except Exception as e:
        print(f"❌ Lakehouse lookup failed: {e}")
        return None

    # --- fetch source notebook definition and patch lakehouse metadata ----
    source_nb_id = get_item_id_by_name(source_workspace_name, source_notebook_name, "Notebook", creds)
    definition_raw = _get_notebook_definition(source_ws, source_nb_id, creds)
    if definition_raw is None:
        return None

    try:
        # find the ipynb content part (path may vary by API version)
        part_paths = [p["path"] for p in definition_raw["parts"]]
        ipynb_part = next(
            (p for p in definition_raw["parts"] if p["path"].endswith(".ipynb")),
            None,
        )
        if ipynb_part is None:
            print(f"❌ No .ipynb part found in notebook definition. Available parts: {part_paths}")
            return None

        nb_obj = json.loads(base64.b64decode(ipynb_part["payload"]))

        # ensure the metadata path exists before patching
        metadata = nb_obj.setdefault("metadata", {})
        dependencies = metadata.setdefault("dependencies", {})
        lakehouse_meta = dependencies.setdefault("lakehouse", {})

        # patch lakehouse metadata
        lakehouse_meta.update({
            "known_lakehouses": [{"id": lakehouse_id}],
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": target_lakehouse_name,
            "default_lakehouse_workspace_id": target_ws,
        })
        content_b64 = base64.b64encode(json.dumps(nb_obj).encode()).decode()
    except Exception as e:
        print(f"⚠️ Could not patch notebook '{source_notebook_name}': {type(e).__name__}: {e}")
        return None

    # --- check if target notebook already exists --------------------------
    existing_notebooks = list_items(target_ws, "Notebook", creds)
    matches = [nb for nb in existing_notebooks if nb["displayName"] == target_notebook_name]

    definition_payload = {
        "format": "ipynb",
        "parts": [{
            "path": "artifact.content.ipynb",
            "payload": content_b64,
            "payloadType": "InlineBase64",
        }],
    }

    if not matches:
        # --- create new notebook ------------------------------------------
        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/items",
            headers=headers,
            json={
                "displayName": target_notebook_name,
                "type": "Notebook",
                "definition": definition_payload,
            },
        )
        if resp.status_code == 202:
            nb_id = _poll_lro(resp.headers["Location"], "Notebook creation", creds)
        elif resp.status_code in (200, 201):
            nb_id = resp.json().get("id")
        else:
            print(f"❌ Create failed: {resp.status_code} {resp.text}")
            return None
        print(f"✅ Created notebook '{target_notebook_name}'")
    else:
        # --- update existing notebook -------------------------------------
        nb_id = matches[0]["id"]

        # Get the .platform from the TARGET notebook (required for updateMetadata)
        target_def = _get_notebook_definition(target_ws, nb_id, creds)
        if target_def:
            platform_payload = next(
                (p["payload"] for p in target_def["parts"] if p["path"] == ".platform"),
                None,
            )
            if platform_payload:
                definition_payload["parts"].append({
                    "path": ".platform",
                    "payload": platform_payload,
                    "payloadType": "InlineBase64",
                })

        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/notebooks/{nb_id}/updateDefinition?updateMetadata=true",
            headers=headers,
            json={"definition": definition_payload},
        )
        if resp.status_code == 202:
            result = _poll_lro(resp.headers["Location"], "Notebook update", creds)
            if result is None:
                return None
        elif resp.status_code != 200:
            print(f"❌ Update failed: {resp.status_code} {resp.text}")
            return None
        print(f"✅ Updated notebook '{target_notebook_name}'")

    print(f"✅ Finished '{source_notebook_name}' → '{target_notebook_name}'")
    return nb_id
