"""
Lakehouse deployment module.
Deploys a lakehouse with OneLake shortcuts from a source to a target workspace
using direct Fabric REST API calls.
"""

import requests
from typing import Optional

from .utils import (
    FABRIC_API,
    _fabric_headers,
    get_workspace_id_by_name,
    get_lakehouse_id_by_name,
    list_items,
)


# =============================================================================
# Shortcut Helpers
# =============================================================================

def _list_shortcuts(
    workspace_id: str,
    item_id: str,
    creds: Optional[dict] = None
) -> list[dict]:
    """
    List all shortcuts for a lakehouse, handling pagination.

    Args:
        workspace_id: The workspace ID (GUID).
        item_id: The lakehouse item ID (GUID).
        creds: Optional credentials dict.

    Returns:
        List of shortcut dicts, each with 'name', 'path', and 'target' keys.
    """
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/shortcuts"

    all_shortcuts = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        all_shortcuts.extend(data.get("value", []))
        url = data.get("continuationUri")

    return all_shortcuts


def _create_onelake_shortcut(
    workspace_id: str,
    item_id: str,
    shortcut_name: str,
    shortcut_path: str,
    source_workspace_id: str,
    source_item_id: str,
    source_path: str,
    conflict_policy: str = "Abort",
    creds: Optional[dict] = None
):
    """
    Create a OneLake shortcut in a lakehouse.

    Args:
        workspace_id: Workspace ID where the shortcut is created.
        item_id: Item ID (lakehouse) where the shortcut is created.
        shortcut_name: Display name of the shortcut.
        shortcut_path: Parent path in the destination (e.g., "Tables" or "Files/folder").
        source_workspace_id: Source workspace ID the shortcut points to.
        source_item_id: Source item ID the shortcut points to.
        source_path: Full path within the source item the shortcut points to.
        conflict_policy: "Abort", "GenerateUniqueName", or "CreateOrOverwrite".
        creds: Optional credentials dict.
    """
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/shortcuts"
    if conflict_policy:
        url += f"?shortcutConflictPolicy={conflict_policy}"

    payload = {
        "name": shortcut_name,
        "path": shortcut_path,
        "target": {
            "oneLake": {
                "workspaceId": source_workspace_id,
                "itemId": source_item_id,
                "path": source_path
            }
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()


# =============================================================================
# Lakehouse Deployment
# =============================================================================

def deploy_lakehouse_with_shortcuts(
    source_workspace_name: str,
    source_lakehouse_name: str,
    target_workspace_name: str,
    target_lakehouse_name: str,
    creds: Optional[dict] = None
):
    """
    Deploy a lakehouse by creating it in the target workspace (if it doesn't exist)
    and replicating all OneLake shortcuts from the source lakehouse.

    Each shortcut created in the target points back to the corresponding
    path in the source lakehouse, so data is accessed via shortcut chain.

    Args:
        source_workspace_name: Name of the source workspace.
        source_lakehouse_name: Name of the source lakehouse.
        target_workspace_name: Name of the target workspace.
        target_lakehouse_name: Name of the target lakehouse to create/use.
        creds: Optional credentials dict with tenant_id, client_id, client_secret.
               Not needed in Fabric notebooks.
    """
    source_workspace_id = get_workspace_id_by_name(source_workspace_name, creds)
    target_workspace_id = get_workspace_id_by_name(target_workspace_name, creds)

    # Create target lakehouse if needed
    existing_lakehouses = list_items(target_workspace_id, "Lakehouse", creds)
    lakehouse_names = [lh["displayName"] for lh in existing_lakehouses]

    if target_lakehouse_name not in lakehouse_names:
        print(f"🔨 Creating lakehouse '{target_lakehouse_name}' in workspace '{target_workspace_name}'")
        headers = _fabric_headers(creds)
        payload = {
            "displayName": target_lakehouse_name,
            "description": "A schema enabled lakehouse.",
            "creationPayload": {"enableSchemas": True}
        }
        response = requests.post(
            f"{FABRIC_API}/workspaces/{target_workspace_id}/lakehouses",
            headers=headers,
            json=payload
        )
        if response.status_code in (201, 202):
            print(f"✅ Created lakehouse '{target_lakehouse_name}'. Response: {response.json()}")
        else:
            print(f"❌ Failed to create lakehouse. Status: {response.status_code}\n{response.text}")
            return
    else:
        print(f"✅ Lakehouse '{target_lakehouse_name}' already exists in workspace '{target_workspace_name}'")

    # Look up lakehouse IDs
    source_lakehouse_id = get_lakehouse_id_by_name(source_workspace_name, source_lakehouse_name, creds)
    target_lakehouse_id = get_lakehouse_id_by_name(target_workspace_name, target_lakehouse_name, creds)

    # List existing shortcuts in target to avoid duplicates
    existing_shortcuts = _list_shortcuts(target_workspace_id, target_lakehouse_id, creds)
    existing_shortcut_names = {s["name"] for s in existing_shortcuts}

    # List shortcuts in source to replicate
    source_shortcuts = _list_shortcuts(source_workspace_id, source_lakehouse_id, creds)

    for shortcut in source_shortcuts:
        shortcut_name = shortcut["name"]
        shortcut_path = shortcut["path"]

        if shortcut_name in existing_shortcut_names:
            print(f"⚠️ Shortcut '{shortcut_name}' already exists in target. Skipping.")
            continue

        # Full path in the source lakehouse that the new shortcut will point to
        source_data_path = f"{shortcut_path}/{shortcut_name}"

        print(f"Creating shortcut '{shortcut_name}' at path '{shortcut_path}'")
        _create_onelake_shortcut(
            workspace_id=target_workspace_id,
            item_id=target_lakehouse_id,
            shortcut_name=shortcut_name,
            shortcut_path=shortcut_path,
            source_workspace_id=source_workspace_id,
            source_item_id=source_lakehouse_id,
            source_path=source_data_path,
            conflict_policy="Abort",
            creds=creds
        )
        print(f"✅ Created shortcut '{shortcut_name}' at path '{shortcut_path}'")
