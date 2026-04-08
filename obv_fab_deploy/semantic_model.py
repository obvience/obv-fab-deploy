"""
Semantic model deployment module.
Deploys Fabric semantic models from a source to a target workspace
and rebinds Direct Lake connections using direct Fabric REST API calls.
"""

import requests
import time
import json
import base64
import re
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

def _poll_lro(url: str, label: str, creds: Optional[dict] = None, max_attempts: int = 15, interval: int = 5, fetch_result: bool = False):
    """
    Poll a long-running operation until it succeeds, fails, or times out.

    Args:
        url: The Location URL from the initial 202 response.
        label: Human-readable label for logging.
        creds: Optional credentials dict.
        max_attempts: Max polling iterations.
        interval: Seconds between polls.
        fetch_result: If True, fetch the /result endpoint on success (for create operations).
                      If False, just return True on success (for update operations).

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
# Semantic Model Definition Helper
# =============================================================================

def _get_semantic_model_definition(
    workspace_id: str, model_id: str, format: str = "TMDL", creds: Optional[dict] = None
) -> Optional[dict]:
    """
    Fetch the full definition of a semantic model via REST API.

    Args:
        workspace_id: The workspace ID (GUID).
        model_id: The semantic model item ID (GUID).
        format: Definition format — "TMDL" (default) or "TMSL".
        creds: Optional credentials dict.

    Returns:
        The definition dict (with 'parts' list), or None on failure.
    """
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition"
    if format:
        url += f"?format={format}"

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
# Direct Lake Mode Detection
# =============================================================================

def _detect_direct_lake_mode(definition: dict) -> Optional[str]:
    """
    Detect whether a semantic model uses Direct Lake on OneLake or Direct Lake on SQL.

    Scans the base64-encoded TMDL definition parts for the connection expression:
      - AzureStorage.DataLake(...) → "onelake"
      - Sql.Database(...)          → "sql"

    Returns:
        "onelake", "sql", or None if no known pattern is found.
    """
    for part in definition.get("parts", []):
        payload_b64 = part.get("payload", "")
        try:
            content = base64.b64decode(payload_b64).decode("utf-8")
        except Exception:
            continue
        if "AzureStorage.DataLake" in content:
            return "onelake"
        if "Sql.Database" in content:
            return "sql"
    return None


# =============================================================================
# SQL Analytics Endpoint Lookup
# =============================================================================

def _get_lakehouse_sql_endpoint(
    workspace_id: str, lakehouse_id: str, creds: Optional[dict] = None
) -> tuple:
    """
    Fetch the SQL analytics endpoint connection string and ID for a lakehouse.

    Calls GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId} and extracts
    the sqlEndpointProperties from the response.

    Returns:
        (connection_string, endpoint_id) tuple.

    Raises:
        ValueError if the SQL endpoint is not provisioned or unavailable.
    """
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    props = resp.json().get("properties", {})
    sql_props = props.get("sqlEndpointProperties", {})
    connection_string = sql_props.get("connectionString")
    endpoint_id = sql_props.get("id")

    if not connection_string or not endpoint_id:
        raise ValueError(
            f"SQL analytics endpoint not available for lakehouse {lakehouse_id}. "
            "It may still be provisioning — check the Fabric portal."
        )
    return connection_string, endpoint_id


# =============================================================================
# Direct Lake Connection Patcher
# =============================================================================

def _patch_direct_lake_connection(
    definition: dict,
    target_workspace_id: str,
    target_lakehouse_id: str,
    target_lakehouse_name: str,
    target_model_name: str,
    direct_lake_mode: Optional[str] = None,
    sql_endpoint_connection_string: Optional[str] = None,
    sql_endpoint_id: Optional[str] = None,
    target_schema_name: Optional[str] = None,
) -> dict:
    """
    Patch TMDL definition parts to point to a new lakehouse.

    Handles both Direct Lake modes:

      - **Direct Lake on OneLake** — patches the AzureStorage.DataLake() URL:
            AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/{ws}/{lh}")
            Also strips all schemaName lines (not used by DL-on-OneLake).

      - **Direct Lake on SQL** — patches the Sql.Database() connection:
            Sql.Database("{sql_endpoint_fqdn}", "{sql_endpoint_id}")
            Optionally remaps schemaName if target_schema_name is provided.

    Also patches the .platform displayName to match the target model name.

    Args:
        definition: The definition dict with a 'parts' list.
        target_workspace_id: Target workspace GUID.
        target_lakehouse_id: Target lakehouse GUID.
        target_lakehouse_name: Target lakehouse display name.
        target_model_name: Target semantic model display name.
        direct_lake_mode: "onelake" or "sql" (from _detect_direct_lake_mode).
        sql_endpoint_connection_string: SQL analytics endpoint FQDN (required for "sql" mode).
        sql_endpoint_id: SQL analytics endpoint GUID (required for "sql" mode).
        target_schema_name: Schema name for the target lakehouse (e.g. "dbo", "o2c").
            For DL-on-SQL: if provided, all schemaName values are remapped to this.
            For DL-on-OneLake: ignored (schemaName lines are always stripped).

    Returns:
        The patched definition dict.
    """
    new_onelake_url = f"https://onelake.dfs.fabric.microsoft.com/{target_workspace_id}/{target_lakehouse_id}"
    patched_count = 0

    for part in definition["parts"]:
        payload_b64 = part.get("payload", "")
        try:
            content = base64.b64decode(payload_b64).decode("utf-8")
        except Exception:
            continue

        original = content

        # --- Patch .platform displayName ---
        if part["path"] == ".platform":
            try:
                platform_obj = json.loads(content)
                platform_obj["metadata"]["displayName"] = target_model_name
                content = json.dumps(platform_obj, indent=2)
            except Exception:
                pass

        # --- Patch connection based on Direct Lake mode ---
        if direct_lake_mode == "onelake":
            # Direct Lake on OneLake: patch AzureStorage.DataLake URL
            content = re.sub(
                r'AzureStorage\.DataLake\s*\(\s*"https://onelake\.dfs\.fabric\.microsoft\.com/[^"]*"',
                f'AzureStorage.DataLake("{new_onelake_url}"',
                content,
            )
            # DL-on-OneLake does not use schemaName — strip all occurrences
            content = re.sub(r'\n[ \t]*schemaName:.*', '', content)
        elif direct_lake_mode == "sql" and sql_endpoint_connection_string and sql_endpoint_id:
            # Direct Lake on SQL: patch Sql.Database with target SQL analytics endpoint
            content = re.sub(
                r'Sql\.Database\s*\(\s*"[^"]*"\s*,\s*"[^"]*"\s*\)',
                f'Sql.Database("{sql_endpoint_connection_string}", "{sql_endpoint_id}")',
                content,
            )
            # Remap schemaName if a target schema was specified
            if target_schema_name:
                content = re.sub(
                    r'(schemaName:)\s*\S+',
                    f'\\1 {target_schema_name}',
                    content,
                )
        else:
            # Unknown mode — try OneLake pattern as fallback
            content = re.sub(
                r'AzureStorage\.DataLake\s*\(\s*"https://onelake\.dfs\.fabric\.microsoft\.com/[^"]*"',
                f'AzureStorage.DataLake("{new_onelake_url}"',
                content,
            )

        if content != original:
            part["payload"] = base64.b64encode(content.encode("utf-8")).decode()
            patched_count += 1
            print(f"   🔗 Patched: {part['path']}")

    if patched_count == 0:
        print("   ⚠️ No lakehouse connection expressions found to patch — model may not be Direct Lake.")

    return definition


# =============================================================================
# Deploy Semantic Model
# =============================================================================

def deploy_semantic_model(
    source_workspace_name: str,
    source_semantic_model_name: str,
    target_workspace_name: str,
    target_semantic_model_name: str,
    target_lakehouse_name: str,
    target_schema_name: Optional[str] = None,
    creds: Optional[dict] = None,
):
    """
    Deploy a semantic model from a source workspace to a target workspace,
    and rebind its Direct Lake connection to the specified target lakehouse.

    If the target semantic model already exists it is updated in-place;
    otherwise a new one is created.

    Args:
        source_workspace_name: Name of the source workspace.
        source_semantic_model_name: Name of the source semantic model.
        target_workspace_name: Name of the target workspace.
        target_semantic_model_name: Display name for the target semantic model.
        target_lakehouse_name: Name of the lakehouse to rebind to in the target workspace.
        target_schema_name: Optional schema name for the target lakehouse (e.g. "dbo").
            For DL-on-SQL models, remaps all schemaName values to this.
            For DL-on-OneLake, schemaName lines are always stripped regardless.
            If None and DL-on-SQL, schema names are left unchanged from source.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
    """
    # --- resolve workspace IDs --------------------------------------------
    try:
        source_ws = get_workspace_id_by_name(source_workspace_name, creds)
        target_ws = get_workspace_id_by_name(target_workspace_name, creds)
    except Exception as e:
        print(f"❌ Workspace resolution failed: {e}")
        return

    # --- get source model definition (TMDL) -------------------------------
    source_model_id = get_item_id_by_name(source_workspace_name, source_semantic_model_name, "SemanticModel", creds)
    definition = _get_semantic_model_definition(source_ws, source_model_id, creds=creds)
    if definition is None:
        print(f"❌ Could not retrieve definition for '{source_semantic_model_name}'")
        return

    # --- resolve target lakehouse and patch connection --------------------
    try:
        target_lakehouse_id = get_lakehouse_id_by_name(target_workspace_name, target_lakehouse_name, creds)

        # Detect Direct Lake mode (OneLake vs SQL)
        dl_mode = _detect_direct_lake_mode(definition)
        print(f"   Detected Direct Lake mode: {dl_mode or 'unknown'}")

        sql_conn_str = None
        sql_ep_id = None

        if dl_mode == "sql":
            try:
                sql_conn_str, sql_ep_id = _get_lakehouse_sql_endpoint(target_ws, target_lakehouse_id, creds)
                print(f"   Resolved target SQL endpoint: {sql_conn_str}")
            except Exception as e:
                print(f"   ⚠️ Could not fetch SQL endpoint for target lakehouse: {e}")

        print(f"   Patching Direct Lake connection → {target_lakehouse_name}")
        definition = _patch_direct_lake_connection(
            definition,
            target_workspace_id=target_ws,
            target_lakehouse_id=target_lakehouse_id,
            target_lakehouse_name=target_lakehouse_name,
            target_model_name=target_semantic_model_name,
            direct_lake_mode=dl_mode,
            sql_endpoint_connection_string=sql_conn_str,
            sql_endpoint_id=sql_ep_id,
            target_schema_name=target_schema_name,
        )
    except Exception as e:
        print(f"⚠️ Lakehouse lookup / rebinding failed: {e}. Deploying without rebinding.")

    # --- check if target model already exists -----------------------------
    existing_models = list_items(target_ws, "SemanticModel", creds)
    matches = [m for m in existing_models if m["displayName"] == target_semantic_model_name]
    headers = _fabric_headers(creds)

    if not matches:
        # --- create new semantic model ------------------------------------
        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/items",
            headers=headers,
            json={
                "displayName": target_semantic_model_name,
                "type": "SemanticModel",
                "definition": definition,
            },
        )
        if resp.status_code == 202:
            result = _poll_lro(resp.headers["Location"], "Semantic model creation", creds, fetch_result=True)
            if result is None:
                return
        elif resp.status_code in (200, 201):
            pass
        else:
            print(f"❌ Create failed: {resp.status_code} {resp.text}")
            return
        print(f"✅ Created semantic model '{target_semantic_model_name}'")
    else:
        # --- update existing semantic model --------------------------------
        model_id = matches[0]["id"]
        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/semanticModels/{model_id}/updateDefinition?updateMetadata=true",
            headers=headers,
            json={"definition": definition},
        )
        if resp.status_code == 202:
            result = _poll_lro(resp.headers["Location"], "Semantic model update", creds)
            if result is None:
                return
        elif resp.status_code != 200:
            print(f"❌ Update failed: {resp.status_code} {resp.text}")
            return
        print(f"✅ Updated semantic model '{target_semantic_model_name}'")

    print(f"✅ Finished '{source_semantic_model_name}' → '{target_semantic_model_name}'")
