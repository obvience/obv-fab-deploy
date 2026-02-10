"""
Pipeline deployment module.
Deploys Fabric data pipelines from a source to a target workspace
using direct Fabric REST API calls.

Rebinds Copy-activity sinks to the target lakehouse and
TridentNotebook activities to the matching notebooks in the target workspace.
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
# Pipeline Definition Helper
# =============================================================================

def _get_pipeline_definition(
    workspace_id: str, pipeline_id: str, creds: Optional[dict] = None
) -> Optional[dict]:
    """
    Fetch the full definition of a data pipeline via REST API.
    Handles both synchronous (200) and long-running (202) responses.

    Args:
        workspace_id: The workspace ID (GUID).
        pipeline_id: The pipeline item ID (GUID).
        creds: Optional credentials dict.

    Returns:
        The definition dict (with 'parts' list), or None on failure.
    """
    headers = _fabric_headers(creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/getDefinition"

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
# Pipeline Content Patcher
# =============================================================================

def _patch_pipeline_content(
    definition: dict,
    target_workspace_id: str,
    target_lakehouse_id: str,
    source_workspace_id: str,
    creds: Optional[dict] = None,
) -> dict:
    """
    Patch the pipeline-content.json part to rebind activities to the target workspace.

    - Copy activity sinks (LakehouseTableSink) → target workspace/lakehouse
    - TridentNotebook activities → matching notebook IDs in the target workspace (by name)

    Args:
        definition: The definition dict with a 'parts' list.
        target_workspace_id: Target workspace GUID.
        target_lakehouse_id: Target lakehouse GUID.
        source_workspace_id: Source workspace GUID (for notebook name lookup).
        creds: Optional credentials dict.

    Returns:
        The patched definition dict.
    """
    # Find the pipeline-content.json part
    content_part = next(
        (p for p in definition["parts"] if p["path"] == "pipeline-content.json"),
        None,
    )
    if content_part is None:
        print("⚠️ pipeline-content.json not found in definition")
        return definition

    pipeline_dict = json.loads(base64.b64decode(content_part["payload"]).decode("utf-8"))

    # --- Build notebook name→ID maps for source and target ----------------
    src_notebooks = list_items(source_workspace_id, "Notebook", creds)
    tgt_notebooks = list_items(target_workspace_id, "Notebook", creds)

    src_nb_id_to_name = {nb["id"]: nb["displayName"] for nb in src_notebooks}
    tgt_nb_name_to_id = {nb["displayName"]: nb["id"] for nb in tgt_notebooks}

    # --- Patch activities -------------------------------------------------
    for activity in pipeline_dict.get("properties", {}).get("activities", []):

        # Rebind Copy-activity sinks to target lakehouse
        if activity.get("type") == "Copy":
            sink = activity.get("typeProperties", {}).get("sink", {})
            if sink.get("type") == "LakehouseTableSink":
                type_props = (
                    sink.get("datasetSettings", {})
                        .get("linkedService", {})
                        .get("properties", {})
                        .get("typeProperties", {})
                )
                type_props["workspaceId"] = str(target_workspace_id)
                type_props["artifactId"] = str(target_lakehouse_id)
                print(f"   🔗 Rebound sink in Copy activity '{activity.get('name')}'")

        # Rebind TridentNotebook activities to target notebook IDs
        elif activity.get("type") == "TridentNotebook":
            nb_id = activity.get("typeProperties", {}).get("notebookId")
            nb_name = src_nb_id_to_name.get(nb_id)
            if nb_name:
                tgt_nb_id = tgt_nb_name_to_id.get(nb_name)
                if tgt_nb_id:
                    activity["typeProperties"]["notebookId"] = tgt_nb_id
                    activity["typeProperties"]["workspaceId"] = str(target_workspace_id)
                    print(f"   🔗 Rebound notebook '{nb_name}' → {tgt_nb_id}")
                else:
                    print(f"   ⚠️ Target notebook '{nb_name}' not found in target workspace")
            else:
                print(f"   ⚠️ Source notebook ID '{nb_id}' not found in source workspace")

    # Re-encode the patched content
    content_part["payload"] = base64.b64encode(
        json.dumps(pipeline_dict).encode("utf-8")
    ).decode()

    return definition


# =============================================================================
# Deploy Pipeline
# =============================================================================

def deploy_pipeline(
    source_workspace_name: str,
    pipeline_name: str,
    target_workspace_name: str,
    target_lakehouse_name: str,
    target_pipeline_name: Optional[str] = None,
    creds: Optional[dict] = None,
):
    """
    Deploy a data pipeline from a source workspace to a target workspace.
    Rebinds Copy-activity sinks to the target lakehouse and
    TridentNotebook activities to matching notebooks in the target workspace.

    If the target pipeline already exists it is updated in-place;
    otherwise a new pipeline is created.

    Args:
        source_workspace_name: Name of the source workspace.
        pipeline_name: Name of the pipeline in the source workspace.
        target_workspace_name: Name of the target workspace.
        target_lakehouse_name: Name of the lakehouse to rebind sinks to.
        target_pipeline_name: Display name for the target pipeline.
                              Defaults to the source pipeline_name if not provided.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
    """
    if target_pipeline_name is None:
        target_pipeline_name = pipeline_name

    # --- resolve workspace and lakehouse IDs ------------------------------
    try:
        source_ws = get_workspace_id_by_name(source_workspace_name, creds)
        target_ws = get_workspace_id_by_name(target_workspace_name, creds)
        target_lh = get_lakehouse_id_by_name(target_workspace_name, target_lakehouse_name, creds)
    except Exception as e:
        print(f"❌ Could not resolve IDs: {e}")
        return

    # --- get source pipeline definition -----------------------------------
    source_pipeline_id = get_item_id_by_name(
        source_workspace_name, pipeline_name, "DataPipeline", creds
    )
    definition = _get_pipeline_definition(source_ws, source_pipeline_id, creds)
    if definition is None:
        print(f"❌ Could not retrieve definition for pipeline '{pipeline_name}'")
        return

    # --- patch activities to target workspace/lakehouse -------------------
    print(f"   Patching pipeline activities → {target_lakehouse_name}")
    definition = _patch_pipeline_content(
        definition, target_ws, target_lh, source_ws, creds
    )

    # --- check if target pipeline already exists --------------------------
    existing_pipelines = list_items(target_ws, "DataPipeline", creds)
    matches = [p for p in existing_pipelines if p["displayName"] == target_pipeline_name]
    headers = _fabric_headers(creds)

    if matches:
        # --- update existing pipeline -------------------------------------
        pipeline_id = matches[0]["id"]
        print(f"🛠️ Pipeline '{target_pipeline_name}' exists. Updating definition...")

        # Get .platform from TARGET pipeline (required for updateMetadata)
        target_def = _get_pipeline_definition(target_ws, pipeline_id, creds)
        if target_def:
            platform_payload = next(
                (p["payload"] for p in target_def["parts"] if p["path"] == ".platform"),
                None,
            )
            if platform_payload:
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
            f"{FABRIC_API}/workspaces/{target_ws}/dataPipelines/{pipeline_id}/updateDefinition?updateMetadata=true",
            headers=headers,
            json={"definition": definition},
        )
        if resp.status_code == 202:
            result = _poll_lro(resp.headers["Location"], "Pipeline update", creds)
            if result is None:
                return
        elif resp.status_code != 200:
            print(f"❌ Update failed: {resp.status_code} {resp.text}")
            return
        print(f"✅ Updated pipeline '{target_pipeline_name}'")
    else:
        # --- create new pipeline ------------------------------------------
        print(f"📄 Pipeline '{target_pipeline_name}' not found. Creating...")

        # Patch .platform displayName for the new pipeline
        for part in definition.get("parts", []):
            if part["path"] == ".platform":
                try:
                    content = base64.b64decode(part["payload"]).decode("utf-8")
                    platform_obj = json.loads(content)
                    platform_obj["metadata"]["displayName"] = target_pipeline_name
                    part["payload"] = base64.b64encode(
                        json.dumps(platform_obj, indent=2).encode()
                    ).decode()
                except Exception:
                    pass

        resp = requests.post(
            f"{FABRIC_API}/workspaces/{target_ws}/items",
            headers=headers,
            json={
                "displayName": target_pipeline_name,
                "type": "DataPipeline",
                "definition": definition,
            },
        )
        if resp.status_code == 202:
            result = _poll_lro(
                resp.headers["Location"], "Pipeline creation", creds, fetch_result=True
            )
            if result is None:
                return
        elif resp.status_code in (200, 201):
            pass
        else:
            print(f"❌ Create failed: {resp.status_code} {resp.text}")
            return
        print(f"✅ Created pipeline '{target_pipeline_name}'")

    print(f"✅ Finished '{pipeline_name}' → '{target_pipeline_name}'")
