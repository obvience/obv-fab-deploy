"""
Fabric Artifact Deployment Library
===================================
A simple library for deploying Microsoft Fabric artifacts.
Works in Fabric notebooks (no setup needed) or regular Python notebooks (pass credentials).

Requirements:
    pip install azure-identity requests

Usage:
    # In Fabric notebook - just works, no credentials needed
    workspace_id = get_workspace_id_by_name("My Workspace")
    
    # In regular Python notebook - pass your credentials
    creds = {
        "tenant_id": "your-tenant-id",
        "client_id": "your-client-id",
        "client_secret": "your-client-secret"
    }
    workspace_id = get_workspace_id_by_name("My Workspace", creds)
"""

import requests
import time
from typing import Optional
from azure.identity import DefaultAzureCredential, ClientSecretCredential


# =============================================================================
# Authentication Helper
# =============================================================================

def _get_fabric_notebook_token(scope: str) -> Optional[str]:
    """
    Try to get a token using Fabric notebook built-in credentials.
    Returns the token string if running in a Fabric notebook, or None otherwise.
    """
    # Strip "/.default" suffix — Fabric's getToken expects the base resource URL
    resource = scope.replace("/.default", "")

    # Try notebookutils (newer Fabric runtime)
    try:
        import notebookutils
        return notebookutils.credentials.getToken(resource)
    except Exception:
        pass

    # Try mssparkutils (older Fabric runtime)
    try:
        import mssparkutils
        return mssparkutils.credentials.getToken(resource)
    except Exception:
        pass

    return None


def _get_token(scope: str, creds: Optional[dict] = None) -> str:
    """
    Get an access token for the specified scope.
    
    Args:
        scope: The API scope (e.g., "https://api.fabric.microsoft.com/.default")
        creds: Optional credentials dict with tenant_id, client_id, client_secret.
               If not provided, tries Fabric notebook auth first, then DefaultAzureCredential.
    
    Returns:
        Access token string.
    """
    try:
        if creds:
            # Use provided Service Principal credentials
            credential = ClientSecretCredential(
                tenant_id=creds["tenant_id"],
                client_id=creds["client_id"],
                client_secret=creds["client_secret"]
            )
            return credential.get_token(scope).token

        # Try Fabric notebook native auth first
        fabric_token = _get_fabric_notebook_token(scope)
        if fabric_token:
            return fabric_token

        # Fall back to DefaultAzureCredential (az login, env vars, managed identity, etc.)
        credential = DefaultAzureCredential()
        return credential.get_token(scope).token
    
    except Exception as e:
        if creds:
            raise RuntimeError(
                f"Failed to authenticate with provided credentials. "
                f"Please check your tenant_id, client_id, and client_secret.\n"
                f"Error: {e}"
            )
        else:
            raise RuntimeError(
                f"Failed to authenticate automatically. "
                f"If you're not running in a Fabric notebook, please provide credentials:\n\n"
                f"    creds = {{\n"
                f'        "tenant_id": "your-tenant-id",\n'
                f'        "client_id": "your-client-id",\n'
                f'        "client_secret": "your-client-secret"\n'
                f"    }}\n"
                f"    result = your_function(..., creds)\n\n"
                f"Error: {e}"
            )


def _fabric_headers(creds: Optional[dict] = None) -> dict:
    """Get headers for Fabric API requests."""
    token = _get_token("https://api.fabric.microsoft.com/.default", creds)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def _powerbi_headers(creds: Optional[dict] = None) -> dict:
    """Get headers for Power BI API requests."""
    token = _get_token("https://analysis.windows.net/powerbi/api/.default", creds)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


# =============================================================================
# API Base URLs
# =============================================================================

FABRIC_API = "https://api.fabric.microsoft.com/v1"
POWERBI_API = "https://api.powerbi.com/v1.0/myorg"


# =============================================================================
# Helper Functions
# =============================================================================

def _is_guid(value: str) -> bool:
    """Check if a string looks like a GUID."""
    return len(value) == 36 and value.count('-') == 4


# =============================================================================
# Workspace Functions
# =============================================================================

def list_workspaces(creds: Optional[dict] = None) -> list[dict]:
    """
    List all workspaces you have access to.
    
    Args:
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        List of workspace dictionaries with 'id', 'displayName', etc.
        
    Example:
        workspaces = list_workspaces()
        for ws in workspaces:
            print(ws['displayName'])
    """
    url = f"{FABRIC_API}/workspaces"
    response = requests.get(url, headers=_fabric_headers(creds))
    response.raise_for_status()
    return response.json().get("value", [])


def get_workspace_id_by_name(workspace_name: str, creds: Optional[dict] = None) -> str:
    """
    Get a workspace ID by its name.
    
    Args:
        workspace_name: The name of the workspace.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        The workspace ID (GUID).
        
    Example:
        workspace_id = get_workspace_id_by_name("Sales Analytics")
    """
    workspaces = list_workspaces(creds)
    matches = [w for w in workspaces if w["displayName"] == workspace_name]
    
    if not matches:
        available = [w["displayName"] for w in workspaces[:10]]
        raise ValueError(
            f"Workspace '{workspace_name}' not found.\n"
            f"Available workspaces: {available}{'...' if len(workspaces) > 10 else ''}"
        )
    if len(matches) > 1:
        raise ValueError(f"Multiple workspaces found with name '{workspace_name}'. Please use the workspace ID instead.")
    
    return matches[0]["id"]


def _resolve_workspace_id(workspace_name_or_id: str, creds: Optional[dict] = None) -> str:
    """Resolve a workspace name or ID to an ID."""
    if _is_guid(workspace_name_or_id):
        return workspace_name_or_id
    return get_workspace_id_by_name(workspace_name_or_id, creds)


# =============================================================================
# Item Functions (Lakehouses, Semantic Models, Reports, etc.)
# =============================================================================

def list_items(workspace: str, item_type: Optional[str] = None, creds: Optional[dict] = None) -> list[dict]:
    """
    List items in a workspace.
    
    Args:
        workspace: Workspace name or ID.
        item_type: Optional filter - 'Lakehouse', 'SemanticModel', 'Report', etc.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        List of item dictionaries.
        
    Example:
        # List all items
        items = list_items("Sales Analytics")
        
        # List only semantic models
        models = list_items("Sales Analytics", "SemanticModel")
    """
    workspace_id = _resolve_workspace_id(workspace, creds)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items"
    if item_type:
        url += f"?type={item_type}"
    
    response = requests.get(url, headers=_fabric_headers(creds))
    response.raise_for_status()
    return response.json().get("value", [])


def get_item_id_by_name(workspace: str, item_name: str, item_type: str, creds: Optional[dict] = None) -> str:
    """
    Get an item's ID by its name and type.
    
    Args:
        workspace: Workspace name or ID.
        item_name: The display name of the item.
        item_type: The type - 'Lakehouse', 'SemanticModel', 'Report', etc.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        The item ID (GUID).
        
    Example:
        model_id = get_item_id_by_name("Sales Analytics", "Sales Model", "SemanticModel")
    """
    items = list_items(workspace, item_type, creds)
    matches = [i for i in items if i["displayName"] == item_name]
    
    if not matches:
        available = [i["displayName"] for i in items[:10]]
        raise ValueError(
            f"{item_type} '{item_name}' not found in workspace.\n"
            f"Available {item_type}s: {available}{'...' if len(items) > 10 else ''}"
        )
    
    return matches[0]["id"]


def get_lakehouse_id_by_name(workspace: str, lakehouse_name: str, creds: Optional[dict] = None) -> str:
    """
    Get a Lakehouse ID by its name.
    
    Args:
        workspace: Workspace name or ID.
        lakehouse_name: The name of the Lakehouse.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        The Lakehouse ID (GUID).
    """
    return get_item_id_by_name(workspace, lakehouse_name, "Lakehouse", creds)


def get_dataset_id_by_name(workspace: str, dataset_name: str, creds: Optional[dict] = None) -> str:
    """
    Get a Semantic Model (Dataset) ID by its name.
    
    Args:
        workspace: Workspace name or ID.
        dataset_name: The name of the Semantic Model.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        The Semantic Model ID (GUID).
    """
    return get_item_id_by_name(workspace, dataset_name, "SemanticModel", creds)


def get_report_id_by_name(workspace: str, report_name: str, creds: Optional[dict] = None) -> str:
    """
    Get a Report ID by its name.
    
    Args:
        workspace: Workspace name or ID.
        report_name: The name of the Report.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        The Report ID (GUID).
    """
    return get_item_id_by_name(workspace, report_name, "Report", creds)


# =============================================================================
# Report Functions
# =============================================================================

def rebind_report(
    report: str,
    dataset: str,
    report_workspace: str,
    dataset_workspace: Optional[str] = None,
    creds: Optional[dict] = None
):
    """
    Rebind a report to a different semantic model (dataset).
    
    Args:
        report: Report name or ID.
        dataset: Target dataset name or ID.
        report_workspace: Workspace containing the report (name or ID).
        dataset_workspace: Workspace containing the dataset (name or ID). 
                          If not provided, uses the same workspace as the report.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Example:
        # Rebind to a dataset in the same workspace
        rebind_report("Sales Report", "Sales Model", "Sales Analytics")
        
        # Rebind to a dataset in a different workspace
        rebind_report("Sales Report", "Sales Model", "Reports WS", "Data WS")
    """
    if dataset_workspace is None:
        dataset_workspace = report_workspace
    
    report_ws_id = _resolve_workspace_id(report_workspace, creds)
    dataset_ws_id = _resolve_workspace_id(dataset_workspace, creds)
    
    # Resolve IDs if names were provided
    report_id = report if _is_guid(report) else get_report_id_by_name(report_ws_id, report, creds)
    dataset_id = dataset if _is_guid(dataset) else get_dataset_id_by_name(dataset_ws_id, dataset, creds)
    
    url = f"{POWERBI_API}/groups/{report_ws_id}/reports/{report_id}/Rebind"
    payload = {"datasetId": dataset_id}
    
    response = requests.post(url, headers=_powerbi_headers(creds), json=payload)
    response.raise_for_status()
    
    print(f"✅ Report '{report}' rebound to dataset '{dataset}'")


# =============================================================================
# Semantic Model Functions
# =============================================================================

def refresh_semantic_model(
    workspace: str,
    model: str,
    wait_for_completion: bool = False,
    creds: Optional[dict] = None
) -> Optional[str]:
    """
    Refresh a semantic model (trigger data refresh).
    
    Args:
        workspace: Workspace name or ID.
        model: Semantic model name or ID.
        wait_for_completion: If True, wait for the refresh to finish.
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        The refresh request ID.
        
    Example:
        # Start refresh and continue
        refresh_semantic_model("Sales Analytics", "Sales Model")
        
        # Wait for refresh to complete
        refresh_semantic_model("Sales Analytics", "Sales Model", wait_for_completion=True)
    """
    workspace_id = _resolve_workspace_id(workspace, creds)
    model_id = model if _is_guid(model) else get_dataset_id_by_name(workspace_id, model, creds)
    
    url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{model_id}/refreshes"
    payload = {"type": "full"}
    
    response = requests.post(url, headers=_powerbi_headers(creds), json=payload)
    response.raise_for_status()
    
    request_id = response.headers.get("x-ms-request-id")
    print(f"✅ Refresh started for '{model}'")
    
    if wait_for_completion:
        _wait_for_refresh(workspace_id, model_id, creds)
    
    return request_id


def _wait_for_refresh(workspace_id: str, model_id: str, creds: Optional[dict] = None, timeout_seconds: int = 3600):
    """Wait for a refresh to complete."""
    url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{model_id}/refreshes?$top=1"
    start_time = time.time()
    
    print("⏳ Waiting for refresh to complete...", end="", flush=True)
    
    while time.time() - start_time < timeout_seconds:
        response = requests.get(url, headers=_powerbi_headers(creds))
        response.raise_for_status()
        
        refreshes = response.json().get("value", [])
        if refreshes:
            status = refreshes[0].get("status")
            if status == "Completed":
                print("\n✅ Refresh completed successfully!")
                return
            elif status == "Failed":
                error = refreshes[0].get("serviceExceptionJson", "Unknown error")
                print(f"\n❌ Refresh failed: {error}")
                raise RuntimeError(f"Refresh failed: {error}")
            elif status in ("Unknown", "Disabled", "Cancelled"):
                print(f"\n❌ Refresh ended with status: {status}")
                raise RuntimeError(f"Refresh ended with status: {status}")
        
        print(".", end="", flush=True)
        time.sleep(10)
    
    raise TimeoutError(f"Refresh did not complete within {timeout_seconds} seconds.")


def get_refresh_history(workspace: str, model: str, top: int = 10, creds: Optional[dict] = None) -> list[dict]:
    """
    Get the refresh history for a semantic model.
    
    Args:
        workspace: Workspace name or ID.
        model: Semantic model name or ID.
        top: Number of records to return (default 10).
        creds: Optional credentials dict. Not needed in Fabric notebooks.
        
    Returns:
        List of refresh history records.
    """
    workspace_id = _resolve_workspace_id(workspace, creds)
    model_id = model if _is_guid(model) else get_dataset_id_by_name(workspace_id, model, creds)
    
    url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{model_id}/refreshes?$top={top}"
    response = requests.get(url, headers=_powerbi_headers(creds))
    response.raise_for_status()
    
    return response.json().get("value", [])


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Fabric Deployment Library")
    print("=" * 60)
    print()
    print("In a Fabric notebook (no credentials needed):")
    print("-" * 60)
    print('    workspaces = list_workspaces()')
    print('    workspace_id = get_workspace_id_by_name("Sales Analytics")')
    print('    refresh_semantic_model("Sales Analytics", "Sales Model")')
    print()
    print("In a regular Python notebook (pass credentials):")
    print("-" * 60)
    print('    creds = {')
    print('        "tenant_id": "your-tenant-id",')
    print('        "client_id": "your-client-id",')
    print('        "client_secret": "your-client-secret"')
    print('    }')
    print('    workspaces = list_workspaces(creds)')
    print('    workspace_id = get_workspace_id_by_name("Sales Analytics", creds)')
    print('    refresh_semantic_model("Sales Analytics", "Sales Model", creds=creds)')
    print()