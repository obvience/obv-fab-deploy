import sempy.fabric as fabric
from sempy.fabric import FabricRestClient
import sempy_labs.lakehouse as lake
import sempy_labs.report as rep
import sempy_labs as labs
import requests
import time
import json
import base64
import pprint
from sempy_labs.directlake import update_direct_lake_model_lakehouse_connection


def get_workspace_id_by_name(workspace_name: str) -> str:
    workspaces = fabric.list_workspaces()
    match = workspaces[workspaces["Name"] == workspace_name]
    if match.empty:
        raise ValueError(f"Workspace '{workspace_name}' not found.")
    if len(match) > 1:
        raise ValueError(f"Multiple workspaces found with name '{workspace_name}', please use ID instead.")
    return match["Id"].iloc[0]


def get_lakehouse_id_by_name(workspace_name, lakehouse_name):
    workspace_id = get_workspace_id_by_name(workspace_name)
    items_df = fabric.list_items(workspace=workspace_id)
    lakehouse_row = items_df[(items_df['Type'] == 'Lakehouse') & (items_df['Display Name'] == lakehouse_name)]
    if lakehouse_row.empty:
        raise ValueError(f"Lakehouse '{lakehouse_name}' not found in workspace '{workspace_name}'.")
    return lakehouse_row['Id'].iloc[0]

def get_item_id_by_name(workspace_name, item_name, item_type):
    workspace_id = get_workspace_id_by_name(workspace_name)
    items_df = fabric.list_items(workspace=workspace_id)
    match = items_df[(items_df["Display Name"] == item_name) & (items_df["Type"] == item_type)]
    if match.empty:
        raise ValueError(f"{item_type} '{item_name}' not found in workspace '{workspace_name}'")
    return match["Id"].iloc[0]


def get_dataset_id_by_name(workspace_id, dataset_name):
    items_df = fabric.list_items(workspace=workspace_id)
    dataset_row = items_df[(items_df['Type'] == 'SemanticModel') & (items_df['Display Name'] == dataset_name)]
    if not dataset_row.empty:
        return dataset_row['Id'].iloc[0]
    else:
        raise ValueError(f"Semantic model '{dataset_name}' not found in workspace '{workspace_id}'.")

def rebind_report(
    report_name_or_id,
    dataset_name_or_id,
    report_workspace_id,
    dataset_workspace_id
):
    """
    Rebinds an existing report to a new semantic model (dataset).
    Accepts names or IDs for report and dataset.
    """
    # Lookup IDs if names are passed
    report_id = report_name_or_id
    if not report_id or len(report_id) != 36:  # crude check for GUID
        report_id = get_report_id_by_name(report_workspace_id, report_name_or_id)

    dataset_id = dataset_name_or_id
    if not dataset_id or len(dataset_id) != 36:
        dataset_id = get_dataset_id_by_name(dataset_workspace_id, dataset_name_or_id)

    try:
        rep.report_rebind(
            report=report_id,
            dataset=dataset_id,
            report_workspace=report_workspace_id,
            dataset_workspace=dataset_workspace_id
        )
        print(f"✅ Rebound report '{report_id}' to dataset '{dataset_id}' in workspace '{report_workspace_id}'.")
    except Exception as e:
        print(f"❌ Failed to rebind report '{report_id}': {e}")

def refresh_semantic_model(workspace_name: str, model_name: str):
    import sempy.fabric as fabric

    try:
        ws = fabric.list_workspaces()
        ws_id = ws[ws["Name"].str.lower() == workspace_name.lower()]["Id"].iloc[0]

        items = fabric.list_items(workspace=ws_id)
        model_id = items[
            (items["Display Name"] == model_name) & 
            (items["Type"] == "SemanticModel")
        ]["Id"].iloc[0]

        fabric.refresh_dataset(dataset=model_id, workspace=ws_id)
        print(f"✅ Refresh triggered for '{model_name}' in '{workspace_name}'")
    except Exception as e:
        print(f"❌ Refresh failed: {e}")

def get_report_id_by_name(workspace_id, report_name):
    items_df = fabric.list_items(workspace=workspace_id)
    report_row = items_df[(items_df['Type'] == 'Report') & (items_df['Display Name'] == report_name)]
    if not report_row.empty:
        return report_row['Id'].iloc[0]
    else:
        raise ValueError(f"Report '{report_name}' not found in workspace '{workspace_id}'.")