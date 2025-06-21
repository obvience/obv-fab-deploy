
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

from .utils import get_workspace_id_by_name, get_lakehouse_id_by_name

def deploy_lakehouse_with_shortcuts(
    source_workspace_name, source_lakehouse_name,
    target_workspace_name, target_lakehouse_name
):
    source_workspace_id = get_workspace_id_by_name(source_workspace_name)
    target_workspace_id = get_workspace_id_by_name(target_workspace_name)

    # Create target lakehouse if needed
    items_df = fabric.list_items(workspace=target_workspace_id)
    lakehouses = items_df[items_df['Type'] == 'Lakehouse']['Display Name'].tolist()
    if target_lakehouse_name not in lakehouses:
        print(f"🔨 Creating lakehouse '{target_lakehouse_name}' in workspace '{target_workspace_name}'")
        client = fabric.FabricRestClient()
        payload = {
            "displayName": target_lakehouse_name,
            "description": "A schema enabled lakehouse.",
            "creationPayload": {"enableSchemas": True}
        }
        response = client.post(
            f"/v1/workspaces/{target_workspace_id}/lakehouses",
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
    source_lakehouse_id = get_lakehouse_id_by_name(source_workspace_name, source_lakehouse_name)
    target_lakehouse_id = get_lakehouse_id_by_name(target_workspace_name, target_lakehouse_name)

    # Migrate shortcuts
    existing_shortcuts_df = lake.list_shortcuts(
        lakehouse=target_lakehouse_id,
        workspace=target_workspace_id
    )
    existing_shortcut_names = set(existing_shortcuts_df['Shortcut Name'].tolist())

    shortcuts_df = lake.list_shortcuts(
        lakehouse=source_lakehouse_id,
        workspace=source_workspace_id
    )

    for _, row in shortcuts_df.iterrows():
        shortcut_name = row['Shortcut Name']
        shortcut_path = row['Shortcut Path'].lstrip('/')
        if shortcut_name in existing_shortcut_names:
            print(f"⚠️ Shortcut '{shortcut_name}' already exists in target. Skipping.")
            continue
        print(f"Creating shortcut '{shortcut_name}' at path '{shortcut_path}'")
        lake.create_shortcut_onelake(
            table_name=shortcut_name,
            source_lakehouse=source_lakehouse_id,
            source_workspace=source_workspace_id,
            destination_lakehouse=target_lakehouse_id,
            destination_workspace=target_workspace_id,
            shortcut_name=shortcut_name,
            source_path=shortcut_path,
            destination_path=shortcut_path,
            shortcut_conflict_policy="Abort"
        )
        print(f"✅ Created shortcut '{shortcut_name}' at path '{shortcut_path}'")
