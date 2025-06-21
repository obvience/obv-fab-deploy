
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

from .utils import get_workspace_id_by_name, get_lakehouse_id_by_name, get_report_id_by_name

def deploy_report(
    source_workspace_name,
    source_report_name,
    target_workspace_name,
    target_report_name,
    target_dataset_name
):
    source_workspace_id = get_workspace_id_by_name(source_workspace_name)
    target_workspace_id = get_workspace_id_by_name(target_workspace_name)
    client = fabric.FabricRestClient()

    try:
        target_report_id = get_report_id_by_name(target_workspace_name, target_report_name)
    except Exception:
        target_report_id = None

    if target_report_id:
        print(f"🛠️ Report '{target_report_name}' exists. Updating definition...")

        df = rep.get_report_definition(
            workspace=source_workspace_id,
            report=source_report_name
        )

        definition = {
            "parts": df.to_dict(orient="records")
        }

        response = client.post(
            f"/v1/workspaces/{target_workspace_id}/reports/{target_report_id}/updateDefinition",
            json={"definition": definition}
        )

        if response.status_code in (200, 202):
            print(f"✅ Report '{target_report_name}' updated.")
            # Always rebind to target dataset
            try:
                rep.report_rebind(
                    report=target_report_name,
                    report_workspace=target_workspace_id,
                    dataset=target_dataset_name,
                    dataset_workspace=target_workspace_id
                )
                print(f"🔗 Rebound to dataset '{target_dataset_name}'")
            except Exception as e:
                print(f"⚠️ Rebind failed: {e}")
        else:
            print(f"⚠️ Update failed: {response.status_code} {response.text}")
    else:
        print(f"📄 Report '{target_report_name}' not found. Cloning...")
        try:
            rep.clone_report(
                report=source_report_name,
                cloned_report=target_report_name,
                workspace=source_workspace_id,
                target_workspace=target_workspace_id,
                target_dataset=target_dataset_name,
                target_dataset_workspace=target_workspace_id
            )
            print(f"✅ Cloned '{target_report_name}' → model '{target_dataset_name}'")
            # Optionally rebind again if needed, but clone_report usually does it.
        except Exception as e:
            print(f"⚠️ Clone/rebind failed: {e}")
