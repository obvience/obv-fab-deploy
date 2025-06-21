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

def deploy_pipeline(source_workspace_name, pipeline_name, target_workspace_name, target_lakehouse_name):
    try:
        src_ws = get_workspace_id_by_name(source_workspace_name)
        tgt_ws = get_workspace_id_by_name(target_workspace_name)
        tgt_lh = get_lakehouse_id_by_name(target_workspace_name, target_lakehouse_name)
    except Exception as e:
        print(f"❌ Could not resolve IDs for input parameters: {e}")
        return

    try:
        df = labs.list_data_pipelines(src_ws)
        row = df[df["Data Pipeline Name"] == pipeline_name]
        if row.empty:
            print(f"❌ Pipeline '{pipeline_name}' not found in '{source_workspace_name}'")
            return

        source_def = labs.get_data_pipeline_definition(name=pipeline_name, workspace=src_ws, decode=False)
        row = source_def[source_def["path"] == "pipeline-content.json"]
        if not row.empty:
            source_pipeline_payload = row.iloc[0].to_dict()
        else:
            print("❌ pipeline-content.json not found")
            return

        # Decode for optional inspection (no modification here)
        source_pipeline_dict = json.loads(base64.b64decode(source_pipeline_payload["payload"]).decode("utf-8"))
        #print(json.dumps(source_pipeline_dict, indent=2))  # For debugging

        # Rebind Sink datasets from source to target workspace and lakehouse
        for activity in source_pipeline_dict.get("properties", {}).get("activities", []):
            if activity.get("type") == "Copy":
                sink = activity.get("typeProperties", {}).get("sink", {})
                if sink.get("type") == "LakehouseTableSink":
                    type_props = (
                        sink.get("datasetSettings", {})
                            .get("linkedService", {})
                            .get("properties", {})
                            .get("typeProperties", {})
                    )
                    type_props["workspaceId"] = str(tgt_ws)
                    type_props["artifactId"] = str(tgt_lh)

        # Rebind Notebooks from soruce wourkspace to corresponding notebooks in target workspace
        # Get all items, filter for notebooks (type == "notebook")
        df_src_items = fabric.list_items(workspace=src_ws)
        df_tgt_items = fabric.list_items(workspace=tgt_ws)

        # Filter to notebooks only
        df_src_notebooks = df_src_items[df_src_items["Type"] == "Notebook"]
        df_tgt_notebooks = df_tgt_items[df_tgt_items["Type"] == "Notebook"]

        # Map target notebook names to their IDs
        tgt_notebook_name_to_id = dict(zip(df_tgt_notebooks["Display Name"], df_tgt_notebooks["Id"]))

        for activity in source_pipeline_dict.get("properties", {}).get("activities", []):
            if activity.get("type") == "TridentNotebook":
                nb_id = activity.get("typeProperties", {}).get("notebookId")
                nb_row = df_src_notebooks[df_src_notebooks["Id"] == nb_id]
                if not nb_row.empty:
                    nb_name = nb_row.iloc[0]["Display Name"]
                    tgt_nb_id = tgt_notebook_name_to_id.get(nb_name)
                    print(f"Checking activity: {activity['name']} (src_nb_id={nb_id}, name={nb_name}, tgt_nb_id={tgt_nb_id})")
                    if tgt_nb_id:
                        print(f"Rebinding notebook '{nb_name}': {nb_id} -> {tgt_nb_id} | ws: {activity['typeProperties']['workspaceId']} -> {str(tgt_ws)}")
                        activity["typeProperties"]["notebookId"] = tgt_nb_id
                        activity["typeProperties"]["workspaceId"] = str(tgt_ws)
                    else:
                        print(f"Target notebook with name '{nb_name}' not found in target workspace.")
                else:
                    print(f"No notebook found in source notebooks with id '{nb_id}'")


    except Exception as e:
        print(f"❌ Could not fetch pipeline definition: {e}")
        return

    try:
        df_target = labs.list_data_pipelines(tgt_ws)
        match = df_target[df_target["Data Pipeline Name"] == pipeline_name]
        if match.empty:
            print(f"ℹ️ Creating pipeline '{pipeline_name}' in target...")
            labs.create_data_pipeline(name=pipeline_name, workspace=tgt_ws)
            df_target = labs.list_data_pipelines(tgt_ws)
            match = df_target[df_target["Data Pipeline Name"] == pipeline_name]

        target_pipeline_id = match["Data Pipeline ID"].iloc[0]

        target_def = labs.get_data_pipeline_definition(name=pipeline_name, workspace=tgt_ws, decode=False)
        target_parts = target_def.to_dict(orient="records")

        # Overwrite ONLY in target_parts
        for i, part in enumerate(target_parts):
            if part["path"] == "pipeline-content.json":
                # Fresh encode from source pipeline dict (not source var)
                new_payload = base64.b64encode(json.dumps(source_pipeline_dict).encode("utf-8")).decode("utf-8")
                target_parts[i]["payload"] = new_payload
                break

        payload = {"definition": {"parts": target_parts}}

        client = fabric.FabricRestClient()
        resp = client.post(
            f"/v1/workspaces/{tgt_ws}/dataPipelines/{target_pipeline_id}/updateDefinition?updateMetadata=true",
            json=payload
        )

        if resp.status_code == 202:
            op_url = resp.headers.get("Location")
            for _ in range(10):
                time.sleep(5)
                status = client.get(op_url).json()
                if status.get("status") == "Succeeded":
                    print(f"✅ Pipeline '{pipeline_name}' updated via LRO.")
                    return
                elif status.get("status") == "Failed":
                    print(f"❌ LRO failed: {status}")
                    return
                print("⏳ Waiting for LRO...")
            print("❌ Timed out waiting for pipeline update.")
        elif resp.status_code in (200, 201):
            print(f"✅ Pipeline '{pipeline_name}' updated.")
        else:
            print(f"❌ Deployment failed ({resp.status_code}): {resp.text}")

    except Exception as e:
        print(f"❌ Deployment error: {e}")
