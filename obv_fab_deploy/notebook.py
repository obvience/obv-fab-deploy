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

def delete_notebook(workspace_name: str, notebook_name: str):
    """
    Delete a Fabric notebook by name from a workspace using the REST API.

    Args:
        workspace_name (str): The Fabric workspace name.
        notebook_name (str): The notebook display name.
    """
    workspace_id = get_workspace_id_by_name(workspace_name)
    items_df = fabric.list_items(workspace=workspace_id)
    match = items_df[(items_df["Type"] == "Notebook") & (items_df["Display Name"] == notebook_name)]
    if match.empty:
        print(f"❌ Notebook '{notebook_name}' not found in workspace '{workspace_name}'.")
        return
    if len(match) > 1:
        print(f"❌ Multiple notebooks named '{notebook_name}' found. Cannot safely delete.")
        return

    notebook_id = match["Id"].iloc[0]
    access_token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com/")
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{notebook_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.delete(url, headers=headers)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print(f"✅ Deleted notebook '{notebook_name}' from workspace '{workspace_name}'.")
    elif response.status_code == 404:
        print(f"❌ Notebook not found: '{notebook_name}' in workspace '{workspace_name}'.")
    else:
        print(f"❌ Failed to delete notebook: {response.status_code} {response.text}")

def deploy_notebook(
    source_workspace_name,
    source_notebook_name,
    target_workspace_name,
    target_notebook_name,
    target_lakehouse_name
):
    import time, base64, json

    source_ws = get_workspace_id_by_name(source_workspace_name)
    target_ws = get_workspace_id_by_name(target_workspace_name)
    client = fabric.FabricRestClient()

    try:
        lakehouse_id = get_lakehouse_id_by_name(target_workspace_name, target_lakehouse_name)
    except Exception as e:
        print(f"❌ Lakehouse lookup failed: {e}")
        return None

    try:
        nb_raw = labs.get_notebook_definition(
            notebook_name=source_notebook_name,
            workspace=source_ws,
            decode=True,
            format="ipynb"
        )
        nb_obj = json.loads(nb_raw)
        nb_obj["metadata"]["dependencies"]["lakehouse"].update({
            "known_lakehouses": [{"id": lakehouse_id}],
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": target_lakehouse_name,
            "default_lakehouse_workspace_id": target_ws
        })
        content_b64 = base64.b64encode(json.dumps(nb_obj).encode()).decode()
    except Exception as e:
        print(f"⚠️ Could not patch notebook '{source_notebook_name}': {e}")
        return None

    items = fabric.list_items(workspace=target_ws)
    row = items[(items.Type == "Notebook") & (items["Display Name"] == target_notebook_name)]

    definition = {
        "format": "ipynb",
        "parts": [{
            "path": "artifact.content.ipynb",
            "payload": content_b64,
            "payloadType": "InlineBase64"
        }]
    }

    def poll_lro(location, label):
        for _ in range(15):
            time.sleep(5)
            status = client.get(location).json()
            if status.get("status") == "Succeeded":
                return client.get(location + "/result").json()["id"]
            elif status.get("status") == "Failed":
                print(f"❌ {label} failed: {status}")
                return None
            else:
                print(f"⏳ {label} running...")
        print(f"❌ Timed out waiting for {label}")
        return None

    if row.empty:
        resp = client.post(
            f"/v1/workspaces/{target_ws}/items",
            json={
                "displayName": target_notebook_name,
                "type": "Notebook",
                "definition": definition
            }
        )
        if resp.status_code == 202:
            nb_id = poll_lro(resp.headers["Location"], "Notebook creation")
        elif resp.status_code in (200, 201):
            nb_id = resp.json().get("id")
        else:
            print(f"❌ Create failed: {resp.status_code} {resp.text}")
            return None
        print(f"✅ Created notebook '{target_notebook_name}'")
    else:
        nb_id = row["Id"].iloc[0]

        # get .platform from source definition
        source_nb_id = get_item_id_by_name(source_workspace_name, source_notebook_name, "Notebook")

        resp_def = client.post(
            f"/v1/workspaces/{source_ws}/notebooks/{source_nb_id}/getDefinition?format=ipynb"
        )

        if resp_def.status_code == 202:
            op_url = resp_def.headers["Location"]
            for _ in range(10):
                time.sleep(5)
                poll = client.get(op_url).json()
                if poll.get("status") == "Succeeded":
                    definition_raw = client.get(op_url + "/result").json()["definition"]
                    break
                elif poll.get("status") == "Failed":
                    print("❌ getDefinition LRO failed")
                    return None
            else:
                print("❌ Timed out waiting for .platform definition")
                return None
        elif resp_def.status_code == 200:
            definition_raw = resp_def.json()["definition"]
        else:
            print(f"❌ getDefinition failed: {resp_def.status_code} {resp_def.text}")
            return None


        platform_payload = next(p["payload"] for p in definition_raw["parts"] if p["path"] == ".platform")

        # build updated definition
        definition = {
            "format": "ipynb",
            "parts": [
                {
                    "path": "artifact.content.ipynb",
                    "payload": content_b64,
                    "payloadType": "InlineBase64"
                },
                {
                    "path": ".platform",
                    "payload": platform_payload,
                    "payloadType": "InlineBase64"
                }
            ]
        }

        # send update
        resp = client.post(
            f"/v1/workspaces/{target_ws}/notebooks/{nb_id}/updateDefinition?updateMetadata=true",
            json={"definition": definition}
        )
        if resp.status_code == 202:
            poll_lro(resp.headers["Location"], "Notebook update")
        elif resp.status_code != 200:
            print(f"❌ Update failed: {resp.status_code} {resp.text}")
            return None
        print(f"✅ Updated notebook '{target_notebook_name}'")


    print(f"✅ Finished '{source_notebook_name}' → '{target_notebook_name}'")
    return nb_id
