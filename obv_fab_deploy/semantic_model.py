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

def deploy_semantic_model(
    source_workspace_name,
    source_semantic_model_name,
    target_workspace_name,
    target_semantic_model_name,
    target_lakehouse_name
):
    """
    Deploys a semantic model from source workspace to target workspace,
    and rebinds it to the specified target lakehouse.
    """
    try:
        source_workspace_id = get_workspace_id_by_name(source_workspace_name)
        target_workspace_id = get_workspace_id_by_name(target_workspace_name)
    except Exception as e:
        print(f"❌ Workspace resolution failed: {e}")
        return

    # Deploy/copy semantic model
    try:
        labs.deploy_semantic_model(
            source_workspace=source_workspace_id,
            source_dataset=source_semantic_model_name,
            target_workspace=target_workspace_id,
            target_dataset=target_semantic_model_name,
            refresh_target_dataset=False,
            overwrite=True
        )
        print(f"✅ Deployed semantic model '{target_semantic_model_name}' to target workspace.")
    except ValueError as e:
        print(f"❌ Cannot deploy model '{source_semantic_model_name}': {e}")
        return
    except Exception as e:
        print(f"❌ Unexpected error during deployment: {e}")
        return

    # Rebind to lakehouse
    try:
        update_direct_lake_model_lakehouse_connection(
            dataset=target_semantic_model_name,
            workspace=target_workspace_id,
            lakehouse=target_lakehouse_name
        )
        print(f"✅ Rebound '{target_semantic_model_name}' to lakehouse '{target_lakehouse_name}'.")
    except Exception as e:
        print(f"⚠️ Rebinding failed: {e}")
