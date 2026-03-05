"""
Test script for Fabric Deployment Library
==========================================
Credentials are loaded from a .env file or environment variables.
Copy .env.example to .env and fill in your values before running.
"""

import os
import sys

# Load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # .env vars must be set manually in the environment

from obv_fab_deploy.utils import (
    list_workspaces,
    get_workspace_id_by_name,
    list_items,
    get_dataset_id_by_name,
    refresh_semantic_model,
)
from obv_fab_deploy.lakehouse import deploy_lakehouse_with_shortcuts
from obv_fab_deploy.notebook import deploy_notebook
from obv_fab_deploy.semantic_model import deploy_semantic_model
from obv_fab_deploy.report import deploy_report
from obv_fab_deploy.pipeline import deploy_pipeline

# =============================================================================
# CREDENTIALS FROM ENVIRONMENT
# =============================================================================
creds = {
    "tenant_id": os.environ.get("FABRIC_TENANT_ID", ""),
    "client_id": os.environ.get("FABRIC_CLIENT_ID", ""),
    "client_secret": os.environ.get("FABRIC_CLIENT_SECRET", ""),
}

if not all(creds.values()):
    print("❌ Missing credentials. Set FABRIC_TENANT_ID, FABRIC_CLIENT_ID, and")
    print("   FABRIC_CLIENT_SECRET in your .env file or environment.")
    print("   See .env.example for the template.")
    sys.exit(1)

# =============================================================================
# TEST 1: List Workspacescle
# =============================================================================
print("=" * 60)
print("TEST 1: List Workspaces")
print("=" * 60)

try:
    workspaces = list_workspaces(creds)
    print(f"✅ Found {len(workspaces)} workspaces:\n")
    for ws in workspaces[:10]:  # Show first 10
        print(f"   - {ws['displayName']}")
        print(f"     ID: {ws['id']}")
        print()
    if len(workspaces) > 10:
        print(f"   ... and {len(workspaces) - 10} more")
except Exception as e:
    print(f"❌ Error: {e}")

# =============================================================================
# TEST 2: Get Workspace ID by Name (update the name below)
# =============================================================================
print("\n" + "=" * 60)
print("TEST 2: Get Workspace ID by Name")
print("=" * 60)

WORKSPACE_NAME = "PRYSMA_DEV"  # <-- Change this to a real workspace name

if WORKSPACE_NAME != "YOUR_WORKSPACE_NAME":
    try:
        workspace_id = get_workspace_id_by_name(WORKSPACE_NAME, creds)
        print(f"✅ Workspace '{WORKSPACE_NAME}' has ID: {workspace_id}")
    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("⏭️  Skipped - update WORKSPACE_NAME variable to test")

# =============================================================================
# TEST 3: List Items in Workspace
# =============================================================================
print("\n" + "=" * 60)
print("TEST 3: List Items in Workspace")
print("=" * 60)

if WORKSPACE_NAME != "YOUR_WORKSPACE_NAME":
    try:
        items = list_items(WORKSPACE_NAME, creds=creds)
        print(f"✅ Found {len(items)} items:\n")
        for item in items[:15]:  # Show first 15
            print(f"   - [{item['type']}] {item['displayName']}")
        if len(items) > 15:
            print(f"   ... and {len(items) - 15} more")
    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("⏭️  Skipped - update WORKSPACE_NAME variable to test")

# =============================================================================
# TEST 4: Get Semantic Model ID (update the name below)
# =============================================================================
print("\n" + "=" * 60)
print("TEST 4: Get Semantic Model ID")
print("=" * 60)

MODEL_NAME = "SM_PRYSMA"  # <-- Change this to a real semantic model name

if WORKSPACE_NAME != "YOUR_WORKSPACE_NAME" and MODEL_NAME != "YOUR_MODEL_NAME":
    try:
        model_id = get_dataset_id_by_name(WORKSPACE_NAME, MODEL_NAME, creds)
        print(f"✅ Model '{MODEL_NAME}' has ID: {model_id}")
    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("⏭️  Skipped - update WORKSPACE_NAME and MODEL_NAME variables to test")

# =============================================================================
# TEST 5: Refresh Semantic Model (OPTIONAL - uncomment to run)
# =============================================================================
print("\n" + "=" * 60)
print("TEST 5: Refresh Semantic Model")
print("=" * 60)

if WORKSPACE_NAME != "YOUR_WORKSPACE_NAME" and MODEL_NAME != "YOUR_MODEL_NAME":
    try:
        refresh_semantic_model(WORKSPACE_NAME, MODEL_NAME, creds=creds)
    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("⏭️  Skipped - update variables and uncomment the code to test")



# =============================================================================
# TEST 6: Deploy Lakehouse with Shortcuts
# =============================================================================
print("\n" + "=" * 60)
print("TEST 6: Deploy Lakehouse with Shortcuts")
print("=" * 60)

try:
    deploy_lakehouse_with_shortcuts(
        source_workspace_name="AI Lab",
        source_lakehouse_name="lh_ai_lab",
        target_workspace_name="AI Lab",
        target_lakehouse_name="lh_ai_lab_bkp",
        creds=creds
    )
except Exception as e:
    print(f"❌ Error: {e}")


# =============================================================================
# TEST 7: Deploy Notebook
# =============================================================================
print("\n" + "=" * 60)
print("TEST 7: Deploy Notebook")
print("=" * 60)

try:
    deploy_notebook(
        source_workspace_name="AI Lab",
        source_notebook_name="nb_obv_fab_deploy_test",
        target_workspace_name="AI Lab",
        target_notebook_name="nb_obv_fab_deploy_test_bkp",
        target_lakehouse_name="lh_ai_lab_bkp",
        creds=creds
    )
except Exception as e:
    print(f"❌ Error: {e}")


# =============================================================================
# TEST 8: Deploy Semantic Model
# =============================================================================
print("\n" + "=" * 60)
print("TEST 8: Deploy Semantic Model")
print("=" * 60)

try:
    deploy_semantic_model(
        source_workspace_name="AI Lab",
        source_semantic_model_name="sm_obv_fab_deploy_test",
        target_workspace_name="AI Lab",
        target_semantic_model_name="sm_obv_fab_deploy_test_bkp",
        target_lakehouse_name="lh_ai_lab_bkp",
        creds=creds
    )
except Exception as e:
    print(f"❌ Error: {e}")


# =============================================================================
# TEST 9: Deploy Report
# =============================================================================
print("\n" + "=" * 60)
print("TEST 9: Deploy Report")
print("=" * 60)

try:
    deploy_report(
        source_workspace_name="AI Lab",
        source_report_name="rpt_obv_fab_deploy_test",
        target_workspace_name="AI Lab",
        target_report_name="rpt_obv_fab_deploy_test_bkp",
        target_dataset_name="sm_obv_fab_deploy_test_bkp",
        creds=creds
    )
except Exception as e:
    print(f"❌ Error: {e}")


# =============================================================================
# TEST 10: Deploy Pipeline
# =============================================================================
print("\n" + "=" * 60)
print("TEST 10: Deploy Pipeline")
print("=" * 60)

try:
    deploy_pipeline(
        source_workspace_name="AI Lab",
        pipeline_name="pl_obv_fab_deploy_test",
        target_workspace_name="AI Lab",
        target_lakehouse_name="lh_ai_lab_bkp",
        target_pipeline_name="pl_obv_fab_deploy_test_bkp",
        creds=creds
    )
except Exception as e:
    print(f"❌ Error: {e}")


print("\n" + "=" * 60)
print("Testing Complete!")
print("=" * 60)