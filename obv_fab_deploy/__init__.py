"""
obv-fab-deploy
==============
Unofficial Microsoft Fabric deployment toolkit by Obvience.

Automates deployment of Lakehouses, Notebooks, Semantic Models,
Reports, and Data Pipelines across Microsoft Fabric workspaces.

Uses direct Fabric REST API calls with azure-identity for authentication.
Works in Fabric notebooks (no setup needed) or from any Python environment
with a Service Principal.
"""

__version__ = "0.2.0"

# --- Core utilities --------------------------------------------------------
from .utils import (
    list_workspaces,
    get_workspace_id_by_name,
    list_items,
    get_item_id_by_name,
    get_lakehouse_id_by_name,
    get_dataset_id_by_name,
    get_report_id_by_name,
    rebind_report,
    refresh_semantic_model,
    get_refresh_history,
)

# --- Deployment functions --------------------------------------------------
from .lakehouse import deploy_lakehouse_with_shortcuts
from .notebook import deploy_notebook, delete_notebook
from .semantic_model import deploy_semantic_model
from .report import deploy_report
from .pipeline import deploy_pipeline

__all__ = [
    # utils
    "list_workspaces",
    "get_workspace_id_by_name",
    "list_items",
    "get_item_id_by_name",
    "get_lakehouse_id_by_name",
    "get_dataset_id_by_name",
    "get_report_id_by_name",
    "rebind_report",
    "refresh_semantic_model",
    "get_refresh_history",
    # deploy
    "deploy_lakehouse_with_shortcuts",
    "deploy_notebook",
    "delete_notebook",
    "deploy_semantic_model",
    "deploy_report",
    "deploy_pipeline",
]
