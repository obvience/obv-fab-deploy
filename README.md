# obv-fab-deploy

Unofficial Microsoft Fabric deployment toolkit built by [obviEnce](https://obvience.com).

Automates deployment of Lakehouses, Notebooks, Semantic Models, Reports, and Data Pipelines across Microsoft Fabric workspaces using direct REST API calls.

## Install

```bash
pip install obv-fab-deploy
```

## Authentication

**In a Fabric notebook** — no setup needed, uses `DefaultAzureCredential` automatically.

**From a regular Python environment** — pass a Service Principal credentials dict:

```python
creds = {
    "tenant_id": "your-tenant-id",
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
}
```

## Quick Start

```python
from obv_fab_deploy import (
    deploy_lakehouse_with_shortcuts,
    deploy_notebook,
    deploy_semantic_model,
    deploy_report,
    deploy_pipeline,
)

creds = {
    "tenant_id": "...",
    "client_id": "...",
    "client_secret": "...",
}

# Deploy a lakehouse with its shortcuts
deploy_lakehouse_with_shortcuts(
    source_workspace_name="DEV",
    source_lakehouse_name="my_lakehouse",
    target_workspace_name="PROD",
    target_lakehouse_name="my_lakehouse",
    creds=creds,
)

# Deploy a notebook (auto-rebinds to target lakehouse)
deploy_notebook(
    source_workspace_name="DEV",
    source_notebook_name="my_notebook",
    target_workspace_name="PROD",
    target_notebook_name="my_notebook",
    target_lakehouse_name="my_lakehouse",
    creds=creds,
)

# Deploy a semantic model (rebinds Direct Lake connection)
deploy_semantic_model(
    source_workspace_name="DEV",
    source_semantic_model_name="my_model",
    target_workspace_name="PROD",
    target_semantic_model_name="my_model",
    target_lakehouse_name="my_lakehouse",
    creds=creds,
)

# Deploy a report (rebinds to target semantic model)
deploy_report(
    source_workspace_name="DEV",
    source_report_name="my_report",
    target_workspace_name="PROD",
    target_report_name="my_report",
    target_dataset_name="my_model",
    creds=creds,
)

# Deploy a data pipeline (rebinds sinks + notebook references)
deploy_pipeline(
    source_workspace_name="DEV",
    pipeline_name="my_pipeline",
    target_workspace_name="PROD",
    target_lakehouse_name="my_lakehouse",
    creds=creds,
)
```

## Utility Functions

```python
from obv_fab_deploy import (
    list_workspaces,
    get_workspace_id_by_name,
    list_items,
    refresh_semantic_model,
    rebind_report,
)

# List all workspaces
workspaces = list_workspaces(creds)

# Get a workspace ID
ws_id = get_workspace_id_by_name("My Workspace", creds)

# List items in a workspace (optionally filter by type)
items = list_items("My Workspace", "SemanticModel", creds)

# Refresh a semantic model
refresh_semantic_model("My Workspace", "My Model", creds=creds)

# Rebind a report to a different semantic model
rebind_report("My Report", "My Model", "My Workspace", creds=creds)
```

## Features

- **Lakehouse** — Deploy lakehouses and replicate OneLake shortcuts
- **Notebook** — Deploy notebooks with automatic default lakehouse rebinding
- **Semantic Model** — Deploy with Direct Lake connection patching (TMDL)
- **Report** — Deploy reports with automatic dataset rebinding
- **Data Pipeline** — Deploy pipelines with sink and notebook activity rebinding
- **Auth** — Works with Service Principal (`ClientSecretCredential`) or environment auth (`DefaultAzureCredential`)
- **No sempy/mssparkutils required** — Uses direct Fabric REST API calls only

## Dependencies

- `requests`
- `azure-identity`

## Project Structure

```
obv_fab_deploy/
├── __init__.py          # Public API exports
├── utils.py             # Auth, workspace/item lookups, rebind, refresh
├── lakehouse.py         # Lakehouse deployment with shortcuts
├── notebook.py          # Notebook deployment
├── semantic_model.py    # Semantic model deployment with Direct Lake rebinding
├── report.py            # Report deployment with dataset rebinding
├── pipeline.py          # Pipeline deployment with activity rebinding
└── test_cases.py        # Manual test script
```

## License

MIT © [obviEnce](https://obvience.com)