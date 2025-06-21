# obv-fab-deploy

Unofficial Fabric deployment toolkit built by obviEnce.
Automates deployment of Lakehouses, Pipelines, Notebooks, Reports, and Semantic Models in Microsoft Fabric.

## Install

```bash
pip install obv-fab-deploy
```

## Usage

from obv_fab_deploy import deploy_pipeline

deploy_pipeline(
    source_workspace_name="MyWorkspaceDEV",
    pipeline_name="MyPipeline",
    target_workspace_name="MyWorkspacePROD",
    target_lakehouse_name="MyLakehouse"
)

## Features
Deploy and rebind Data Pipelines between Fabric workspaces
Clone and rebind Power BI Reports and Semantic Models
Deploy and auto-bind Notebooks to the correct lakehouse
Sync Lakehouse Shortcuts from source to target
Compatible with sempy_labs and FabricRestClient
Designed for scripting, automation, and CI/CD workflows

## Folder Structure
obv_fab_deploy/
├── obv_fab_deploy/
│   └── deploy.py
├── pyproject.toml
├── README.md
├── .gitignore


## License
MIT © obviEnce