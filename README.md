# Fabric Agents

AI agents for Microsoft Fabric that automate data cleaning, semantic model creation, and synthetic data generation. Built as [VS Code Agent Skills](https://agentskills.io/) for use with GitHub Copilot in Agent mode.

## Agents

### Fabric Data Cleaner

Discovers tables in Fabric lakehouses, analyzes data quality, and generates self-contained PySpark notebooks that clean the data. Deploys notebooks to Fabric, runs them in sequence, and writes results to a `_cleaned` table.

**Cleaning algorithms**: profiling, duplicate detection, null analysis, type validation (Spanish comma decimals), statistical summary, IQR outlier detection, date format validation, Spanish DNI/NIE checksum validation, email/phone format checks.

**Prompt**: `/clean-table orders in my_lakehouse`

### Fabric Semantic Model Creator

Analyzes lakehouse table schemas and sample data to infer a star-schema Power BI semantic model. Classifies tables as fact/dimension, detects relationships via FK/PK matching, generates DAX measures, and produces TMDL files deployable to Fabric.

**Prompt**: `/create-semantic-model all tables in my_lakehouse, My Workspace`

### Fabric Synthetic Data Generator

Designs realistic data schemas from domain templates (retail, healthcare, airlines, custom), generates synthetic data with referential integrity using `faker`, and uploads Parquet files to Fabric lakehouse tables.

**Prompt**: `/generate-synthetic-data retail data for my_lakehouse`

## Prerequisites

- [VS Code](https://code.visualstudio.com/) with [GitHub Copilot](https://marketplace.visualstudio.com/items?itemName=GitHub.copilot) extension
- [Fabric MCP Server](https://marketplace.visualstudio.com/items?itemName=fabric.vscode-fabric-mcp-server) VS Code extension for workspace/table discovery
- Azure CLI logged in (`az login`) for Fabric REST API access
- Python 3.10+

## Setup

1. Clone this repository
2. Open in VS Code
3. Install the Fabric MCP Server extension
4. Log in to Azure CLI: `az login`
5. The agents create a `.venv` automatically on first run

## Repository Structure

```
.github/
├── agents/
│   ├── fabric-data-cleaner.agent.md        # Data cleaning agent
│   ├── fabric-semantic-model.agent.md      # Semantic model agent
│   └── fabric-synthetic-data.agent.md      # Synthetic data agent
├── prompts/
│   ├── clean-table.prompt.md               # /clean-table entry point
│   ├── create-semantic-model.prompt.md     # /create-semantic-model entry point
│   └── generate-synthetic-data.prompt.md   # /generate-synthetic-data entry point
└── skills/
    ├── fabric-data-cleaner/
    │   ├── SKILL.md                        # Algorithm reference + gotchas
    │   ├── notebooks/                      # 9 self-contained PySpark notebook templates
    │   ├── references/                     # DNI validation rules
    │   └── scripts/
    │       ├── fabric_notebook.py          # Deploy/run/delete notebooks via REST API
    │       └── validate_notebooks.py       # Validate generated notebooks before deploy
    ├── fabric-semantic-model/
    │   ├── SKILL.md                        # TMDL templates + classification rules
    │   └── scripts/
    │       └── fabric_semantic_model.py    # Deploy/list/delete models + SQL endpoint + table schemas
    └── fabric-synthetic-data/
        ├── SKILL.md                        # Domain templates + generation rules
        └── scripts/
            └── fabric_synthetic_data.py    # Upload Parquet + load as Delta tables
```

## How It Works

Each agent follows a phased workflow with clear human-in-the-loop checkpoints:

1. **Discover** — Uses the Fabric MCP Server to find workspaces, lakehouses, and tables
2. **Analyze** — Classifies data and presents findings for user confirmation
3. **Generate** — Creates artifacts locally (notebooks, TMDL files, or Parquet files)
4. **Deploy** — Uploads to Fabric via REST API scripts (or saves locally)
5. **Verify** — Confirms deployment success
6. **Cleanup** — Removes temporary artifacts from Fabric and local workspace

All agents use Python scripts (not raw API calls) for Fabric operations. Scripts authenticate via `DefaultAzureCredential` which picks up your `az login` session.

## Generated Output Directories

These are created at runtime and gitignored:

| Directory | Agent | Contents |
|-----------|-------|----------|
| `cleaning_runs/` | Data Cleaner | Customized PySpark notebooks per table/run |
| `semantic_models/` | Semantic Model | TMDL definition files per model/run |
| `synthetic_data/` | Synthetic Data | Parquet files per generation run |

Each uses the pattern `{directory}/{lakehouse}/{name}/{YYYY-MM-DD_HHmmss}/` for multiple runs.

## Spanish Locale Support

All agents are configured for Spanish data:
- Date format: `dd/MM/yyyy`
- Decimal separator: `,` (comma)
- DNI/NIE validation with mod-23 checksum
- Phone numbers: `+34 6XX XXX XXX`
- `faker` locale: `es_ES`
- Semantic model culture: `es-ES`
