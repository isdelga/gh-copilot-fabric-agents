"""
Fabric Synthetic Data Manager — upload Parquet files and load as Delta tables via Fabric REST API.

Authentication: Uses Azure CLI credentials (az login) via DefaultAzureCredential.

Usage:
    python fabric_synthetic_data.py upload             <workspace_id> <lakehouse_id> <local_path> <remote_filename>
    python fabric_synthetic_data.py load-table         <workspace_id> <lakehouse_id> <table_name> <relative_path>
    python fabric_synthetic_data.py load-via-notebook   <workspace_id> <lakehouse_id> <parquet_dir> <output_notebook>
    python fabric_synthetic_data.py deploy-notebook    <workspace_id> <lakehouse_id> <notebook_name> <ipynb_path>
    python fabric_synthetic_data.py run-notebook       <workspace_id> <notebook_id>
    python fabric_synthetic_data.py status-notebook    <workspace_id> <notebook_id> <job_instance_id>
    python fabric_synthetic_data.py delete-notebook    <workspace_id> <notebook_id>
    python fabric_synthetic_data.py list-tables        <workspace_id> <lakehouse_id>
    python fabric_synthetic_data.py delete-table       <workspace_id> <lakehouse_id> <table_name>

Upload flow (standard):     upload Parquet → load-table (Load Table API)
Upload flow (schemas-enabled): upload Parquet → load-via-notebook → deploy-notebook → run-notebook

When a lakehouse has schemas enabled, the Load Table API fails. Use load-via-notebook
to generate a PySpark notebook that reads the uploaded Parquet files and writes them
as Delta tables. Then deploy and run the notebook in Fabric.
"""

import sys
import json
import time
import requests
from azure.identity import DefaultAzureCredential

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


def get_headers(content_type="application/json"):
    """Get auth headers using Azure CLI credentials."""
    credential = DefaultAzureCredential()
    token = credential.get_token(FABRIC_SCOPE)
    return {
        "Authorization": f"Bearer {token.token}",
        "Content-Type": content_type,
    }


def get_onelake_headers():
    """Get auth headers for OneLake DFS operations."""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return {
        "Authorization": f"Bearer {token.token}",
    }


def upload_file(workspace_id: str, lakehouse_id: str, local_path: str, remote_filename: str):
    """Upload a local file to OneLake Files/synthetic_data/ via DFS API."""
    remote_path = f"Files/synthetic_data/{remote_filename}"

    with open(local_path, "rb") as f:
        data = f.read()

    headers = get_onelake_headers()

    # Step 1: Create file (PUT with ?resource=file)
    create_url = f"{ONELAKE_DFS}/{workspace_id}/{lakehouse_id}/{remote_path}?resource=file"
    resp = requests.put(create_url, headers=headers, timeout=60)
    if resp.status_code not in (200, 201):
        print(f"Error creating file: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    # Step 2: Append data (PATCH with ?action=append&position=0)
    append_url = f"{ONELAKE_DFS}/{workspace_id}/{lakehouse_id}/{remote_path}?action=append&position=0"
    resp = requests.patch(append_url, headers=headers, data=data, timeout=120)
    if resp.status_code not in (200, 202):
        print(f"Error appending data: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    # Step 3: Flush (PATCH with ?action=flush&position=len)
    flush_url = f"{ONELAKE_DFS}/{workspace_id}/{lakehouse_id}/{remote_path}?action=flush&position={len(data)}"
    resp = requests.patch(flush_url, headers=headers, timeout=60)
    if resp.status_code not in (200,):
        print(f"Error flushing: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    print(f"Uploaded {local_path} → {remote_path} ({len(data):,} bytes)")
    return remote_path


def load_table(workspace_id: str, lakehouse_id: str, table_name: str, relative_path: str):
    """Load a file from Files/ as a Delta table via the Load Table API."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables/{table_name}/load"

    payload = {
        "relativePath": relative_path,
        "pathType": "File",
        "mode": "Overwrite",
        "formatOptions": {
            "format": "Parquet",
        },
    }

    resp = requests.post(url, headers=get_headers(), json=payload, timeout=60)

    if resp.status_code == 202:
        operation_url = resp.headers.get("Location")
        print(f"Loading table '{table_name}' from {relative_path}...")
        result = _poll_long_running_operation(operation_url)
        if result is not None:
            print(f"Table '{table_name}' loaded successfully")
        return result
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def list_tables(workspace_id: str, lakehouse_id: str):
    """List all tables in the lakehouse."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
    resp = requests.get(url, headers=get_headers(), timeout=30)

    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)

    tables = resp.json().get("data", [])
    if not tables:
        print("No tables found.")
    for t in tables:
        print(f"  {t['name']} ({t.get('format', 'unknown')})")
    return tables


def delete_table(workspace_id: str, lakehouse_id: str, table_name: str):
    """Delete a table from the lakehouse."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables/{table_name}"
    resp = requests.delete(url, headers=get_headers(), timeout=30)

    if resp.status_code == 200:
        print(f"Deleted table '{table_name}'")
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def _poll_long_running_operation(operation_url: str, max_wait: int = 300):
    """Poll a long-running operation until completion."""
    headers = get_headers()
    elapsed = 0
    while elapsed < max_wait:
        resp = requests.get(operation_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"Poll error {resp.status_code}: {resp.text}", file=sys.stderr)
            return None

        data = resp.json()
        status = data.get("status", "Unknown")
        print(f"  Status: {status} ({elapsed}s elapsed)")

        if status in ("Succeeded", "Completed"):
            return data
        elif status in ("Failed", "Cancelled"):
            error = data.get("error", {})
            print(f"  Failed: {error.get('message', 'Unknown error')}", file=sys.stderr)
            return None

        retry_after = int(resp.headers.get("Retry-After", 10))
        time.sleep(retry_after)
        elapsed += retry_after

    print(f"Timed out after {max_wait}s", file=sys.stderr)
    return None


def generate_load_notebook(workspace_id: str, lakehouse_id: str, parquet_dir: str, output_path: str):
    """Generate a PySpark notebook that loads Parquet files from Files/synthetic_data/ as Delta tables.

    Use this when the lakehouse has schemas enabled and the Load Table API fails.
    The notebook reads each .parquet file uploaded to Files/synthetic_data/ and writes
    it as a Delta table using spark.write.
    """
    import glob
    import os

    parquet_files = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
    if not parquet_files:
        print(f"No .parquet files found in {parquet_dir}", file=sys.stderr)
        sys.exit(1)

    table_names = [os.path.splitext(os.path.basename(f))[0] for f in parquet_files]

    # Build notebook cells
    cells = []

    # Markdown header
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "# Load Synthetic Data as Delta Tables\n",
            "This notebook reads Parquet files from `Files/synthetic_data/` and writes them as Delta tables.\n",
            "\n",
            "**Auto-generated — run all cells.**"
        ]
    })

    # Code cell: load all tables
    code_lines = [
        "from pyspark.sql import SparkSession\n",
        "from pyspark.sql.types import TimestampNTZType, TimestampType, DateType\n",
        "from pyspark.sql import functions as F\n",
        "\n",
        "spark = SparkSession.builder.getOrCreate()\n",
        "\n",
        "def fix_timestamp_ntz(df):\n",
        "    \"\"\"Cast TIMESTAMP_NTZ columns to DATE — Fabric Delta doesn't support TIMESTAMP_NTZ.\"\"\"\n",
        "    for field in df.schema.fields:\n",
        "        if isinstance(field.dataType, TimestampNTZType):\n",
        "            df = df.withColumn(field.name, F.col(field.name).cast(DateType()))\n",
        "    return df\n",
        "\n",
        "tables = [\n",
    ]
    for name in table_names:
        code_lines.append(f'    ("{name}", "Files/synthetic_data/{name}.parquet"),\n')
    code_lines.append("]\n")
    code_lines.append("\n")
    code_lines.append("for table_name, parquet_path in tables:\n")
    code_lines.append("    print(f\"Loading {table_name}...\")\n")
    code_lines.append("    df = spark.read.parquet(parquet_path)\n")
    code_lines.append("    df = fix_timestamp_ntz(df)\n")
    code_lines.append("    df.write.mode(\"overwrite\").format(\"delta\").saveAsTable(table_name)\n")
    code_lines.append("    print(f\"  {table_name}: {df.count():,} rows written\")\n")
    code_lines.append("\n")
    code_lines.append("print(\"\\nAll tables loaded successfully.\")\n")

    cells.append({
        "cell_type": "code",
        "metadata": {},
        "outputs": [],
        "source": code_lines,
        "execution_count": None,
    })

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": cells,
        "metadata": {
            "kernel_info": {"name": "synapse_pyspark"},
            "language_info": {"name": "python"},
            "dependencies": {
                "lakehouse": {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": "",
                    "default_lakehouse_workspace_id": workspace_id,
                    "known_lakehouses": [{"id": lakehouse_id}],
                }
            },
        },
    }

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1)

    print(f"Generated load notebook: {output_path}")
    print(f"  Tables: {', '.join(table_names)}")
    return output_path


def deploy_notebook(workspace_id: str, lakehouse_id: str, notebook_name: str, ipynb_path: str):
    """Deploy a notebook to Fabric with lakehouse binding."""
    import base64

    with open(ipynb_path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    # Ensure lakehouse metadata
    nb.setdefault("metadata", {})
    nb["metadata"]["kernel_info"] = {"name": "synapse_pyspark"}
    nb["metadata"]["dependencies"] = {
        "lakehouse": {
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": "",
            "default_lakehouse_workspace_id": workspace_id,
            "known_lakehouses": [{"id": lakehouse_id}],
        }
    }

    encoded_notebook = base64.b64encode(json.dumps(nb).encode("utf-8")).decode("utf-8")

    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": notebook_name},
        "config": {"version": "2.0", "logicalId": ""},
    }
    encoded_platform = base64.b64encode(json.dumps(platform).encode("utf-8")).decode("utf-8")

    payload = {
        "displayName": notebook_name,
        "description": f"Auto-generated load notebook: {notebook_name}",
        "definition": {
            "format": "ipynb",
            "parts": [
                {"path": "notebook-content.ipynb", "payload": encoded_notebook, "payloadType": "InlineBase64"},
                {"path": ".platform", "payload": encoded_platform, "payloadType": "InlineBase64"},
            ],
        },
    }

    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks"
    resp = requests.post(url, headers=get_headers(), json=payload, timeout=60)

    if resp.status_code == 201:
        notebook_item = resp.json()
        print(f"Created notebook: {notebook_item['displayName']} (id: {notebook_item['id']})")
        return notebook_item["id"]
    elif resp.status_code == 202:
        operation_url = resp.headers.get("Location")
        print(f"Notebook creation in progress...")
        return _poll_long_running_operation(operation_url)
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def run_notebook(workspace_id: str, notebook_id: str):
    """Trigger an on-demand run of a notebook."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{notebook_id}/jobs/RunNotebook/instances"
    resp = requests.post(url, headers=get_headers(), timeout=60)

    if resp.status_code == 202:
        location = resp.headers.get("Location")
        retry_after = int(resp.headers.get("Retry-After", 30))
        print(f"Notebook run started. Retry after: {retry_after}s")
        if location and "/instances/" in location:
            job_id = location.rstrip("/").split("/instances/")[-1].split("?")[0]
            print(f"Job instance ID: {job_id}")
            return job_id
        return None
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def get_notebook_status(workspace_id: str, notebook_id: str, job_instance_id: str):
    """Poll notebook job status."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_instance_id}"
    resp = requests.get(url, headers=get_headers(), timeout=30)

    if resp.status_code == 200:
        status = resp.json()
        print(json.dumps(status, indent=2))
        return status
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def delete_notebook(workspace_id: str, notebook_id: str):
    """Delete a notebook from the workspace."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks/{notebook_id}"
    resp = requests.delete(url, headers=get_headers(), timeout=30)

    if resp.status_code == 200:
        print(f"Deleted notebook {notebook_id}")
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "upload" and len(sys.argv) == 6:
        upload_file(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif command == "load-table" and len(sys.argv) == 6:
        load_table(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif command == "load-via-notebook" and len(sys.argv) == 6:
        generate_load_notebook(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif command == "deploy-notebook" and len(sys.argv) == 6:
        deploy_notebook(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif command == "run-notebook" and len(sys.argv) == 4:
        run_notebook(sys.argv[2], sys.argv[3])
    elif command == "status-notebook" and len(sys.argv) == 5:
        get_notebook_status(sys.argv[2], sys.argv[3], sys.argv[4])
    elif command == "delete-notebook" and len(sys.argv) == 4:
        delete_notebook(sys.argv[2], sys.argv[3])
    elif command == "list-tables" and len(sys.argv) == 4:
        list_tables(sys.argv[2], sys.argv[3])
    elif command == "delete-table" and len(sys.argv) == 5:
        delete_table(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
