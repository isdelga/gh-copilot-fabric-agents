"""
Fabric Semantic Model Manager — deploy, list, delete, and get SQL endpoint via Fabric REST API.

Authentication: Uses Azure CLI credentials (az login) via DefaultAzureCredential.
No .env file needed if you are already logged in.

Usage:
    python fabric_semantic_model.py deploy       <workspace_id> <model_name> <tmdl_folder>
    python fabric_semantic_model.py list         <workspace_id> [<model_name>]
    python fabric_semantic_model.py delete       <workspace_id> <model_id>
    python fabric_semantic_model.py sql-endpoint <workspace_id> <lakehouse_id>    python fabric_semantic_model.py list-tables  <workspace_id> <lakehouse_id>
The deploy command reads a local TMDL folder structure and uploads it as a
Fabric semantic model item. The folder must contain:
  - definition/         (TMDL files: database.tmdl, model.tmdl, relationships.tmdl, tables/*.tmdl)
  - definition.pbism    (semantic model settings)

The sql-endpoint command returns the SQL analytics endpoint for a lakehouse,
which is needed for the dataSource block in model.tmdl.
"""

import sys
import os
import json
import base64
import time
import uuid
import requests
from azure.identity import DefaultAzureCredential

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def get_headers():
    """Get auth headers using the logged-in user's Azure CLI credentials."""
    credential = DefaultAzureCredential()
    token = credential.get_token(FABRIC_SCOPE)
    return {
        "Authorization": f"Bearer {token.token}",
        "Content-Type": "application/json",
    }


def _encode_file(path: str) -> str:
    """Read a file and return its base64-encoded content."""
    with open(path, "r", encoding="utf-8") as f:
        return base64.b64encode(f.read().encode("utf-8")).decode("utf-8")


def _collect_tmdl_parts(folder: str) -> list:
    """Walk the TMDL folder and collect all definition parts for the API payload."""
    parts = []

    # Walk the definition/ subfolder for TMDL files
    definition_dir = os.path.join(folder, "definition")
    if not os.path.isdir(definition_dir):
        print(f"Error: {definition_dir} not found", file=sys.stderr)
        sys.exit(1)

    for root, dirs, files in os.walk(definition_dir):
        for filename in sorted(files):
            filepath = os.path.join(root, filename)
            # Path relative to the model folder (e.g., "definition/tables/sales.tmdl")
            rel_path = os.path.relpath(filepath, folder).replace("\\", "/")
            parts.append({
                "path": rel_path,
                "payload": _encode_file(filepath),
                "payloadType": "InlineBase64",
            })

    # Add definition.pbism
    pbism_path = os.path.join(folder, "definition.pbism")
    if os.path.isfile(pbism_path):
        parts.append({
            "path": "definition.pbism",
            "payload": _encode_file(pbism_path),
            "payloadType": "InlineBase64",
        })
    else:
        # Generate a default one
        default_pbism = json.dumps({"version": "4.0", "settings": {}})
        parts.append({
            "path": "definition.pbism",
            "payload": base64.b64encode(default_pbism.encode()).decode(),
            "payloadType": "InlineBase64",
        })

    # Add .platform file
    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {
            "type": "SemanticModel",
            "displayName": os.path.basename(folder),
        },
        "config": {
            "version": "2.0",
            "logicalId": str(uuid.uuid4()),
        },
    }
    parts.append({
        "path": ".platform",
        "payload": base64.b64encode(json.dumps(platform).encode()).decode(),
        "payloadType": "InlineBase64",
    })

    return parts


def deploy_semantic_model(workspace_id: str, model_name: str, tmdl_folder: str):
    """Upload a local TMDL folder as a Fabric semantic model item."""
    if not os.path.isdir(tmdl_folder):
        print(f"Error: folder not found: {tmdl_folder}", file=sys.stderr)
        sys.exit(1)

    parts = _collect_tmdl_parts(tmdl_folder)
    print(f"Collected {len(parts)} definition parts from {tmdl_folder}")

    payload = {
        "displayName": model_name,
        "description": f"Auto-generated semantic model: {model_name}",
        "definition": {
            "parts": parts,
        },
    }

    url = f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels"
    resp = requests.post(url, headers=get_headers(), json=payload, timeout=120)

    if resp.status_code == 201:
        model = resp.json()
        print(f"Created semantic model: {model['displayName']} (id: {model['id']})")
        return model["id"]
    elif resp.status_code == 202:
        operation_url = resp.headers.get("Location")
        operation_id = resp.headers.get("x-ms-operation-id")
        print(f"Semantic model creation in progress (operation: {operation_id})")
        return _poll_long_running_operation(operation_url)
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def list_semantic_models(workspace_id: str, model_name: str = None):
    """List semantic models in the workspace, or find one by name."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels"
    resp = requests.get(url, headers=get_headers(), timeout=30)

    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)

    models = resp.json().get("value", [])

    if model_name:
        matches = [m for m in models if m["displayName"] == model_name]
        if matches:
            for m in matches:
                print(f"{m['id']}  {m['displayName']}")
            return matches[0]["id"]
        else:
            print(f"Not found: {model_name}")
            return None
    else:
        if not models:
            print("No semantic models found.")
        for m in models:
            print(f"{m['id']}  {m['displayName']}")
        return models


def delete_semantic_model(workspace_id: str, model_id: str):
    """Delete a semantic model from the workspace."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels/{model_id}"
    resp = requests.delete(url, headers=get_headers(), timeout=30)

    if resp.status_code == 200:
        print(f"Deleted semantic model {model_id}")
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def _poll_long_running_operation(operation_url: str, max_wait: int = 300):
    """Poll a long-running operation until it completes."""
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
            result_id = data.get("resourceId") or data.get("id")
            if result_id:
                print(f"  Resource ID: {result_id}")
            return result_id
        elif status in ("Failed", "Cancelled"):
            error = data.get("error", {})
            print(f"  Failed: {error.get('message', 'Unknown error')}", file=sys.stderr)
            return None

        retry_after = int(resp.headers.get("Retry-After", 10))
        time.sleep(retry_after)
        elapsed += retry_after

    print(f"Timed out after {max_wait}s", file=sys.stderr)
    return None


def get_sql_endpoint(workspace_id: str, lakehouse_id: str):
    """Get the SQL analytics endpoint for a lakehouse."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
    resp = requests.get(url, headers=get_headers(), timeout=30)

    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    sql_props = data.get("properties", {}).get("sqlEndpointProperties", {})
    connection_string = sql_props.get("connectionString")

    if connection_string:
        print(connection_string)
        return connection_string
    else:
        print("SQL endpoint not found in lakehouse properties", file=sys.stderr)
        print(f"Available properties: {json.dumps(data.get('properties', {}), indent=2)}", file=sys.stderr)
        sys.exit(1)


def list_tables(workspace_id: str, lakehouse_id: str):
    """List all tables in a lakehouse with their column schemas via the REST API.

    This is the reliable fallback when MCP onelake_get_table fails.
    Uses GET /v1/workspaces/{id}/lakehouses/{id}/tables for table names,
    then reads each table's Delta log to extract column schemas.
    """
    headers = get_headers()

    # Step 1: List tables via REST API
    url = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
    resp = requests.get(url, headers=headers, timeout=60)

    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)

    tables_data = resp.json().get("data", [])
    if not tables_data:
        print("No tables found.")
        return []

    # Step 2: Get lakehouse info for OneLake DFS access
    lh_resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
        headers=headers, timeout=30
    )
    lh_data = lh_resp.json()
    onelake_path = lh_data.get("properties", {}).get("oneLakeTablesPath", "")

    results = []
    for table_info in tables_data:
        table_name = table_info.get("name", "unknown")
        table_format = table_info.get("format", "unknown")
        table = {"name": table_name, "format": table_format, "columns": []}

        # Step 3: Try reading Delta log for schema
        if table_format.lower() == "delta" and onelake_path:
            delta_log_url = f"{onelake_path}/{table_name}/_delta_log/00000000000000000000.json"
            try:
                delta_resp = requests.get(delta_log_url, headers=headers, timeout=30)
                if delta_resp.status_code == 200:
                    # Delta log is newline-delimited JSON; find the metaData entry
                    for line in delta_resp.text.strip().split("\n"):
                        entry = json.loads(line)
                        if "metaData" in entry:
                            schema_str = entry["metaData"].get("schemaString", "{}")
                            schema = json.loads(schema_str)
                            for field in schema.get("fields", []):
                                table["columns"].append({
                                    "name": field.get("name"),
                                    "type": field.get("type"),
                                    "nullable": field.get("nullable", True),
                                })
                            break
            except Exception as e:
                print(f"  Warning: Could not read Delta log for {table_name}: {e}", file=sys.stderr)

        results.append(table)

    # Output as JSON
    print(json.dumps(results, indent=2))
    return results


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "deploy" and len(sys.argv) == 5:
        deploy_semantic_model(sys.argv[2], sys.argv[3], sys.argv[4])
    elif command == "list" and len(sys.argv) in (3, 4):
        name = sys.argv[3] if len(sys.argv) == 4 else None
        list_semantic_models(sys.argv[2], name)
    elif command == "delete" and len(sys.argv) == 4:
        delete_semantic_model(sys.argv[2], sys.argv[3])
    elif command == "sql-endpoint" and len(sys.argv) == 4:
        get_sql_endpoint(sys.argv[2], sys.argv[3])
    elif command == "list-tables" and len(sys.argv) == 4:
        list_tables(sys.argv[2], sys.argv[3])
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
