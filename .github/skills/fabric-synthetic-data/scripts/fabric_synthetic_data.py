"""
Fabric Synthetic Data Manager — upload Parquet files and load as Delta tables via Fabric REST API.

Authentication: Uses Azure CLI credentials (az login) via DefaultAzureCredential.

Usage:
    python fabric_synthetic_data.py upload       <workspace_id> <lakehouse_id> <local_path> <remote_filename>
    python fabric_synthetic_data.py load-table   <workspace_id> <lakehouse_id> <table_name> <relative_path>
    python fabric_synthetic_data.py list-tables  <workspace_id> <lakehouse_id>
    python fabric_synthetic_data.py delete-table <workspace_id> <lakehouse_id> <table_name>

Upload flow: upload Parquet to Files/ → load-table to register as Delta table.
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


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "upload" and len(sys.argv) == 6:
        upload_file(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif command == "load-table" and len(sys.argv) == 6:
        load_table(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif command == "list-tables" and len(sys.argv) == 4:
        list_tables(sys.argv[2], sys.argv[3])
    elif command == "delete-table" and len(sys.argv) == 5:
        delete_table(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
