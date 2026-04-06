"""
Fabric Notebook Manager — deploy, run, poll, list, and delete notebooks via Fabric REST API.

Authentication: Uses Azure CLI credentials (az login) via DefaultAzureCredential.
No .env file needed if you are already logged in.

Usage:
    python fabric_notebook.py deploy  <workspace_id> <lakehouse_id> <notebook_name> <ipynb_path>
    python fabric_notebook.py run     <workspace_id> <notebook_id>
    python fabric_notebook.py status  <workspace_id> <notebook_id> <job_instance_id>
    python fabric_notebook.py list    <workspace_id> [<notebook_name>]
    python fabric_notebook.py delete  <workspace_id> <notebook_id>

The deploy command requires the lakehouse_id so the notebook is automatically
attached to the default lakehouse. Without this, spark.table() calls fail because
the Spark session has no lakehouse context.
"""

import sys
import json
import base64
import time
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


def deploy_notebook(workspace_id: str, lakehouse_id: str, notebook_name: str, ipynb_path: str):
    """Upload a local .ipynb file as a new Fabric notebook item with lakehouse attached."""
    with open(ipynb_path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    # Inject Fabric-specific metadata so the notebook has a default lakehouse
    # and runs on the synapse_pyspark kernel. Without this, spark.table() fails.
    nb.setdefault("metadata", {})
    nb["metadata"]["kernel_info"] = {"name": "synapse_pyspark"}
    nb["metadata"]["dependencies"] = {
        "lakehouse": {
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": "",
            "default_lakehouse_workspace_id": workspace_id,
            "known_lakehouses": [
                {
                    "id": lakehouse_id,
                }
            ],
        }
    }

    notebook_content = json.dumps(nb)
    encoded_notebook = base64.b64encode(notebook_content.encode("utf-8")).decode("utf-8")

    # Build .platform file with notebook type metadata
    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {
            "type": "Notebook",
            "displayName": notebook_name,
        },
        "config": {
            "version": "2.0",
            "logicalId": "",
        },
    }
    encoded_platform = base64.b64encode(json.dumps(platform).encode("utf-8")).decode("utf-8")

    payload = {
        "displayName": notebook_name,
        "description": f"Auto-generated cleaning notebook: {notebook_name}",
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": encoded_notebook,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": ".platform",
                    "payload": encoded_platform,
                    "payloadType": "InlineBase64",
                },
            ],
        },
    }

    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks"
    resp = requests.post(url, headers=get_headers(), json=payload, timeout=60)

    if resp.status_code == 201:
        notebook = resp.json()
        print(f"Created notebook: {notebook['displayName']} (id: {notebook['id']})")
        return notebook["id"]
    elif resp.status_code == 202:
        operation_url = resp.headers.get("Location")
        operation_id = resp.headers.get("x-ms-operation-id")
        print(f"Notebook creation in progress (operation: {operation_id})")
        print(f"Poll: {operation_url}")
        return _poll_long_running_operation(operation_url)
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def run_notebook(workspace_id: str, notebook_id: str):
    """Trigger an on-demand run of a Fabric notebook."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{notebook_id}/jobs/RunNotebook/instances"
    resp = requests.post(url, headers=get_headers(), timeout=60)

    if resp.status_code == 202:
        location = resp.headers.get("Location")
        retry_after = int(resp.headers.get("Retry-After", 30))
        print(f"Notebook run started.")
        print(f"Poll status: {location}")
        print(f"Retry after: {retry_after}s")

        # Extract job instance ID from location URL
        # Format: .../jobs/instances/<job_instance_id>
        if location and "/instances/" in location:
            job_instance_id = location.rstrip("/").split("/instances/")[-1].split("?")[0]
            print(f"Job instance ID: {job_instance_id}")
            return job_instance_id
        return None
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def get_job_status(workspace_id: str, notebook_id: str, job_instance_id: str):
    """Poll the status of a notebook job run."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_instance_id}"
    resp = requests.get(url, headers=get_headers(), timeout=30)

    if resp.status_code == 200:
        status = resp.json()
        print(json.dumps(status, indent=2))
        return status
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def list_notebooks(workspace_id: str, notebook_name: str = None):
    """List all notebooks in the workspace, or find one by display name."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks"
    resp = requests.get(url, headers=get_headers(), timeout=30)

    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)

    notebooks = resp.json().get("value", [])

    if notebook_name:
        matches = [nb for nb in notebooks if nb["displayName"] == notebook_name]
        if matches:
            for nb in matches:
                print(f"{nb['id']}  {nb['displayName']}")
            return matches[0]["id"]
        else:
            print(f"Not found: {notebook_name}")
            return None
    else:
        if not notebooks:
            print("No notebooks found.")
        for nb in notebooks:
            print(f"{nb['id']}  {nb['displayName']}")
        return notebooks


def delete_notebook(workspace_id: str, notebook_id: str):
    """Delete a notebook from the Fabric workspace."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks/{notebook_id}"
    resp = requests.delete(url, headers=get_headers(), timeout=30)

    if resp.status_code == 200:
        print(f"Deleted notebook {notebook_id}")
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


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "deploy" and len(sys.argv) == 6:
        deploy_notebook(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif command == "run" and len(sys.argv) == 4:
        run_notebook(sys.argv[2], sys.argv[3])
    elif command == "status" and len(sys.argv) == 5:
        get_job_status(sys.argv[2], sys.argv[3], sys.argv[4])
    elif command == "list" and len(sys.argv) in (3, 4):
        name = sys.argv[3] if len(sys.argv) == 4 else None
        list_notebooks(sys.argv[2], name)
    elif command == "delete" and len(sys.argv) == 4:
        delete_notebook(sys.argv[2], sys.argv[3])
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
