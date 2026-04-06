---
name: fabric-data-cleaner
description: "Algorithm reference and self-contained PySpark notebook templates for cleaning Microsoft Fabric lakehouse tables. Covers profiling, duplicate detection, null analysis, type validation, statistics, IQR outlier detection, date format validation, Spanish DNI/NIE checksum validation, and email/phone format checks. Each notebook can be uploaded and run independently in Fabric."
compatibility: "Requires the Fabric MCP Server VS Code extension (fabric.vscode-fabric-mcp-server) for workspace and table discovery. Notebooks run on Microsoft Fabric's PySpark environment."
---

# Fabric Data Cleaner — Algorithm Reference

Self-contained PySpark `.ipynb` notebook templates for cleaning Microsoft Fabric lakehouse tables. Each notebook can be uploaded and run independently — configure `{{TABLE_NAME}}` and `{{LAKEHOUSE_NAME}}` in the first cell, run all cells to analyze, then optionally run the fix cell.

## Column Classification Rules

Classify the table's columns to select which notebooks to use:

| Category | Detection Rule | Which Notebook |
|----------|---------------|----------------|
| **Numeric** | Type is `int`, `long`, `float`, `double`, `decimal` | [type_validation.ipynb](./notebooks/type_validation.ipynb), [statistics.ipynb](./notebooks/statistics.ipynb), [outliers.ipynb](./notebooks/outliers.ipynb) |
| **Date/Timestamp** | Type is `date` or `timestamp`, or column name contains `date`, `fecha`, `time` | [date_validation.ipynb](./notebooks/date_validation.ipynb) |
| **DNI/Personal ID** | Column name contains `dni`, `nie`, `nif`, `documento`, `identidad`, `id_personal` | [dni_validation.ipynb](./notebooks/dni_validation.ipynb) |
| **Email** | Column name contains `email`, `correo`, `mail` | [contact_validation.ipynb](./notebooks/contact_validation.ipynb) |
| **Phone** | Column name contains `phone`, `tel`, `movil`, `telefono` | [contact_validation.ipynb](./notebooks/contact_validation.ipynb) |
| **Any column** | Always applicable | [profiling.ipynb](./notebooks/profiling.ipynb), [duplicates.ipynb](./notebooks/duplicates.ipynb), [nulls.ipynb](./notebooks/nulls.ipynb) |

## Gotchas

- **Notebook chaining**: Fix-cell notebooks read from `{TABLE_NAME}_cleaned` if it exists, otherwise from the original table. This means running multiple notebooks in sequence chains their fixes — each one builds on the previous. Profiling and statistics always read the original for baseline comparison.
- **Lakehouse binding**: Notebooks deployed via REST API must include lakehouse metadata in their JSON (`metadata.dependencies.lakehouse`) or `spark.table()` will fail. The deployment script handles this automatically when you pass the `lakehouse_id` argument. Without the binding, the notebook fails during session init (~30-50s) with no detail error — do NOT misdiagnose this as a capacity issue. The MCP server may report `capacityId: null` even when a capacity is assigned; ignore that field.
- **Table references use just the table name, not `lakehouse.table`.** In Fabric, when the default lakehouse is attached, `spark.table("my_table")` works. Using `spark.table("my_lakehouse.my_table")` fails with `TABLE_OR_VIEW_NOT_FOUND`. The `LAKEHOUSE_NAME` config variable is for display/logging only — never use it as a Spark schema qualifier.
- In Spanish-locale CSV exports, commas are decimal separators (`1.234,56` not `1234.56`). The `type_validation` notebook detects and fixes this, but if you skip it, numeric casts will silently produce nulls.
- The date `01/02/2024` means February 1st in Spain (`dd/MM/yyyy`), not January 2nd. Always default to `dd/MM/yyyy` unless the user specifies otherwise. The `date_validation` notebook auto-detects the best format.
- Whitespace-only strings (`"   "`) pass `isNotNull()` checks in Spark. The `nulls` notebook catches these; without it, they silently survive filters.
- DNI validation requires the exact mod-23 lookup table `TRWAGMYFPDXBNJZSQVHLCKE`. Do not attempt to compute the letter from scratch — use the UDF in the notebook.
- NIE prefix replacement is `X=0`, `Y=1`, `Z=2` — **not** their ASCII values. This is a common implementation error.
- Fabric notebooks use `display()` instead of `df.show()` for rich rendering. All templates use `display()`.
- Fix cells write to `{TABLE_NAME}_cleaned` as Delta tables. The original table is never modified. All notebooks write to the same `_cleaned` table — run them in order so each builds on the previous result.

## Notebooks — Always Applicable

### 1. Data Profiling — [profiling.ipynb](./notebooks/profiling.ipynb)

Run first on any table. Shows schema, sample rows, and per-column null/distinct counts.

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`

### 2. Duplicate Detection — [duplicates.ipynb](./notebooks/duplicates.ipynb)

Finds exact duplicates (window function over all columns) and near-duplicates (same natural key, different values). Fix cell removes exact duplicates via `dropDuplicates()` → writes `{TABLE_NAME}_cleaned`.

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`, `{{KEY_COLUMNS}}` (Python list, e.g., `["id", "fecha"]`; use `[]` to skip near-duplicate check)

### 3. Null & Missing Value Analysis — [nulls.ipynb](./notebooks/nulls.ipynb)

Counts `NULL`, empty string `""`, and whitespace-only values per column. Flags columns with >5% missing. Fix cell trims whitespace and replaces empties with `NULL` → writes `{TABLE_NAME}_cleaned`.

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`

## Notebooks — Numeric Columns

### 4. Type Validation — [type_validation.ipynb](./notebooks/type_validation.ipynb)

Detects non-numeric strings in columns that should be numeric. Also catches comma decimal separators (Spanish locale: `^\d+,\d+$`). Fix cell replaces commas with dots and casts to `DoubleType` → writes `{TABLE_NAME}_cleaned`.

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`, `{{NUMERIC_COLUMNS}}` (Python list, e.g., `["precio", "cantidad"]`)

### 5. Statistical Summary — [statistics.ipynb](./notebooks/statistics.ipynb)

Computes count, min, max, mean, median (`percentile_approx`), and stddev per numeric column. Read-only — no fix cell.

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`, `{{NUMERIC_COLUMNS}}`

### 6. Outlier Detection — [outliers.ipynb](./notebooks/outliers.ipynb)

IQR method: bounds are `[Q1 - k×IQR, Q3 + k×IQR]`. Default `k=1.5` (standard), use `3.0` for extreme-only. Fix cell adds boolean `{column}_outlier_flag` (does NOT delete) → writes `{TABLE_NAME}_cleaned`.

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`, `{{NUMERIC_COLUMNS}}`, `{{IQR_MULTIPLIER}}` (default `1.5`)

## Notebooks — Date Columns

### 7. Date Format Validation — [date_validation.ipynb](./notebooks/date_validation.ipynb)

For typed dates: checks for suspicious ranges (before 1900, future dates). For string dates: tries 8 common formats starting with the expected one, auto-detects the best match. Fix cell parses using the detected format → writes `{TABLE_NAME}_cleaned`.

Formats tested: `yyyy-MM-dd`, `dd/MM/yyyy`, `dd-MM-yyyy`, `yyyy-MM-dd HH:mm:ss`, `dd/MM/yyyy HH:mm:ss`, `d/M/yyyy`, `yyyy/MM/dd`, `yyyyMMdd`

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`, `{{DATE_COLUMNS}}` (Python list), `{{EXPECTED_DATE_FORMAT}}` (default `"dd/MM/yyyy"`)

## Notebooks — Personal Data (Spanish)

### 8. DNI/NIE Validation — [dni_validation.ipynb](./notebooks/dni_validation.ipynb)

Validates Spanish national IDs using the mod-23 checksum algorithm:

- **DNI** (8 digits + letter): `number % 23` → lookup in `TRWAGMYFPDXBNJZSQVHLCKE`
- **NIE** (X/Y/Z + 7 digits + letter): replace prefix (`X=0`, `Y=1`, `Z=2`), then same algorithm
- Pre-processing: uppercase, strip spaces/dashes/dots

UDF returns `(is_valid, reason)` with reasons: `valid_dni`, `valid_nie`, `empty`, `invalid_format`, `dni_bad_letter_expected_X`, `nie_bad_letter_expected_X`. Fix cell adds `{column}_valid` flag → writes `{TABLE_NAME}_cleaned`.

See [./references/dni-validation-rules.md](./references/dni-validation-rules.md) for the full algorithm reference.

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`, `{{DNI_COLUMNS}}` (Python list, e.g., `["dni", "documento_identidad"]`)

### 9. Contact Validation — [contact_validation.ipynb](./notebooks/contact_validation.ipynb)

- **Email**: regex `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
- **Spanish phone**: 9 digits starting with `6`/`7`/`8`/`9`, optionally prefixed with `+34`. Pre-processing strips spaces, dashes, dots, parentheses.

Fix cell adds `{column}_valid_email` / `{column}_valid_phone` flags → writes `{TABLE_NAME}_cleaned`.

**Placeholders**: `{{TABLE_NAME}}`, `{{LAKEHOUSE_NAME}}`, `{{EMAIL_COLUMNS}}` (use `[]` if none), `{{PHONE_COLUMNS}}` (use `[]` if none)

## Notebook Deployment, Execution & Cleanup

Use [./scripts/fabric_notebook.py](./scripts/fabric_notebook.py) to manage notebooks in Fabric via the REST API. The script uses `DefaultAzureCredential` — it picks up the logged-in user's Azure CLI session (`az login`), so no `.env` or secrets file is needed.


**Install dependencies** (one-time):
```bash
pip install azure-identity requests
```

### Deploy a notebook

Reads a local `.ipynb` file, injects Fabric lakehouse metadata (kernel info + default lakehouse binding), base64-encodes it with a `.platform` file, and creates a Notebook item via `POST /v1/workspaces/{id}/notebooks`. The `lakehouse_id` is **required** — without it, the notebook's Spark session has no lakehouse context and `spark.table()` calls fail.

```bash
python scripts/fabric_notebook.py deploy <workspace_id> <lakehouse_id> <notebook_name> <path_to.ipynb>
```

Returns the notebook ID on success. If the API returns 202 (provisioning), the script polls the operation URL until completion.

### Run a notebook

Triggers an on-demand execution via `POST /v1/workspaces/{id}/items/{notebook_id}/jobs/RunNotebook/instances`. The notebook runs on the Fabric Spark cluster attached to the workspace.

```bash
python scripts/fabric_notebook.py run <workspace_id> <notebook_id>
```

Returns the job instance ID. The notebook runs asynchronously — use `status` to poll.

### Check job status

Polls the job instance to check if the run completed, failed, or is still running.

```bash
python scripts/fabric_notebook.py status <workspace_id> <notebook_id> <job_instance_id>
```

### List notebooks

Lists all notebooks in the workspace, or checks if a specific notebook exists by display name. Returns the notebook ID if found.

```bash
python scripts/fabric_notebook.py list <workspace_id>                # list all
python scripts/fabric_notebook.py list <workspace_id> <notebook_name> # find by name
```

Use `list` to check if a notebook already exists before deploying, or to retrieve the notebook ID after a 202 deploy.

### Delete a notebook

Removes the notebook from the workspace via `DELETE /v1/workspaces/{id}/notebooks/{notebook_id}`. Use this to clean up after the cleaning notebooks have been run.

```bash
python scripts/fabric_notebook.py delete <workspace_id> <notebook_id>
```

### Deployment gotchas

- **Lakehouse binding is mandatory.** The `deploy` command requires `lakehouse_id` because it injects `metadata.dependencies.lakehouse` into the notebook JSON and includes a `.platform` file. Without this, the Spark session starts with no lakehouse context and `spark.table()` fails immediately. The lakehouse ID can be obtained from Phase 1 discovery via MCP `onelake_list_items`.
- The create API may return **202 instead of 201** for notebooks with definitions. Always handle both — the script does this automatically.
- Notebook `displayName` must be **unique within the workspace**. If a notebook with the same name exists, the API returns `ItemDisplayNameAlreadyInUse`. Delete the old one first or use a different name.
- The `RunNotebook` job type is the correct one for notebooks. Do not use `DefaultJob` — that's for other item types.
- Job status polling: the `Location` header from the run response is the poll URL. Respect the `Retry-After` header (usually 30-60s).
- `DefaultAzureCredential` tries Azure CLI first, then managed identity, then environment variables. If `az login` is active, it just works.
- **Idempotency — do not call deploy twice for the same notebook.** The `deploy` command is not idempotent. If a deploy returns 202 (provisioning), wait for the operation to complete — do NOT call `deploy` again with the same name. The second call will fail with `ItemDisplayNameAlreadyInUse`. Similarly, after deleting a notebook, the name may take up to 90 seconds to become available; the API returns `ItemDisplayNameNotAvailableYet` (409, retriable) during this window. The correct pattern is: deploy → wait for completion → `list` to get the ID → run. Never fire-and-forget a deploy and immediately retry.
- **One operation at a time.** Do not deploy multiple notebooks in parallel — the Fabric API may throttle or return intermittent errors. Deploy, confirm success, then deploy the next one sequentially.
- **Use `list` to verify state.** Before deploying, run `list <workspace_id> <notebook_name>` to check if the notebook already exists. After deploying, use `list` to confirm it was created and retrieve its ID. This avoids duplicate deploys and guessing IDs.

## Notebook Validation

Use [./scripts/validate_notebooks.py](./scripts/validate_notebooks.py) to verify generated notebooks before deployment. Run it against the output directory after generating customized notebooks.

```bash
python scripts/validate_notebooks.py <directory>
```

The script checks every `.ipynb` file in the directory for:

1. **Valid JSON**: `json.load()` succeeds without exceptions.
2. **No remaining placeholders**: no `{{...}}` strings anywhere in the file.
3. **Notebook structure**: top-level `cells` and `metadata` keys exist; every cell has `cell_type` and `source`; every code cell has `outputs`.

Exit code `0` means all notebooks passed. Exit code `1` means at least one check failed — fix the notebook and re-run. Exit code `2` means invalid usage (bad directory or no `.ipynb` files found).

**Always validate before deploying.** A notebook with broken JSON will fail to upload; leftover placeholders will cause runtime `NameError`s in Spark.

## Global Rules

- **PySpark only.** Use `pyspark.sql.functions`, never pandas for processing.
- **Non-destructive.** Fix cells write to a new `{TABLE_NAME}_cleaned` table. The original is never modified. All notebooks target the same `_cleaned` suffix.
- **Fix cells require manual execution.** Every fix cell is preceded by a markdown warning.
- **Self-contained.** Each notebook has its own imports, config, SparkSession, and table loading. No cross-notebook dependencies.
