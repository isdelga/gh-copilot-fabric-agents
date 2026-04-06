---
name: fabric-semantic-model
description: "Infer and generate Power BI semantic models (TMDL format) from Microsoft Fabric lakehouse tables. Covers star-schema detection, table classification (fact/dimension), relationship inference, DAX measure generation, data type mapping, and deployment via REST API. Use when: create semantic model, build data model, star schema, Power BI model, TMDL, fact table, dimension table."
compatibility: "Requires the Fabric MCP Server VS Code extension for table discovery. Generates TMDL files deployable to Microsoft Fabric via REST API."
---

# Fabric Semantic Model Creator — Reference

Generates Power BI semantic models in TMDL format by analyzing lakehouse table schemas and sample data. The agent reasons about table names, column names, data types, cardinality, and sample values to infer the best star-schema model.

## Table Classification Rules

Classify each lakehouse table as **fact** or **dimension** based on these signals:

| Signal | Fact Table | Dimension Table |
|--------|-----------|-----------------|
| **Name patterns** | Contains `fact_`, `fct_`, `sales`, `orders`, `transactions`, `events`, `log`, `movements` | Contains `dim_`, `lookup`, `calendar`, `date`, `product`, `customer`, `employee`, `geography`, `category`, `status` |
| **Column patterns** | Has multiple `_id`/`_key` foreign keys + numeric measure columns (`amount`, `quantity`, `price`, `total`, `count`) | Has a single primary key + descriptive text columns (`name`, `description`, `label`, `title`, `code`) |
| **Row count** | Typically high (transactions) | Typically low (reference data) |
| **Numeric density** | Many numeric columns relative to total | Few numeric columns, mostly strings |

If ambiguous, default to **dimension**. Better to have an extra dimension than miss a fact table's relationships.

## Relationship Inference Rules

Detect relationships by matching column names across tables:

1. **Exact key match**: `orders.customer_id` → `customers.customer_id` (or `customers.id`)
2. **Name-based match**: column named `{table_name}_id` or `{table_name}_key` in a fact table points to the dimension with that name
3. **Single-column PK in dimension**: if a dimension has exactly one `_id`/`_key` column and a fact table has a column with the same name, infer the relationship
4. **Date keys**: columns named `date`, `date_id`, `fecha`, `order_date`, etc. connect to a date/calendar dimension

All relationships are **many-to-one** (fact → dimension) unless the agent detects a bridge/junction table (many-to-many).

Cross-filter direction: **Single** by default. Only use **Both** for bridge tables.

## DAX Measure Generation Rules

Generate measures based on column classification:

| Column Pattern | Measure | DAX Expression |
|---------------|---------|---------------|
| `amount`, `total`, `revenue`, `sales`, `importe` | Sum | `SUM('{table}'[{column}])` |
| `quantity`, `qty`, `count`, `cantidad`, `units` | Sum | `SUM('{table}'[{column}])` |
| `price`, `cost`, `precio`, `coste` | Average | `AVERAGE('{table}'[{column}])` |
| `discount`, `descuento` | Sum + Average | Both measures |
| Any numeric in fact table | Row Count | `COUNTROWS('{table}')` (one per fact table) |
| `id`/`key` in dimension | Distinct Count | `DISTINCTCOUNT('{table}'[{column}])` |

Naming convention: `Total {Column}`, `Avg {Column}`, `# {Table}` (row count), `# Unique {Column}` (distinct count). Use business-friendly names, not technical ones.

## Data Type Mapping

Map lakehouse (Spark) types to TMDL types:

| Spark Type | TMDL `dataType` | Notes |
|-----------|-----------------|-------|
| `string` | `string` | |
| `int`, `integer` | `int64` | TMDL uses `int64` for all integers |
| `long`, `bigint` | `int64` | |
| `float` | `double` | TMDL has no float, use double |
| `double` | `double` | |
| `decimal` | `decimal` | |
| `boolean` | `boolean` | |
| `date` | `dateTime` | TMDL uses `dateTime` for dates |
| `timestamp` | `dateTime` | |

## TMDL File Structure

The semantic model is a folder of `.tmdl` files plus metadata:

```
{model_name}/
├── definition/
│   ├── database.tmdl          # Database-level settings
│   ├── model.tmdl             # Model-level settings (culture, access)
│   ├── relationships.tmdl     # All relationships
│   └── tables/
│       ├── {table1}.tmdl      # Table definition + columns + measures + partition
│       ├── {table2}.tmdl
│       └── ...
└── definition.pbism            # Semantic model settings
```

### database.tmdl template

```tmdl
database '{MODEL_NAME}'
	compatibilityLevel: 1604
```

### model.tmdl template

The model must include a `dataSource` block pointing to the lakehouse's SQL analytics endpoint. The agent must fetch this endpoint from the Fabric REST API: `GET /v1/workspaces/{id}/lakehouses/{id}` → look for `properties.sqlEndpointProperties.connectionString`.

```tmdl
model Model
	culture: es-ES
	defaultPowerBIDataSourceVersion: powerBI_V3
	discourageImplicitMeasures
	sourceQueryCulture: es-ES

	dataSource '{LAKEHOUSE_NAME}'
		type: structured
		connectionDetails
			protocol: tds
			address
				server: {SQL_ENDPOINT}
				database: {LAKEHOUSE_NAME}
		credential
			AuthenticationKind: OAuth2
```

### Table .tmdl template (for each table)

Partitions use Power Query `m` expressions with `mode: directQuery` connecting to the SQL analytics endpoint. Do NOT use `entity` partitions or `expressionSource: DatabaseQuery` — those fail when deploying via REST API.

```tmdl
table '{TABLE_NAME}'

	measure '# {TABLE_NAME}' = COUNTROWS('{TABLE_NAME}')
		formatString: #,##0

	column {COLUMN_NAME}
		dataType: {TMDL_TYPE}
		sourceColumn: {SOURCE_COLUMN}
		summarizeBy: {none|sum|average|count}

	partition '{TABLE_NAME}' = m
		mode: directQuery
		source =
			let
				Source = Sql.Database("{SQL_ENDPOINT}", "{LAKEHOUSE_NAME}"),
				dbo_{TABLE_NAME} = Source{{[Schema="dbo",Item="{TABLE_NAME}"]}}[Data]
			in dbo_{TABLE_NAME}
```

### relationships.tmdl template

```tmdl
relationship {GUID}
	fromColumn: '{FACT_TABLE}'.{FK_COLUMN}
	toColumn: '{DIM_TABLE}'.{PK_COLUMN}
```

### definition.pbism

```json
{
  "version": "4.0",
  "settings": {}
}
```

## Sample Data Analysis

When classifying tables and inferring relationships, analyze sample data (first 100 rows) to:

1. **Confirm cardinality**: A column with 10 distinct values in 10000 rows is likely a FK to a dimension
2. **Detect date formats**: Sample values reveal if a string column is actually a date
3. **Detect key patterns**: IDs that are sequential integers vs UUIDs vs composite
4. **Validate relationships**: Check that FK values actually exist in the referenced dimension's PK

## Gotchas

- **Star schema is mandatory.** DAX is optimized for star schema. Flat/denormalized tables work but give poor performance and confuse AI tools. If the lakehouse has a single flat table, the agent should recommend splitting it into fact + dimensions (but not do it automatically).
- **Partitions must use Power Query `m` with `mode: directQuery`.** Do NOT use `entity` partitions, `expressionSource: DatabaseQuery`, or `mode: directLake` — those all fail when deploying via the REST API. The correct pattern is a Power Query `Sql.Database()` expression pointing to the lakehouse SQL analytics endpoint. See the table template above.
- **The model needs a `dataSource` block.** Without it, the partition's `Sql.Database()` call has no endpoint. Fetch the SQL analytics endpoint from `GET /v1/workspaces/{id}/lakehouses/{id}` → `properties.sqlEndpointProperties.connectionString`. Use `credential: AuthenticationKind: OAuth2`.
- **Use the script for SQL endpoint and schema discovery, not MCP.** The MCP server doesn't expose the SQL analytics endpoint and `onelake_get_table` fails for some lakehouses. Use the script's `sql-endpoint` command for the endpoint and `list-tables` command for table schemas. These use the REST API + Delta log reading and are reliable.
- **`summarizeBy: none` for all non-measure columns.** Only measure columns should have `sum`/`average`. Setting `summarizeBy` on regular columns creates implicit measures which pollute the model.
- **Use `discourageImplicitMeasures`** in model.tmdl. This forces explicit DAX measures and prevents automatic SUM/COUNT on numeric columns.
- **Spanish locale**: Use `culture: es-ES` and `sourceQueryCulture: es-ES` in model.tmdl. Format strings use `.` for thousands and `,` for decimals in Spanish.
- **Relationship GUIDs**: Generate a unique UUID for each relationship. Use Python's `uuid.uuid4()`.
- **Column names with spaces or special chars**: Wrap in single quotes in TMDL: `column 'Total Amount'`.
- **Key columns should be `isHidden` and `isKey`**: Primary keys in dimensions should be hidden from report view and marked as the table key.
- **One `COUNTROWS` measure per fact table**: Always generate this as the base row-count measure.
- **The Fabric API requires `definition` as a required field** for semantic model creation (unlike notebooks where it's optional). The `definition/` folder (TMDL) must be included in the payload.
- **`displayName` must be unique.** Use the script's `list` command to check before deploying. If a model with the same name exists, ask the user — do not overwrite.

## Deployment, Management & Cleanup

Use [./scripts/fabric_semantic_model.py](./scripts/fabric_semantic_model.py) to manage semantic models in Fabric via the REST API. Uses `DefaultAzureCredential` (Azure CLI session).

**Install dependencies** (one-time):
```bash
pip install azure-identity requests
```

### Deploy a semantic model

Reads a local TMDL folder, base64-encodes each file, and creates a SemanticModel item via `POST /v1/workspaces/{id}/semanticModels`. Includes the `.platform` and `definition.pbism` files.

```bash
python scripts/fabric_semantic_model.py deploy <workspace_id> <model_name> <tmdl_folder_path>
```

### List semantic models

```bash
python scripts/fabric_semantic_model.py list <workspace_id> [<model_name>]
```

### Delete a semantic model

```bash
python scripts/fabric_semantic_model.py delete <workspace_id> <model_id>
```

### List tables with schemas

Returns all tables in a lakehouse with their column names and types as JSON. Uses the Fabric REST API to list tables, then reads each table's Delta log to extract the schema. Use it instead of MCP `onelake_get_table` if it fails.

```bash
python scripts/fabric_semantic_model.py list-tables <workspace_id> <lakehouse_id>
```

### Get lakehouse SQL analytics endpoint

Returns the SQL analytics endpoint required for the `dataSource` block in model.tmdl. Run this in Phase 1 and use the output when generating TMDL files.

```bash
python scripts/fabric_semantic_model.py sql-endpoint <workspace_id> <lakehouse_id>
```

### Deployment gotchas

- The `definition` field is **required** for semantic model creation — unlike notebooks, you cannot create an empty model and update it later.
- `displayName` must be unique in the workspace.
- TMDL format is the default and recommended. Use `definition/` folder structure.
- The API may return 202 for provisioning — the script polls automatically.

## Global Rules

- **TMDL format only.** Generate TMDL files, not model.bim (TMSL). TMDL is the modern format, text-based, and Git-friendly.
- **Star schema.** Always model as star schema with clear fact and dimension tables.
- **Explicit measures only.** Use `discourageImplicitMeasures` and create all measures explicitly in DAX.
- **Business-friendly names.** Tables, columns, and measures should use clear names like "Total Revenue" not "TR_AMT".
- **Non-destructive.** Never overwrite an existing semantic model. Deploy with a unique name or ask the user first.
