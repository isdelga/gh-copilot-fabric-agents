---
name: fabric-synthetic-data
description: "Generate and upload synthetic data to Microsoft Fabric lakehouse tables. Covers star-schema generation, realistic fake data (names, dates, IDs, transactions), configurable row counts, referential integrity between tables, Parquet export, and OneLake upload. Use when: synthetic data, test data, fake data, sample data, generate tables, populate lakehouse, seed data, mock data."
compatibility: "Requires the Fabric MCP Server VS Code extension for workspace discovery. Uses Python with pandas and pyarrow for local data generation. Upload via OneLake MCP + Fabric REST API."
---

# Fabric Synthetic Data Generator — Reference

Generates realistic synthetic data locally as Parquet files, then uploads them to a Fabric lakehouse as Delta tables. Designed for populating test/dev lakehouses with star-schema-compatible data.

## Data Domain Templates

The agent should ask the user what domain they want. Common domains and their table structures:

### Retail / E-commerce
| Table | Type | Key Columns | Sample Columns |
|-------|------|-------------|----------------|
| customers | dim | customer_id | name, email, phone, city, country, registration_date |
| products | dim | product_id | name, category, brand, price, cost |
| stores | dim | store_id | name, city, country, opening_date |
| dates | dim | date_id | date, year, quarter, month, month_name, day, day_of_week, is_weekend |
| orders | fact | order_id | customer_id, store_id, date_id, total_amount, discount, tax |
| order_lines | fact | order_line_id | order_id, product_id, quantity, unit_price, line_total |

### Healthcare
| Table | Type | Key Columns | Sample Columns |
|-------|------|-------------|----------------|
| patients | dim | patient_id | name, dni, birth_date, gender, city, phone |
| doctors | dim | doctor_id | name, speciality, hospital_id |
| hospitals | dim | hospital_id | name, city, beds, founded_date |
| visits | fact | visit_id | patient_id, doctor_id, hospital_id, visit_date, diagnosis_code, cost |

### Airlines (like your existing data)
| Table | Type | Key Columns | Sample Columns |
|-------|------|-------------|----------------|
| airlines | dim | airline_id | name, code, country |
| airports | dim | airport_id | name, code, city, country, latitude, longitude |
| aircraft | dim | aircraft_id | model, manufacturer, capacity, airline_id |
| passengers | dim | passenger_id | name, email, phone, passport, nationality |
| flights | fact | flight_id | airline_id, aircraft_id, origin_airport_id, dest_airport_id, departure_date, arrival_date, status, delay_minutes |
| bookings | fact | booking_id | passenger_id, flight_id, booking_date, seat, class, price |

### Citizen Incidents (Smart City / Municipal)
| Table | Type | Key Columns | Sample Columns |
|-------|------|-------------|----------------|
| citizens | dim | citizen_id | name, dni, email, phone, birth_date, address, city, postal_code, registration_date |
| streets | dim | street_id | street_name, street_type, district, neighborhood, postal_code, city, latitude, longitude |
| incident_types | dim | incident_type_id | name, category, default_priority_id, sla_hours |
| priorities | dim | priority_id | name, level, max_resolution_hours, color |
| statuses | dim | status_id | name, is_final, sort_order |
| departments | dim | department_id | name, manager_name, phone, email |
| technicians | dim | technician_id | name, dni, department_id, role, hire_date, phone |
| dates | dim | date_id | date, year, quarter, month, month_name, day, day_of_week, is_weekend, is_holiday |
| incidents | fact | incident_id | citizen_id, street_id, incident_type_id, priority_id, status_id, department_id, assigned_technician_id, report_date_id, resolution_date_id, description, latitude, longitude, channel, resolution_time_hours, citizen_satisfaction, cost |
| incident_updates | fact | update_id | incident_id, technician_id, update_date_id, previous_status_id, new_status_id, comment |

### Custom
The user describes their tables and the agent designs the schema.

## Questions to Ask the User

Before generating data, the agent must gather:

1. **Domain**: What kind of data? (retail, healthcare, airlines, HR, finance, or custom)
2. **Tables**: Which tables to generate? (all from template, or pick specific ones)
3. **Volume**: How many rows per table? Suggest defaults:
   - Dimensions: 100-10,000 rows
   - Facts: 10,000-1,000,000 rows
   - Date dimension: auto-calculated from date range
4. **Date range**: What time period? (e.g., 2020-2024). Affects date dimension and fact table timestamps.
5. **Locale**: Spanish (DNIs, Spanish names, phone +34) or international?
6. **Target lakehouse**: Which workspace and lakehouse to upload to?
7. **Referential integrity**: Should FK values always reference valid PKs? (default: yes)

## Data Generation Rules

### Real-Time-Friendly Schema Design

Because the agent may later be asked to deploy a real-time streaming notebook (see
"Real-Time Streaming (Optional Follow-Up)"), **every schema must be designed from
the start so it can be streamed event-by-event without rework.** Apply these rules
during Phase 2 (schema design), not as an afterthought:

1. **Pick a clear "stream-able" fact.** At least one fact table must represent a
   discrete event that could plausibly happen in real time (an order placed, an
   incident reported, a flight departing, a sensor reading, a booking made). If the
   domain has no such fact, add one. Facts that are periodic aggregates (e.g.
   `monthly_sales_summary`) are NOT stream-able — do not pick those as the only fact.

2. **Event timestamp column.** Every stream-able fact should have either:
   - a `report_date_id` / `event_date_id` FK to the date dimension, **and/or**
   - a full-resolution `event_timestamp` (timestamp) column.

   The runtime notebook always injects an `event_timestamp` field into the emitted
   JSON, so having one in the fact schema too is natural and avoids surprising the
   downstream consumer.

3. **Separate "event-creation" columns from "event-completion" columns.** A real-time
   event is emitted the moment it starts — completion data does not exist yet. Mark
   the completion columns explicitly in the schema proposal with a **(post-event)**
   tag so the user understands they will be `NULL` in the stream. Typical examples:

   | Creation-time (always set) | Post-event (often NULL in stream) |
   |----------------------------|-----------------------------------|
   | citizen_id, incident_type_id, priority_id, report_date_id, channel, latitude, longitude, description | resolution_date_id, resolution_time_hours, citizen_satisfaction, cost |
   | customer_id, product_id, store_id, order_date_id, quantity, unit_price | shipped_date_id, delivered_date_id, return_reason, rating |
   | passenger_id, flight_id, booking_date_id, seat, class, price | check_in_time, boarding_time, satisfaction_score |

   The fact should still include these columns (the static dataset fills them in), but
   the real-time generator will emit them as `None` most of the time.

4. **Status / lifecycle columns.** If the fact has a status FK (`status_id`), the
   `statuses` dimension must include at least one "initial" status (`open`,
   `pending`, `scheduled`, `booked`) so new events have something valid to point to.
   Avoid schemas where every status implies the event already finished.

5. **Long-integer primary keys.** Stream-able fact PKs must be `int64` / `long`. The
   streaming template generates PKs as `int(time.time()*1000)*1000 + counter` which
   overflows anything smaller. Do NOT use UUID strings or composite PKs for a fact
   the user might want to stream.

6. **Dimensions must be cacheable in memory.** Keep every dimension that a
   stream-able fact references under ~50,000 rows. The streaming notebook loads each
   dim with `.limit(50000).toPandas()`, so huge dims would get silently truncated.
   If a "dimension" is naturally large (e.g. `citizens` = 10M), either (a) cap it at
   50k for the demo, or (b) warn the user and tell them the stream will only sample
   the first 50k PKs.

7. **Categorical value lists must be explicit and reusable.** For every categorical
   column (`channel`, `class`, `category`, `gender`, …), record the **exact** list of
   allowed values in Phase 2. The streaming notebook's `generate_event()` re-uses
   the same list via `random.choice([...])`. Ad-hoc categories break parity between
   the static table and the live stream.

8. **Numeric ranges must be recorded.** Same as categoricals — for every numeric
   column that isn't a key, record the realistic `(min, max)` range in Phase 2. The
   streaming notebook re-uses these ranges via `random.uniform(min, max)`.

9. **Avoid "post-hoc only" facts.** Do not design a fact table whose rows can only
   be computed *after* the event (e.g. `customer_lifetime_value_snapshot`,
   `daily_aggregate`). These have no streaming semantics.

When presenting the schema to the user in Phase 2, annotate each fact column with
one of: **(key)**, **(FK → dim)**, **(creation)**, **(post-event)**, **(enum: a/b/c)**,
**(range: min..max)**. This makes it trivial to produce a correct `RECORD_GENERATOR_BODY`
later without re-interviewing the user.

### Primary Keys
- Use sequential integers starting from 1: `1, 2, 3, ...`
- Date dimension keys use `YYYYMMDD` format: `20240101, 20240102, ...`

### Foreign Keys
- Randomly sample from the referenced dimension's PK values
- For time-series facts, distribute dates across the full date range (not uniform — use a slight recent-bias if realistic)

### Realistic Values
- **Names**: For Spanish locale, generate peninsular Spanish names with two surnames (paterno + materno), e.g., "Antonio García López". Use common first names and surnames from Spain (not Latin American). Do NOT use `faker` for Spanish names — it generates Latin American names.
- **Emails**: Derive from names: `firstname.lastname@domain.com`
- **Spanish DNIs**: Generate valid DNIs using the mod-23 algorithm (8 digits + computed letter)
- **Phone numbers**: Spanish format `+34 6XX XXX XXX` (mobile) or `+34 9XX XXX XXX` (landline)
- **Prices/amounts**: Use `random.uniform()` with realistic ranges, round to 2 decimals
- **Dates**: Use `pandas.date_range()` for date dimensions, `random.choice()` from date_id list for facts
- **Categories**: Use realistic category lists (not "Category A, B, C")

### Spanish DNI Generation (valid checksums)
```python
import random

DNI_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"

def generate_valid_dni():
    number = random.randint(10000000, 99999999)
    letter = DNI_LETTERS[number % 23]
    return f"{number}{letter}"
```

### Date Dimension Generation
```python
import pandas as pd

def generate_date_dimension(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({
        'date_id': dates.strftime('%Y%m%d').astype(int),
        'date': dates,
        'year': dates.year,
        'quarter': dates.quarter,
        'month': dates.month,
        'month_name': dates.strftime('%B'),
        'day': dates.day,
        'day_of_week': dates.strftime('%A'),
        'is_weekend': dates.weekday >= 5,
    })
    return df
```

## Output Format

Generate data as **Parquet files** (one per table). Save locally to:
`./synthetic_data/{LAKEHOUSE_NAME}/{TIMESTAMP}/{table_name}.parquet`

Use `pandas` + `pyarrow` for Parquet generation:
```python
df.to_parquet(f"{output_dir}/{table_name}.parquet", index=False, engine="pyarrow")
```

## Dirty Data Generation (Optional)

When requested, inject realistic data quality issues into the generated dataset. The user specifies a **dirt percentage** (e.g., 5% = 5% of rows per table are affected). Apply the following contamination types randomly across eligible columns:

### Contamination Types

| Type | Applies To | What It Does |
|------|-----------|-------------|
| **Null injection** | Any column | Replace value with `None` / `NaN` |
| **Empty string** | String columns | Replace value with `""` |
| **Whitespace-only** | String columns | Replace value with `"   "` (spaces) |
| **Exact duplicate rows** | Any table | Duplicate a random row exactly |
| **Near-duplicate rows** | Fact tables | Duplicate a row but change one non-key column |
| **Invalid DNI** | DNI columns | Wrong checksum letter, missing letter, wrong format (e.g., `1234567X` instead of `12345678X`) |
| **Invalid email** | Email columns | Missing `@`, missing domain, spaces (e.g., `juan garcia@`, `noarroba.com`, `juan @mail .com`) |
| **Invalid phone** | Phone columns | Wrong digit count, invalid prefix (e.g., `+34 1234`, `612345`, `+33 612345678`) |
| **Comma decimal** | Numeric columns stored as string | Replace `.` with `,` as decimal separator (e.g., `"1234,56"` instead of `1234.56`) |
| **String in numeric** | Numeric columns stored as string | Replace value with non-numeric text (e.g., `"N/A"`, `"pending"`, `"-"`, `"#REF!"`) |
| **Wrong date format** | Date columns stored as string | Mix formats: some `dd/MM/yyyy`, some `MM/dd/yyyy`, some `yyyy-MM-dd` |
| **Future/past dates** | Date columns | Dates far in the future (2099) or before 1900 |
| **Leading/trailing spaces** | String columns | Add spaces around values (e.g., `" Madrid "`) |
| **Case inconsistency** | String columns | Random uppercase/lowercase (e.g., `"MADRID"`, `"madrid"`, `"Madrid"`) |

### How to Apply Dirt

1. Calculate the number of dirty rows: `dirty_count = int(len(df) * dirt_percentage / 100)`
2. For each contamination type, randomly select a subset of rows and eligible columns
3. Distribute contamination types roughly evenly — don't apply all dirt to one column
4. Exact duplicate injection: append `int(dirty_count * 0.1)` duplicated rows to the table
5. For numeric columns that need comma decimals or string contamination: convert the column to string type first, then inject the dirty values
6. Apply dirt AFTER generating clean data — never corrupt the primary keys or foreign keys (that would break referential integrity)

## Real-Time Streaming (Optional Follow-Up)

After the static dataset is loaded into the lakehouse, the user can optionally deploy a
**real-time streaming notebook** that continuously emits synthetic fact records matching
the same schema and pushes them to a **Fabric Eventstream** custom endpoint (Event
Hub–compatible). This is useful for demoing real-time analytics, KQL databases,
Activator alerts, or Power BI direct-lake-on-event.

### Notebook template

Use [./notebooks/realtime_stream.ipynb](./notebooks/realtime_stream.ipynb). It is a
templated PySpark notebook with the following placeholders that the agent must fill
in before deployment:

| Placeholder | Filled by | Meaning |
|-------------|-----------|---------|
| `{{EVENTSTREAM_CONNECTION_STRING}}` | user | Full Event Hub connection string of the Eventstream custom endpoint |
| `{{EVENTSTREAM_ENTITY_PATH}}` | user | Event hub name (e.g. `es_xxx`) |
| `{{DURATION_MINUTES}}` | user | How long the notebook streams before stopping (e.g. `10`) |
| `{{EVENTS_PER_SECOND}}` | user | Emission rate (e.g. `5`) |
| `{{BATCH_SIZE}}` | user | Events per Event Hub batch (default `20`) |
| `{{FACT_TABLE_NAME}}` | agent | The fact table whose schema is mirrored |
| `{{LAKEHOUSE_NAME}}` | agent | Display-only label |
| `{{DIMENSION_TABLES}}` | agent | Python list of dim tables the notebook will cache for FK sampling, e.g. `["passengers","airports"]` |
| `{{PK_COLUMN}}` | agent | Primary key column of the fact table (e.g. `flight_id`) |
| `{{RECORD_GENERATOR_BODY}}` | agent | Python dict body that builds one event from the fact schema (see rules below) |

### How the notebook works

1. Installs `azure-eventhub` via `%pip install`.
2. Loads every dimension listed in `DIMENSION_TABLES` into an in-memory list of dicts
   (`dim_cache[table_name]`) so FKs and categorical values can be sampled fast.
3. Calls `generate_event()` (whose body the agent filled in) on each tick.
4. Sends events in batches of `BATCH_SIZE` via `EventHubProducerClient` until
   `DURATION_MINUTES` elapses.

### Rules for `{{RECORD_GENERATOR_BODY}}`

The agent must generate Python `"key": value,` entries (comma-terminated, indented
with 8 spaces to match the surrounding dict) for every column in the fact schema
**except** the PK and `event_timestamp` (those are emitted by the template itself).

> **Note**: `event_timestamp` is an intentional streaming-only field added by the
> template. It is NOT part of the fact table schema — it exists so downstream
> Eventstream consumers (KQL DB, Activator, Power BI direct-lake-on-event) have a
> reliable event time. Do not try to "match" the fact schema by removing it.

- **FK columns**: sample from the matching dimension cache:
  `"airline_id": random.choice(dim_cache["airlines"])["airline_id"],`
- **Date FKs to a date dimension**: for a real-time stream the event is happening
  **now**, so prefer `int(datetime.now(timezone.utc).strftime("%Y%m%d"))` over sampling
  from `dim_cache["dates"]`. Only sample from the date dim if the column semantically
  refers to a past date (e.g. `booking_date_id` on a flight that departs in the future).
- **Numeric amounts**: `round(random.uniform(low, high), 2)` with realistic ranges.
- **Categorical strings**: `random.choice([...realistic list...])`.
- **Status / enum columns**: match the exact value set used in the static data. For a
  new event, default to the "open / in progress / pending" status rather than a
  terminal one.
- **"Post-event" columns must default to `None`.** Any column that is only populated
  *after* the event completes — e.g. `resolution_date_id`, `resolution_time_hours`,
  `citizen_satisfaction`, `cost`, `delay_minutes`, `actual_arrival`, `rating` — should
  be `None` for a freshly-emitted event (or `None` ~80% of the time with a realistic
  value ~20% of the time to simulate late-arriving completions). Do NOT fill these
  with random values for new events — it breaks the "live stream" semantics.
- **Free-form timestamps** inside the event (departure, resolution, etc.): derive from
  `datetime.now(timezone.utc)` with small random offsets — never use static past dates.
- **Type drift from dirty data is expected.** If Phase 4 injected nulls into a long
  column, pandas converts it to `float64`/`double` in the stored Delta table. The
  streaming generator should still emit the **semantically correct** type (integer for
  IDs, `None` for unknown) — Spark/Eventstream will coerce. Do not try to emit floats
  just because the stored column is `double`.
- Do NOT overwrite the template's `{{PK_COLUMN}}` entry or `event_timestamp`.

### Fact table selection

- If the dataset has exactly one fact table, use it.
- If there are multiple, ask the user which one to stream. Pick the one with the most
  time-oriented semantics (e.g. `orders`, `incidents`, `flights`, `bookings`).
- Only one fact table per notebook. If the user wants multiple streams, deploy multiple
  notebooks (one per fact).

### Parameters to ask the user

1. **Eventstream connection string** (paste from Fabric custom endpoint)
2. **Event hub name** (from the same endpoint panel)
3. **Duration in minutes** (default: 10)
4. **Events per second** (default: 5; cap around 50 for a single notebook)
5. **Batch size** (default: 20)
6. **Fact table to stream** (if more than one fact exists)

### Deployment

Reuse the existing script commands — no new commands are needed:

1. Fill placeholders in a local copy of `realtime_stream.ipynb` under
   `./synthetic_data/{LAKEHOUSE_NAME}/{TIMESTAMP}/realtime_stream.ipynb`
2. `deploy-notebook <workspace_id> <lakehouse_id> <notebook_name> <ipynb_path>`
3. Do **not** auto-run the notebook. The user may want to configure the Eventstream
   side first, or run it manually from Fabric when ready. Report the notebook URL /
   id and let the user trigger it.
4. The user runs the notebook from Fabric when their Eventstream is ready. It will
   self-terminate after `DURATION_MINUTES`.

## Upload Flow

The upload to Fabric is a two-step process per table. There are **two paths** depending on whether the lakehouse has schemas enabled.

### Standard path (schemas NOT enabled)

1. **Upload Parquet to OneLake `Files/`**: Use the script's `upload` command
2. **Load as Delta table**: Use the script's `load-table` command

### Schemas-enabled path (Load Table API fails)

When a lakehouse has schemas enabled, the Load Table API returns an error. Use the notebook-based approach instead:

1. **Upload Parquet to OneLake `Files/`**: Same `upload` command
2. **Generate a load notebook**: Use `load-via-notebook` — creates a PySpark `.ipynb` that reads all Parquet files from `Files/synthetic_data/` and writes them as Delta tables via `spark.write`
3. **Deploy the notebook**: Use `deploy-notebook` — uploads to Fabric with lakehouse binding
4. **Run the notebook**: Use `run-notebook` — triggers execution on the Fabric Spark cluster
5. **Poll status**: Use `status-notebook` — wait until completed or failed
6. **Delete the notebook**: Use `delete-notebook` — clean up after successful run

**How to detect schemas-enabled**: If `load-table` fails with an error mentioning schemas, switch to the notebook path. Do NOT improvise — use the script commands above.

## Script — fabric_synthetic_data.py

Use [./scripts/fabric_synthetic_data.py](./scripts/fabric_synthetic_data.py) to upload and load data. Uses `DefaultAzureCredential` (Azure CLI).

**Install dependencies** (one-time):
```bash
pip install azure-identity requests pandas pyarrow faker
```

### Upload a Parquet file to OneLake Files/

```bash
python scripts/fabric_synthetic_data.py upload <workspace_id> <lakehouse_id> <local_parquet_path> <remote_filename>
```

Uploads to `Files/synthetic_data/{remote_filename}` in the lakehouse.

### Load a file as a Delta table (standard path)

```bash
python scripts/fabric_synthetic_data.py load-table <workspace_id> <lakehouse_id> <table_name> <relative_path>
```

Calls the Load Table API. If this fails due to schemas being enabled, switch to the notebook path below.

### Generate a load notebook (schemas-enabled path)

```bash
python scripts/fabric_synthetic_data.py load-via-notebook <workspace_id> <lakehouse_id> <parquet_dir> <output_notebook.ipynb>
```

Scans `<parquet_dir>` for `.parquet` files and generates a single PySpark notebook that loads all of them as Delta tables. The notebook includes lakehouse binding metadata.

### Deploy a load notebook to Fabric

```bash
python scripts/fabric_synthetic_data.py deploy-notebook <workspace_id> <lakehouse_id> <notebook_name> <ipynb_path>
```

Uploads the notebook with lakehouse binding so `spark.write.saveAsTable()` works.

### Run a notebook

```bash
python scripts/fabric_synthetic_data.py run-notebook <workspace_id> <notebook_id>
```

### Check notebook job status

```bash
python scripts/fabric_synthetic_data.py status-notebook <workspace_id> <notebook_id> <job_instance_id>
```

### Delete a notebook

```bash
python scripts/fabric_synthetic_data.py delete-notebook <workspace_id> <notebook_id>
```

### List existing tables

```bash
python scripts/fabric_synthetic_data.py list-tables <workspace_id> <lakehouse_id>
```

### Delete a table

```bash
python scripts/fabric_synthetic_data.py delete-table <workspace_id> <lakehouse_id> <table_name>
```

## Gotchas

- **Schemas-enabled lakehouses break the Load Table API.** If `load-table` fails, the lakehouse likely has schemas enabled. Switch to the notebook path: `load-via-notebook` → `deploy-notebook` → `run-notebook` → `status-notebook` → `delete-notebook`. Do NOT try to improvise or create notebooks manually — use the script commands.
- **`TIMESTAMP_NTZ` is not supported by Fabric Delta.** Pandas `datetime64` columns get written to Parquet as `TIMESTAMP_NTZ` which Fabric rejects with `Columns of the specified data types are not supported`. The `load-via-notebook` script handles this automatically by casting `TIMESTAMP_NTZ` columns to `DATE` before writing. If generating data manually, use `.dt.date` to convert datetime to date objects before saving Parquet.
- **`list-tables` and `load-table` both fail on schemas-enabled lakehouses.** The entire Lakehouse Tables REST API is unsupported. Use MCP `onelake_list_tables` for listing and the notebook path for loading.
- **Upload then load — two steps.** You cannot write directly to `Tables/`. Upload Parquet to `Files/` first, then load as Delta.
- **Load Table `relativePath` must start with `Files/`** and use the pattern `Files/path/to/file.parquet`. No leading slash.
- **Table names must match `^[a-zA-Z_][a-zA-Z0-9_]{0,255}$`**. No spaces, no dashes, no special chars.
- **`faker` locale for Spanish is `es_ES`**. Use `Faker('es_ES')` for Spanish names, addresses, etc.
- **Generate dimensions before facts** so FK values can reference valid PKs.
- **Date dimension should cover the full range** of dates in fact tables. Generate it first, then sample date_ids for facts.
- **Parquet preserves types.** Use proper pandas dtypes: `int64` for IDs, `float64` for amounts, `datetime64` for dates, `object` for strings.
- **Load Table API is async (202).** The script polls automatically.
- **OneLake upload has a file size limit.** For large datasets (>100MB), consider splitting into multiple Parquet files and using `pathType: Folder`.
- **Real-time notebook needs `%pip install azure-eventhub`.** Fabric Spark pools don't ship it by default. The template already installs it — do not remove the magic.
- **Eventstream connection string must include an `EntityPath` or the event hub name must be passed separately.** The template passes both and uses `eventhub_name=EVENTSTREAM_ENTITY_PATH` explicitly; this works whether or not `EntityPath` is in the connection string.
- **Don't auto-run the real-time notebook.** The user usually wants to configure consumers on the Eventstream side (KQL DB, Activator, Power BI) before events start flowing. Deploy only — let the user trigger it.

## Global Rules

- **Generate dimensions first, facts second.** Facts reference dimensions via FKs.
- **Always maintain referential integrity.** Every FK value must exist in the referenced dimension.
- **Use realistic data, not "test1, test2".** Use faker for names, realistic ranges for numbers, proper date distributions.
- **Parquet format only.** No CSV — Parquet preserves types and is faster to load.
- **Non-destructive by default.** Use `mode: Overwrite` for the specific table, but never delete unrelated tables.
