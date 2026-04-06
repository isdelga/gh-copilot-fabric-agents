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

### Primary Keys
- Use sequential integers starting from 1: `1, 2, 3, ...`
- Date dimension keys use `YYYYMMDD` format: `20240101, 20240102, ...`

### Foreign Keys
- Randomly sample from the referenced dimension's PK values
- For time-series facts, distribute dates across the full date range (not uniform — use a slight recent-bias if realistic)

### Realistic Values
- **Names**: Use `faker` library with appropriate locale (`es_ES` for Spanish, `en_US` for international)
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

## Upload Flow

The upload to Fabric is a two-step process per table:

1. **Upload Parquet to OneLake `Files/`**: Use MCP `onelake_upload_file` or the script's `upload` command
2. **Load as Delta table**: Use the script's `load-table` command which calls `POST /v1/workspaces/{id}/lakehouses/{id}/tables/{name}/load`

This two-step process is required because the Load Table API reads from `Files/` (not `Tables/`).

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

### Load a file as a Delta table

```bash
python scripts/fabric_synthetic_data.py load-table <workspace_id> <lakehouse_id> <table_name> <relative_path>
```

Calls the Load Table API with `mode: Overwrite`, `format: Parquet`, `pathType: File`.
The `relative_path` is relative to the lakehouse root (e.g., `Files/synthetic_data/customers.parquet`).

### List existing tables

```bash
python scripts/fabric_synthetic_data.py list-tables <workspace_id> <lakehouse_id>
```

### Delete a table

```bash
python scripts/fabric_synthetic_data.py delete-table <workspace_id> <lakehouse_id> <table_name>
```

## Gotchas

- **Upload then load — two steps.** You cannot write directly to `Tables/`. Upload Parquet to `Files/` first, then call Load Table to register it as Delta.
- **Load Table `relativePath` must start with `Files/`** and use the pattern `Files/path/to/file.parquet`. No leading slash.
- **Table names must match `^[a-zA-Z_][a-zA-Z0-9_]{0,255}$`**. No spaces, no dashes, no special chars.
- **`faker` locale for Spanish is `es_ES`**. Use `Faker('es_ES')` for Spanish names, addresses, etc.
- **Generate dimensions before facts** so FK values can reference valid PKs.
- **Date dimension should cover the full range** of dates in fact tables. Generate it first, then sample date_ids for facts.
- **Parquet preserves types.** Use proper pandas dtypes: `int64` for IDs, `float64` for amounts, `datetime64` for dates, `object` for strings.
- **Load Table API is async (202).** The script polls automatically.
- **OneLake upload has a file size limit.** For large datasets (>100MB), consider splitting into multiple Parquet files and using `pathType: Folder`.

## Global Rules

- **Generate dimensions first, facts second.** Facts reference dimensions via FKs.
- **Always maintain referential integrity.** Every FK value must exist in the referenced dimension.
- **Use realistic data, not "test1, test2".** Use faker for names, realistic ranges for numbers, proper date distributions.
- **Parquet format only.** No CSV — Parquet preserves types and is faster to load.
- **Non-destructive by default.** Use `mode: Overwrite` for the specific table, but never delete unrelated tables.
