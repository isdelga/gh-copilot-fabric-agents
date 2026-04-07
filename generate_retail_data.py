"""Generate synthetic retail data for lh_prueba lakehouse."""
import os
import random
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from faker import Faker

fake = Faker('es_ES')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

LAKEHOUSE_NAME = "lh_prueba"
TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H%M%S")
OUTPUT_DIR = f"./synthetic_data/{LAKEHOUSE_NAME}/{TIMESTAMP}"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Spanish DNI generation ---
DNI_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"

def generate_valid_dni():
    number = random.randint(10000000, 99999999)
    letter = DNI_LETTERS[number % 23]
    return f"{number}{letter}"

def generate_spanish_phone():
    return f"+34 6{random.randint(10, 99):02d} {random.randint(100, 999):03d} {random.randint(100, 999):03d}"

SPANISH_CITIES = [
    "Madrid", "Barcelona", "Sevilla", "Valencia", "Bilbao",
    "Zaragoza", "Málaga", "Murcia", "Palma de Mallorca", "Las Palmas",
    "Alicante", "Córdoba", "Valladolid", "Vigo", "Gijón",
    "Granada", "A Coruña", "Vitoria-Gasteiz", "Santa Cruz de Tenerife", "Pamplona"
]

PRODUCT_CATEGORIES = {
    "Electrónica": ["Smartphone", "Tablet", "Portátil", "Auriculares", "Altavoz Bluetooth", "Smartwatch", "Cargador", "Ratón inalámbrico", "Teclado mecánico", "Monitor"],
    "Hogar": ["Lámpara de mesa", "Cojín decorativo", "Espejo", "Alfombra", "Jarrón", "Vela aromática", "Marco de fotos", "Reloj de pared", "Perchero", "Estantería"],
    "Alimentación": ["Aceite de oliva", "Café molido", "Chocolate negro", "Miel", "Mermelada", "Galletas", "Frutos secos", "Té verde", "Especias", "Conservas"],
    "Ropa": ["Camiseta", "Pantalón vaquero", "Chaqueta", "Zapatillas", "Bufanda", "Gorra", "Vestido", "Sudadera", "Cinturón", "Calcetines"],
    "Deportes": ["Balón de fútbol", "Esterilla yoga", "Pesas", "Cuerda de saltar", "Botella deportiva", "Mochila trekking", "Gafas de natación", "Raqueta de pádel", "Guantes de boxeo", "Bicicleta estática"],
}

BRANDS = {
    "Electrónica": ["Samsung", "Apple", "Xiaomi", "Sony", "Logitech"],
    "Hogar": ["IKEA", "Zara Home", "Maisons du Monde", "Leroy Merlin", "El Corte Inglés"],
    "Alimentación": ["Hacendado", "La Española", "Nestlé", "Gallo", "El Pozo"],
    "Ropa": ["Zara", "Mango", "Pull&Bear", "Massimo Dutti", "Bershka"],
    "Deportes": ["Nike", "Adidas", "Decathlon", "Puma", "Reebok"],
}

STORE_TYPES = ["Centro Comercial", "Tienda de Barrio", "Gran Superficie", "Outlet", "Pop-up Store"]

# ============================================================
# 1. DATE DIMENSION
# ============================================================
print("Generating dates dimension...")
date_range = pd.date_range(start="2024-01-01", end="2025-12-31", freq="D")
dates_df = pd.DataFrame({
    "date_id": date_range.strftime("%Y%m%d").astype(int),
    "date": date_range,
    "year": date_range.year,
    "quarter": date_range.quarter,
    "month": date_range.month,
    "month_name": date_range.strftime("%B"),
    "day": date_range.day,
    "day_of_week": date_range.strftime("%A"),
    "is_weekend": date_range.weekday >= 5,
})
dates_df.to_parquet(f"{OUTPUT_DIR}/dates.parquet", index=False, engine="pyarrow")
print(f"  dates: {len(dates_df)} rows")

# ============================================================
# 2. CUSTOMERS DIMENSION
# ============================================================
print("Generating customers dimension...")
customers = []
for i in range(1, 101):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower().replace(' ', '')}@{fake.free_email_domain()}"
    customers.append({
        "customer_id": i,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": generate_spanish_phone(),
        "dni": generate_valid_dni(),
        "city": random.choice(SPANISH_CITIES),
        "registration_date": fake.date_between(start_date="-5y", end_date="today"),
    })
customers_df = pd.DataFrame(customers)
customers_df.to_parquet(f"{OUTPUT_DIR}/customers.parquet", index=False, engine="pyarrow")
print(f"  customers: {len(customers_df)} rows")

# ============================================================
# 3. PRODUCTS DIMENSION
# ============================================================
print("Generating products dimension...")
products = []
for i in range(1, 101):
    category = random.choice(list(PRODUCT_CATEGORIES.keys()))
    name = random.choice(PRODUCT_CATEGORIES[category])
    brand = random.choice(BRANDS[category])
    price = round(random.uniform(5.0, 500.0), 2)
    cost = round(price * random.uniform(0.3, 0.7), 2)
    products.append({
        "product_id": i,
        "name": f"{name} {brand}",
        "category": category,
        "brand": brand,
        "price": price,
        "cost": cost,
    })
products_df = pd.DataFrame(products)
products_df.to_parquet(f"{OUTPUT_DIR}/products.parquet", index=False, engine="pyarrow")
print(f"  products: {len(products_df)} rows")

# ============================================================
# 4. STORES DIMENSION
# ============================================================
print("Generating stores dimension...")
stores = []
for i in range(1, 101):
    city = random.choice(SPANISH_CITIES)
    store_type = random.choice(STORE_TYPES)
    stores.append({
        "store_id": i,
        "name": f"{store_type} {city} {i}",
        "city": city,
        "address": fake.street_address(),
        "opening_date": fake.date_between(start_date="-10y", end_date="today"),
    })
stores_df = pd.DataFrame(stores)
stores_df.to_parquet(f"{OUTPUT_DIR}/stores.parquet", index=False, engine="pyarrow")
print(f"  stores: {len(stores_df)} rows")

# ============================================================
# 5. ORDERS FACT TABLE
# ============================================================
print("Generating orders fact table...")
customer_ids = customers_df["customer_id"].tolist()
store_ids = stores_df["store_id"].tolist()
date_ids = dates_df["date_id"].tolist()

# Slight recent-bias: weight later dates more heavily
date_weights = np.arange(1, len(date_ids) + 1, dtype=float)
date_weights = date_weights / date_weights.sum()

orders = []
for i in range(1, 1001):
    total_amount = round(random.uniform(10.0, 2000.0), 2)
    discount_pct = round(random.uniform(0.0, 0.15), 2)
    tax_pct = 0.21
    orders.append({
        "order_id": i,
        "customer_id": random.choice(customer_ids),
        "store_id": random.choice(store_ids),
        "date_id": int(np.random.choice(date_ids, p=date_weights)),
        "total_amount": total_amount,
        "discount": round(total_amount * discount_pct, 2),
        "tax": round(total_amount * tax_pct, 2),
    })
orders_df = pd.DataFrame(orders)
orders_df.to_parquet(f"{OUTPUT_DIR}/orders.parquet", index=False, engine="pyarrow")
print(f"  orders: {len(orders_df)} rows")

# ============================================================
# 6. ORDER_LINES FACT TABLE
# ============================================================
print("Generating order_lines fact table...")
order_ids = orders_df["order_id"].tolist()
product_ids = products_df["product_id"].tolist()

order_lines = []
line_id = 1
for order_id in order_ids:
    num_lines = random.randint(1, 5)
    for _ in range(num_lines):
        product_id = random.choice(product_ids)
        product_row = products_df[products_df["product_id"] == product_id].iloc[0]
        quantity = random.randint(1, 10)
        unit_price = product_row["price"]
        line_total = round(quantity * unit_price, 2)
        order_lines.append({
            "order_line_id": line_id,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "line_total": line_total,
        })
        line_id += 1
order_lines_df = pd.DataFrame(order_lines)
order_lines_df.to_parquet(f"{OUTPUT_DIR}/order_lines.parquet", index=False, engine="pyarrow")
print(f"  order_lines: {len(order_lines_df)} rows")

# ============================================================
# SUMMARY
# ============================================================
print(f"\nAll files saved to: {OUTPUT_DIR}")
print(f"  dates.parquet       : {len(dates_df):>6} rows")
print(f"  customers.parquet   : {len(customers_df):>6} rows")
print(f"  products.parquet    : {len(products_df):>6} rows")
print(f"  stores.parquet      : {len(stores_df):>6} rows")
print(f"  orders.parquet      : {len(orders_df):>6} rows")
print(f"  order_lines.parquet : {len(order_lines_df):>6} rows")

# ============================================================
# FK INTEGRITY CHECK
# ============================================================
print("\nFK Integrity Checks:")
checks = [
    ("orders.customer_id", orders_df["customer_id"], set(customer_ids)),
    ("orders.store_id", orders_df["store_id"], set(store_ids)),
    ("orders.date_id", orders_df["date_id"], set(date_ids)),
    ("order_lines.order_id", order_lines_df["order_id"], set(order_ids)),
    ("order_lines.product_id", order_lines_df["product_id"], set(product_ids)),
]
all_ok = True
for name, fk_col, pk_set in checks:
    orphans = set(fk_col) - pk_set
    status = "OK" if not orphans else f"FAIL ({len(orphans)} orphans)"
    if orphans:
        all_ok = False
    print(f"  {name}: {status}")

if all_ok:
    print("\nAll FK integrity checks passed!")
else:
    print("\nWARNING: Some FK checks failed!")

# Print sample rows
print("\n--- Sample: dates (first 5) ---")
print(dates_df.head().to_string(index=False))
print("\n--- Sample: customers (first 5) ---")
print(customers_df.head().to_string(index=False))
print("\n--- Sample: products (first 5) ---")
print(products_df.head().to_string(index=False))
print("\n--- Sample: stores (first 5) ---")
print(stores_df.head().to_string(index=False))
print("\n--- Sample: orders (first 5) ---")
print(orders_df.head().to_string(index=False))
print("\n--- Sample: order_lines (first 5) ---")
print(order_lines_df.head().to_string(index=False))
