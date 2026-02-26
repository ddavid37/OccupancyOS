"""
Hotel Operational Ontology Ingestion
Produces three clean CSVs for Palantir Foundry:
  - bookings_core.csv
  - arrival_metadata.csv
  - booking_financials.csv
"""

import hashlib
import numpy as np
import pandas as pd

# ── 1. Load ────────────────────────────────────────────────────────────────────
RAW_PATH = r"archive\hotel_bookings.csv"
OUT_DIR   = r"."

df = pd.read_csv(RAW_PATH)
print(f"Loaded  : {df.shape[0]:,} rows × {df.shape[1]} columns")

# ── 2. Null-safety on source columns used downstream ──────────────────────────
# children has 4 nulls; adults/babies are clean — fill children with 0
df["children"] = df["children"].fillna(0).astype(int)

# ── 3. Derived columns ────────────────────────────────────────────────────────
MONTH_MAP = {
    "January": 1,  "February": 2, "March": 3,     "April": 4,
    "May": 5,       "June": 6,     "July": 7,      "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}
df["arrival_date_month"] = df["arrival_date_month"].map(MONTH_MAP)

df["total_guests"]  = df["adults"] + df["children"] + df["babies"]
df["stay_duration"] = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]

# ── 4. Primary key: SHA-256 hash of (lead_time, year, day_of_month) ───────────
# Row index is included as a salt so the composite key is globally unique
# even when (lead_time, year, day_of_month) repeats across bookings.
def make_booking_id(row):
    key = f"{row.name}|{row['lead_time']}|{row['arrival_date_year']}|{row['arrival_date_day_of_month']}"
    return hashlib.sha256(key.encode()).hexdigest()

df["booking_id"] = df.apply(make_booking_id, axis=1)

total_ids = df["booking_id"].nunique()
assert total_ids == len(df), f"Duplicate booking_ids found! ({total_ids} unique / {len(df)} rows)"
print(f"Unique booking_ids : {total_ids:,}  (all {len(df):,} rows distinct)")

# ── 5. Simulate realistic total_cost ─────────────────────────────────────────
# Base nightly rates (USD) per hotel type
BASE_RATE = {
    "Resort Hotel": 220,
    "City Hotel":   130,
}

# Seasonal multiplier keyed on arrival month
# Peak: Jun–Aug  |  Shoulder: Apr–May, Sep–Oct  |  Low: Nov–Mar
SEASON_MULT = {
    1: 0.80, 2: 0.80, 3: 0.85,
    4: 0.95, 5: 1.00,
    6: 1.20, 7: 1.35, 8: 1.30,
    9: 1.10, 10: 0.95,
    11: 0.85, 12: 0.90,
}

rng = np.random.default_rng(seed=42)

def simulate_total_cost(row):
    base        = BASE_RATE.get(row["hotel"], 150)
    season      = SEASON_MULT.get(row["arrival_date_month"], 1.0)
    # Per-night rate with +/-15% random spread for natural variance
    nightly     = base * season * rng.uniform(0.85, 1.15)
    nights      = max(row["stay_duration"], 1)   # at least 1 night billed
    full_cost   = round(nightly * nights, 2)
    # Canceled: charge 20% cancellation fee
    if row["is_canceled"] == 1:
        return round(full_cost * 0.20, 2)
    return full_cost

df["total_cost"] = df.apply(simulate_total_cost, axis=1)

# ── 6. Build output frames ────────────────────────────────────────────────────
bookings_core = df[[
    "booking_id", "hotel", "is_canceled",
    "lead_time", "total_guests", "stay_duration",
]].copy()

arrival_metadata = df[[
    "booking_id", "arrival_date_year", "arrival_date_month",
    "arrival_date_day_of_month", "stays_in_weekend_nights",
]].copy()

booking_financials = df[[
    "booking_id", "hotel", "is_canceled", "stay_duration", "total_cost",
]].copy()

# ── 7. Null audit ─────────────────────────────────────────────────────────────
frames = [
    ("bookings_core",      bookings_core),
    ("arrival_metadata",   arrival_metadata),
    ("booking_financials", booking_financials),
]
for name, frame in frames:
    null_total = frame.isnull().sum().sum()
    assert null_total == 0, f"[{name}] still contains {null_total} null value(s)!"
    print(f"{name:22s}: {len(frame):,} rows | nulls = {null_total}")

# ── 8. Export ─────────────────────────────────────────────────────────────────
bookings_core.to_csv(     f"{OUT_DIR}\\bookings_core.csv",      index=False)
arrival_metadata.to_csv(  f"{OUT_DIR}\\arrival_metadata.csv",   index=False)
booking_financials.to_csv(f"{OUT_DIR}\\booking_financials.csv", index=False)

print("\nExported:")
print(f"  bookings_core.csv      : {OUT_DIR}\\bookings_core.csv")
print(f"  arrival_metadata.csv   : {OUT_DIR}\\arrival_metadata.csv")
print(f"  booking_financials.csv : {OUT_DIR}\\booking_financials.csv")
print("Done.")
