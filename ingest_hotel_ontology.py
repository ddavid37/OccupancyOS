"""
Hotel Operational Ontology Ingestion
Produces two clean CSVs for Palantir Foundry:
  - bookings_core.csv
  - arrival_metadata.csv
"""

import hashlib
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

# ── 5. Build output frames ────────────────────────────────────────────────────
bookings_core = df[[
    "booking_id", "hotel", "is_canceled",
    "lead_time", "total_guests", "stay_duration",
]].copy()

arrival_metadata = df[[
    "booking_id", "arrival_date_year", "arrival_date_month",
    "arrival_date_day_of_month", "stays_in_weekend_nights",
]].copy()

# ── 6. Null audit ─────────────────────────────────────────────────────────────
for name, frame in [("bookings_core", bookings_core), ("arrival_metadata", arrival_metadata)]:
    null_total = frame.isnull().sum().sum()
    assert null_total == 0, f"[{name}] still contains {null_total} null value(s)!"
    print(f"{name:20s}: {len(frame):,} rows | nulls = {null_total}")

# ── 7. Export ─────────────────────────────────────────────────────────────────
bookings_core.to_csv(    f"{OUT_DIR}\\bookings_core.csv",     index=False)
arrival_metadata.to_csv( f"{OUT_DIR}\\arrival_metadata.csv",  index=False)

print("\nExported:")
print(f"  bookings_core.csv    : {OUT_DIR}\\bookings_core.csv")
print(f"  arrival_metadata.csv : {OUT_DIR}\\arrival_metadata.csv")
print("Done.")
