# OccupancyOS — Hotel Operational Ontology Ingestion

Pipeline that cleans raw hotel booking data and exports two structured CSVs
ready for ingestion into **Palantir Foundry**.

---

## Source Data

| File | Location | Rows | Columns |
|---|---|---|---|
| `hotel_bookings.csv` | `archive/` | 119,390 | 32 |

---

## Pipeline

Run the pipeline from the project root:

```bash
python ingest_hotel_ontology.py
```

### Steps

1. **Load** — reads `archive/hotel_bookings.csv` into a DataFrame.
2. **Null fix** — fills 4 missing values in `children` with `0`.
3. **Month conversion** — maps `arrival_date_month` from string (`"July"`) to integer (`7`).
4. **Derived columns**
   - `total_guests = adults + children + babies`
   - `stay_duration = stays_in_weekend_nights + stays_in_week_nights`
5. **Primary key** — generates `booking_id` as a SHA-256 hash of `row_index | lead_time | arrival_date_year | arrival_date_day_of_month`. The row index is included as a salt because the three domain columns alone do not produce a unique combination across all rows.
6. **Null audit** — asserts zero nulls in both output frames before writing.
7. **Export** — writes the two CSVs below.

---

## Output Files

### `bookings_core.csv`

Core booking facts.

| Column | Type | Description |
|---|---|---|
| `booking_id` | string (SHA-256) | Primary key |
| `hotel` | string | Hotel name |
| `is_canceled` | int (0/1) | Cancellation flag |
| `lead_time` | int | Days between booking and arrival |
| `total_guests` | int | Adults + children + babies |
| `stay_duration` | int | Weekend nights + week nights |

### `arrival_metadata.csv`

Arrival date breakdown and weekend split.

| Column | Type | Description |
|---|---|---|
| `booking_id` | string (SHA-256) | Primary key (join key) |
| `arrival_date_year` | int | Year of arrival |
| `arrival_date_month` | int (1–12) | Month of arrival (numeric) |
| `arrival_date_day_of_month` | int | Day of arrival |
| `stays_in_weekend_nights` | int | Nights on weekend |

Both files share `booking_id` as a join key and contain **zero null values**.

---

## Project Structure

```
OccupancyOS/
├── archive/
│   └── hotel_bookings.csv       # Raw source data
├── ingest_hotel_ontology.py     # Ingestion pipeline
├── bookings_core.csv            # Output: core booking facts
├── arrival_metadata.csv         # Output: arrival date metadata
└── README.md
```
