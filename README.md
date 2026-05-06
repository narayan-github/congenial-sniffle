# Lab 5 – Incremental Ingestion (CDC & Watermarking) | Telecom Domain

## Objective
Build a telecom data pipeline that processes call records incrementally using:
- **Watermarking** (timestamp-based filtering)
- **CDC** (Change Data Capture simulation)
- Avoids full data reloads

## Dataset
| File | Description |
|------|-------------|
| `cdr_day1.csv` | Initial call detail records (Day 1) |
| `cdr_day2.csv` | New + updated records (Day 2) |
| `cdr_day2_late.csv` | Day 2 data with a late-arriving record |
| `cdr_day2_delete.csv` | Day 2 data with soft-delete flag |

## Steps Covered
1. Full Load (baseline – the slow way)
2. Watermarking – process only new/updated records
3. CDC Simulation – INSERT and UPDATE handling
4. Aggregation after incremental load
5. Watermark persistence to file
6. Full pipeline function

## Exercises
- **Exercise 1:** Add `network_type` (4G/5G) column to pipeline
- **Exercise 2:** Handle late-arriving data using update timestamp
- **Exercise 3:** Simulate deletes using `is_deleted` flag

## Run
```bash
pip install pandas
python pipeline.py
```
