# Sample input data

This folder holds a small sample of the synthetic input files that the pipeline ingests.
The full evaluation datasets (up to 1,000,000 rows per dataset) are not stored in the
repository because `chapter5_reproducibility/generate_data.py` regenerates them exactly
with seed 42.

| File | Content |
| --- | --- |
| `orders_001.csv` | One row per order line, with customer and product attributes; `order_ts` in local time (Europe/Berlin) without an offset |
| `clickstream_001.csv` | One row per user event; event details as a JSON object in `event_payload` |
| `payments_page_0001.csv` | One page of a payment-gateway export; amounts in minor units, gateway metadata as JSON in `metadata`; `created` as a UTC epoch |
| `injection_log.csv` | Ground truth: every record that received an injected defect and its expected error code |

About 2% of the records in each file carry exactly one injected defect, so the sample also
shows what the quarantine catches.

All timestamps are converted to UTC by the engine and stored as `TIMESTAMPTZ` in columns
ending in `_utc`; `date_key` is derived from the UTC date.

To regenerate this sample (from the repository root):

```bash
pip install -r chapter5_reproducibility/requirements.txt
python chapter5_reproducibility/generate_data.py --rows 100 --out src --seed 42
```

To load the sample with the engine, copy the CSV files (not `injection_log.csv`) into
`data/inbox/` and run the engine as described in the main README.
