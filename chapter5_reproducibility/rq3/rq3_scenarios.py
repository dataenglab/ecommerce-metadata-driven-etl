"""rq3_scenarios.py - change-effort experiment: YAML configuration vs hard-coded baseline.

For each schema-change scenario the same change is applied to (a) pipeline_config.yaml for
etl_engine.py and (b) the hard-coded baseline. Effort is counted as lines added + removed in a
unified diff (Python: non-blank, non-comment lines). Each variant is then executed against a
scenario file to confirm that v0 fails as expected and the changed version behaves correctly.
"""
import csv
import difflib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from pgutil import PIPELINE as CH4, dsn_for, query, recreate_db  # noqa: E402
TIER = HERE.parent / "tiers" / "tier_10000" / "orders_001.csv"
DSN = dsn_for("rq3")
ENV = dict(os.environ, ETL_PG_DSN=DSN, ETL_PII_SALT="rq3-salt")

CFG_V0 = (CH4 / "pipeline_config.yaml").read_text(encoding="utf-8").replace(
    "{type: integer,   nullable: false, min: 1, max: 32767}", "{type: integer,   nullable: false, min: 1}", 1)
BASE_V0 = (HERE / "baseline_orders.py").read_text(encoding="utf-8")

RETURNS_DDL = """
CREATE TABLE dwh.fact_return (
    return_id       TEXT          PRIMARY KEY,
    order_id        TEXT          NOT NULL,
    line_number     SMALLINT      NOT NULL,
    date_key        INTEGER       NOT NULL REFERENCES dwh.dim_date,
    product_sk      BIGINT        REFERENCES dwh.dim_product,
    returned_at_utc TIMESTAMPTZ   NOT NULL,
    quantity        INTEGER       NOT NULL CHECK (quantity > 0),
    refund_amount   NUMERIC(12,2) NOT NULL CHECK (refund_amount >= 0),
    currency_code   CHAR(3)       NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$'),
    reason          TEXT          NOT NULL,
    run_id          UUID          NOT NULL
);
"""
RETURNS_YAML = """
  returns:
    file_pattern: "returns_*.csv"
    target: dwh.fact_return
    business_key: [return_id]
    mandatory_columns:
      [return_id, order_id, line_number, returned_at, product_sku, quantity, refund_amount, currency, reason]
    columns:
      return_id:     {type: string,    nullable: false}
      order_id:      {type: string,    nullable: false}
      line_number:   {type: integer,   nullable: false, min: 1, max: 32767}
      returned_at:   {type: timestamp, nullable: false, format: iso8601}
      product_sku:   {type: string,    nullable: false}
      quantity:      {type: integer,   nullable: false, min: 1}
      refund_amount: {type: decimal,   nullable: false, min: 0}
      currency:      {type: string,    nullable: false, standardise: upper, rename: currency_code,
                      allowed: [EUR, USD, GBP, CHF, PLN, SEK], error_code: INVALID_CURRENCY}
      reason:        {type: string,    nullable: false,
                      allowed: [damaged, wrong_item, not_as_described, changed_mind]}
    lookups:
      product_sk: {dimension: dwh.dim_product, key: product_sku, at: returned_at, required: false}
"""

SCENARIOS = {
    "S1 renamed attribute (currency_code -> currency)": {
        "yaml": [("product_name, category, quantity, unit_price, total_amount, currency_code]",
                  "product_name, category, quantity, unit_price, total_amount, currency]"),
                 ("      currency_code:   {type: string,    nullable: false, standardise: upper,\n",
                  "      currency:        {type: string,    nullable: false, standardise: upper, rename: currency_code,\n")],
        "py": [("    df = pd.read_csv(path, dtype=str, keep_default_na=False)\n",
                "    df = pd.read_csv(path, dtype=str, keep_default_na=False)\n"
                "    df = df.rename(columns={\"currency\": \"currency_code\"})\n")],
        "data": "rename",
    },
    "S2 optional column becomes mandatory (discount_amount)": {
        "yaml": [("[order_id, line_number, order_ts, customer_id, product_sku,\n",
                  "[order_id, line_number, order_ts, customer_id, product_sku, discount_amount,\n"),
                 ("discount_amount: {type: decimal,   nullable: true,  min: 0, default: 0}",
                  "discount_amount: {type: decimal,   nullable: false, min: 0}")],
        "py": [("            \"category\", \"quantity\", \"unit_price\", \"total_amount\", \"currency_code\"]",
                "            \"category\", \"quantity\", \"unit_price\", \"total_amount\", \"currency_code\", \"discount_amount\"]"),
               ("                          \"discount_amount\", \"shipping_amount\"]",
                "                          \"shipping_amount\"]"),
               ("    for c in [\"discount_amount\", \"shipping_amount\"]:\n        df[c] = df[c].where",
                "    for c in [\"shipping_amount\"]:\n        df[c] = df[c].where")],
        "data": "discount_null",
    },
    "S3 new domain constraint (line_number <= 32767)": {
        "yaml": [("{type: integer,   nullable: false, min: 1}\n      order_ts",
                  "{type: integer,   nullable: false, min: 1, max: 32767}\n      order_ts")],
        "py": [("    flag(out[\"line_number\"] < 1, \"DOMAIN_VIOLATION\", \"line_number: below minimum 1\")\n",
                "    flag(out[\"line_number\"] < 1, \"DOMAIN_VIOLATION\", \"line_number: below minimum 1\")\n"
                "    flag(out[\"line_number\"] > 32767, \"DOMAIN_VIOLATION\", \"line_number: above maximum 32767\")\n")],
        "data": "overflow",
    },
    "S4 onboard a new source (returns)": {
        "yaml_append": RETURNS_YAML,
        "py_new": HERE / "baseline_returns.py",
        "data": "returns",
    },
}


def code_lines(text):
    return [l for l in text.splitlines() if l.strip() and not l.strip().startswith("#")]


def diff_stat(a, b, python=False):
    a_l, b_l = (code_lines(a), code_lines(b)) if python else (a.splitlines(), b.splitlines())
    add = rem = 0
    for line in difflib.unified_diff(a_l, b_l, lineterm="", n=0):
        if line.startswith("+") and not line.startswith("+++"):
            add += 1
        elif line.startswith("-") and not line.startswith("---"):
            rem += 1
    return add, rem


def apply(text, edits):
    for old, new in edits:
        assert text.count(old) == 1, f"edit anchor not unique/found: {old[:60]!r}"
        text = text.replace(old, new)
    return text


def scenario_data(kind, folder):
    with open(TIER, newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    head, body = rows[0], [r for r in rows[1:201]]
    clean = [r for r in body if r[11] not in ("", "two") and r[16] != "XYZ"][:100]
    if kind == "rename":
        head = [("currency" if h == "currency_code" else h) for h in head]
    elif kind == "discount_null":
        for r in clean[:5]:
            r[13] = ""
    elif kind == "overflow":
        clean[3][1] = "40000"
    if kind == "returns":
        head = ["return_id", "order_id", "line_number", "returned_at", "product_sku", "quantity",
                "refund_amount", "currency", "reason"]
        clean = [[f"R{i:06d}", r[0], r[1], "2025-12-01T10:00:00Z", r[8], r[11], r[15], r[16].lower(),
                  "damaged" if i % 2 else "changed_mind"] for i, r in enumerate(clean)]
        clean[7][8] = "lost"                     # invalid reason -> DOMAIN_VIOLATION
        name = "returns_001.csv"
    else:
        name = "orders_001.csv"
    (folder / "data" / "inbox").mkdir(parents=True, exist_ok=True)
    with open(folder / "data" / "inbox" / name, "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows([head] + clean)
    return len(clean)


def reset_db(extra_ddl=""):
    recreate_db("rq3", extra_ddl)


def outcome(folder, cmd):
    p = subprocess.run(cmd, cwd=folder, env=ENV, capture_output=True, text=True)
    log = query("rq3", "SELECT status, rows_read, rows_inserted, rows_quarantined, coalesce(message,'') "
                       "FROM etl.pipeline_logs ORDER BY log_id DESC LIMIT 1")
    res = "|".join(str(v) for v in log[0]) if log else ""
    codes = query("rq3", "SELECT coalesce(string_agg(DISTINCT error_code, ','), '') FROM etl.quarantine_staging")[0][0]
    return {"exit": p.returncode, "log": res, "quarantine_codes": codes}


def main():
    work = HERE / "work"
    results = []
    for name, sc in SCENARIOS.items():
        if "yaml_append" in sc:
            cfg_new = CFG_V0.rstrip("\n") + "\n" + sc["yaml_append"]
            y_add, y_rem = diff_stat(CFG_V0, cfg_new)
            new_script = sc["py_new"].read_text(encoding="utf-8")
            p_add, p_rem = len(code_lines(new_script)), 0
            ddl = RETURNS_DDL
        else:
            cfg_new = apply(CFG_V0, sc["yaml"])
            y_add, y_rem = diff_stat(CFG_V0, cfg_new)
            base_new = apply(BASE_V0, sc["py"])
            p_add, p_rem = diff_stat(BASE_V0, base_new, python=True)
            ddl = ""
        runs = {}
        for impl, version in [("yaml_v0", "v0"), ("yaml_changed", "new"), ("py_v0", "v0"), ("py_changed", "new")]:
            folder = work / name.split()[0] / impl
            shutil.rmtree(folder, ignore_errors=True)
            folder.mkdir(parents=True)
            n = scenario_data(sc["data"], folder)
            reset_db(ddl)
            if impl.startswith("yaml"):
                (folder / "cfg.yaml").write_text(CFG_V0 if version == "v0" else cfg_new, encoding="utf-8")
                cmd = [sys.executable, str(CH4 / "etl_engine.py"), "--config", "cfg.yaml", "--log-level", "ERROR"]
            else:
                if "py_new" in sc:
                    script = BASE_V0 if version == "v0" else sc["py_new"].read_text(encoding="utf-8")
                else:
                    script = BASE_V0 if version == "v0" else base_new
                (folder / "etl.py").write_text(script, encoding="utf-8")
                cmd = [sys.executable, "etl.py"]
            runs[impl] = outcome(folder, cmd)
        results.append({"scenario": name, "rows": n, "yaml_added": y_add, "yaml_removed": y_rem,
                        "py_added": p_add, "py_removed": p_rem, "runs": runs})
        print(json.dumps(results[-1], indent=1))
    (HERE.parent / "results").mkdir(exist_ok=True)
    (HERE.parent / "results" / "rq3_results.json").write_text(json.dumps(results, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
