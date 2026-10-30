#!/usr/bin/env python3
"""etl_engine.py - configuration-driven batch ETL for e-commerce data.

The engine contains no source-specific logic. Everything it does to a file
(contract, types, rules, pseudonymisation, dimension handling, target table)
is read from pipeline_config.yaml. Usage:

    ETL_PG_DSN="dbname=shop user=etl" ETL_PII_SALT="..." \
        python etl_engine.py --config pipeline_config.yaml

Exit codes: 0 all files loaded, 1 at least one file failed and was rolled
back, 2 run halted on a contract violation, 3 configuration error.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import psycopg2
import yaml
from psycopg2 import sql
from psycopg2.extras import Json, execute_batch, execute_values

log = logging.getLogger("etl_engine")

EXIT_OK, EXIT_SOURCE_FAILED, EXIT_CONTRACT, EXIT_CONFIG = 0, 1, 2, 3

ERROR_CODES = {
    "NULL_VIOLATION", "TYPE_ERROR", "DOMAIN_VIOLATION", "DUPLICATE_KEY",
    "ARITHMETIC_MISMATCH", "NEGATIVE_AMOUNT", "INVALID_CURRENCY",
    "JSON_PARSE_ERROR",
}
PG_TYPES = {"string": "TEXT", "integer": "BIGINT", "decimal": "NUMERIC",
            "timestamp": "TIMESTAMPTZ"}
PII_ACTIONS = {"hash", "drop", "truncate"}
OFFSET_RE = r"(?:Z|[+-]\d{2}:?\d{2})$"


class ConfigError(Exception):
    """The configuration is malformed or refers to objects that do not exist."""


class ContractViolation(Exception):
    """A source file lacks a mandatory column; the run must halt."""


# =====================================================================
# 1. Configuration layer
# =====================================================================

def load_config(path: Path) -> dict:
    """Parse the YAML file and check its internal consistency."""
    with open(path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)          # plain data types only, no object construction
    validate_config(cfg)
    return cfg


def validate_config(cfg: dict) -> None:
    problems: list[str] = []
    for key in ("pipeline", "defaults", "sources"):
        if key not in cfg:
            problems.append(f"missing top-level section '{key}'")
    if (cfg.get("pipeline", {}).get("load", {}).get("transaction_scope", "per_source")) != "per_source":
        problems.append("load.transaction_scope: only 'per_source' (one transaction per file) is supported")
    for name, src in (cfg.get("sources") or {}).items():
        cols = src.get("columns", {})
        for col in src.get("mandatory_columns", []):
            if col not in cols:
                problems.append(f"{name}: mandatory column '{col}' is not declared")
        for col, spec in cols.items():
            if spec.get("type") not in (*PG_TYPES, "json"):
                problems.append(f"{name}.{col}: unsupported type '{spec.get('type')}'")
            if spec.get("error_code", "DOMAIN_VIOLATION") not in ERROR_CODES:
                problems.append(f"{name}.{col}: unknown error_code")
        for col, rule in src.get("pii", {}).items():
            if col not in cols:
                problems.append(f"{name}: pii field '{col}' is not declared")
            if rule.get("action") not in PII_ACTIONS:
                problems.append(f"{name}.{col}: unknown pii action")
        for jcol in src.get("json_fields", {}):
            if cols.get(jcol, {}).get("type") != "json":
                problems.append(f"{name}: json_fields entry '{jcol}' is not a json column")
        for rule in src.get("rules", []):
            if "expression" not in rule or rule.get("error_code") not in ERROR_CODES:
                problems.append(f"{name}: rule '{rule.get('name')}' needs an expression and a known error_code")
        if sum(1 for s in cols.values() if s.get("type") == "timestamp") != 1:
            problems.append(f"{name}: exactly one timestamp column is required (it drives date_key)")
    if problems:
        raise ConfigError("; ".join(problems))


def verify_targets(cur, cfg: dict) -> None:
    """Check that every target and dimension table exists in the database."""
    tables = set()
    for src in cfg["sources"].values():
        tables.add(src["target"])
        tables.update(src.get("dimensions", {}))
        tables.update(lk["dimension"] for lk in src.get("lookups", {}).values())
    for table in sorted(tables):
        cur.execute("SELECT to_regclass(%s)", (table,))
        if cur.fetchone()[0] is None:
            raise ConfigError(f"target table '{table}' does not exist")


def ts_column(src: dict) -> str:
    """Output name of the source's timestamp column (normalised to UTC)."""
    col, spec = next((c, s) for c, s in src["columns"].items() if s["type"] == "timestamp")
    return spec.get("rename", col) + "_utc"


def lookups_for(src: dict) -> dict[str, dict]:
    """Surrogate-key lookups: explicit 'lookups' plus those implied by 'dimensions'."""
    result = {}
    for dim, dspec in src.get("dimensions", {}).items():
        sk = dim.split(".")[-1].removeprefix("dim_") + "_sk"
        result[sk] = {"dimension": dim, "key": dspec["business_key"],
                      "at": dspec["effective_from"], "required": True}
    result.update(src.get("lookups", {}))
    return result


# =====================================================================
# 2. Keyed pseudonymisation (HMAC-SHA-256)
# =====================================================================

class Pseudonymiser:
    def __init__(self, salt_env: str):
        salt = os.environ.get(salt_env)
        if not salt:
            raise ConfigError(f"environment variable {salt_env} is not set")
        self._key = salt.encode("utf-8")

    def digest(self, value, lower: bool = False):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).strip()
        if lower:
            text = text.lower()
        return hmac.new(self._key, text.encode("utf-8"), hashlib.sha256).hexdigest()

    def apply(self, frame: pd.DataFrame, pii: dict) -> pd.DataFrame:
        """Return a copy in which no configured personal-data field survives in clear."""
        out = frame.copy()
        for col, rule in pii.items():
            if col not in out.columns:
                continue
            action, target = rule["action"], rule.get("output", col)
            if action == "hash":
                lower = rule.get("normalise") == "lower"
                out[target] = out[col].map(lambda v: self.digest(v, lower))
            elif action == "truncate":
                n = int(rule["length"])
                out[target] = out[col].map(
                    lambda v: None if v is None else str(v).strip().upper()[:n])
            if action == "drop" or target != col:
                out = out.drop(columns=col)
        return out


# =====================================================================
# 3. Extraction and contract check
# =====================================================================

def read_source(path: Path, src: dict, defaults: dict) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False,
                        sep=src.get("delimiter", defaults["delimiter"]),
                        encoding=src.get("encoding", defaults["encoding"]))
    frame.columns = [c.strip() for c in frame.columns]
    missing = [c for c in src["mandatory_columns"] if c not in frame.columns]
    if missing:
        raise ContractViolation(f"{path.name}: missing mandatory column(s) {', '.join(missing)}")
    extra = [c for c in frame.columns if c not in src["columns"]]
    if extra:
        log.warning("%s: ignoring undeclared column(s) %s", path.name, ", ".join(extra))
    for col in src["columns"]:                       # optional columns may be absent
        if col not in frame.columns:
            frame[col] = ""
    frame = frame[list(src["columns"])].astype(object)
    frame = frame.where(frame.apply(lambda s: s.str.strip() != ""), None)
    frame.index = pd.RangeIndex(1, len(frame) + 1)   # record number, header excluded
    return frame


# =====================================================================
# 4. Validation and quarantine routing
# =====================================================================

@dataclass
class Validation:
    typed: pd.DataFrame
    types: dict[str, str]
    code: pd.Series
    message: pd.Series

    def flag(self, mask: pd.Series, code: str, message) -> None:
        """Record an error for rows that do not already have one (first error wins)."""
        sel = mask.fillna(False).astype(bool) & self.code.isna()
        if sel.any():
            self.code[sel] = code
            self.message[sel] = message[sel] if isinstance(message, pd.Series) else message

    @property
    def valid(self) -> pd.Series:
        return self.code.isna()


def _parse_timestamps(raw: pd.Series, fmt: str, tz: str) -> pd.Series:
    out = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns, UTC]")
    present = raw.notna()
    if not present.any():
        return out
    values = raw[present].astype(str).str.strip()
    if fmt == "epoch_seconds":
        secs = pd.to_numeric(values, errors="coerce")
        out[present] = pd.to_datetime(secs, unit="s", utc=True)
        return out
    if fmt == "iso8601":
        aware = values.str.contains(OFFSET_RE, regex=True)
        if aware.any():
            out[aware[aware].index] = pd.to_datetime(
                values[aware], format="ISO8601", utc=True, errors="coerce")
        naive_vals = values[~aware]
        naive = pd.to_datetime(naive_vals, format="ISO8601", errors="coerce")
    else:
        naive_vals = values
        naive = pd.to_datetime(values, format=fmt, errors="coerce")
    if len(naive_vals):
        localised = naive.dt.tz_localize(tz, ambiguous="NaT", nonexistent="NaT")
        out[naive_vals.index] = localised.dt.tz_convert("UTC")
    return out


def check_column(v: Validation, raw: pd.Series, name: str, out_name: str,
                 spec: dict, tz: str, withhold: bool, json_code: str | None = None):
    """Type-convert one column, flag failures, and store the typed result."""
    def msg(problem: str) -> pd.Series | str:
        if withhold:                                   # never echo personal data
            return f"{name}: {problem} (value withheld)"
        return f"{name}: {problem} (value '" + raw.astype(str) + "')"

    if spec.get("standardise") in ("upper", "lower"):
        raw = raw.map(lambda s: None if s is None else getattr(str(s).strip(), spec["standardise"])())
    if not spec.get("nullable", True):
        v.flag(raw.isna(), "NULL_VIOLATION", f"{name}: required value missing")
    elif "default" in spec:
        raw = raw.where(raw.notna(), str(spec["default"]))

    kind, present = spec["type"], raw.notna()
    if kind == "string":
        typed = raw
    elif kind in ("integer", "decimal"):
        num = pd.to_numeric(raw, errors="coerce")
        bad = present & num.isna()
        if kind == "integer":
            bad |= present & num.notna() & (num % 1 != 0)
        v.flag(bad, "TYPE_ERROR", msg(f"not a valid {kind}"))
        typed = num.round().astype("Int64") if kind == "integer" else num
    elif kind == "timestamp":
        typed = _parse_timestamps(raw, spec.get("format", "iso8601"), tz)
        v.flag(present & typed.isna(), "TYPE_ERROR", msg("not a valid timestamp"))
        out_name = out_name + "_utc"
    elif kind == "json":
        def parse(s):
            try:
                obj = json.loads(s)
                return obj if isinstance(obj, dict) else None
            except (TypeError, ValueError):
                return None
        typed = raw.map(lambda s: None if s is None else parse(s))
        v.flag(present & typed.isna(), json_code or "JSON_PARSE_ERROR",
               f"{name}: not a valid JSON object")
        return typed                                   # flattened by the caller, not stored

    ok = typed.notna()
    if "min" in spec:
        v.flag(ok & (typed < spec["min"]), "DOMAIN_VIOLATION", msg(f"below minimum {spec['min']}"))
    if "max" in spec:
        v.flag(ok & (typed > spec["max"]), "DOMAIN_VIOLATION", msg(f"above maximum {spec['max']}"))
    if "pattern" in spec:
        v.flag(ok & ~raw.astype(str).str.fullmatch(spec["pattern"]), "DOMAIN_VIOLATION",
               msg("does not match the required pattern"))
    if "allowed" in spec:
        v.flag(ok & ~typed.isin(spec["allowed"]), spec.get("error_code", "DOMAIN_VIOLATION"),
               msg("not an allowed value"))
    v.typed[out_name] = typed
    v.types[out_name] = kind


def validate(frame: pd.DataFrame, src: dict, defaults: dict) -> Validation:
    v = Validation(typed=pd.DataFrame(index=frame.index), types={},
                   code=pd.Series(None, index=frame.index, dtype=object),
                   message=pd.Series(None, index=frame.index, dtype=object))
    tz = src.get("timezone", defaults["timezone"])
    pii = src.get("pii", {})
    for col, spec in src["columns"].items():
        parsed = check_column(v, frame[col], col, spec.get("rename", col), spec, tz,
                              withhold=col in pii,
                              json_code=src.get("json_fields", {}).get(col, {}).get("on_parse_error"))
        if spec["type"] == "json":
            for out, fspec in src.get("json_fields", {}).get(col, {}).get("flatten", {}).items():
                values = parsed.map(lambda d, p=fspec["path"]:
                                    None if not isinstance(d, dict) or d.get(p) is None else str(d[p]))
                check_column(v, values, f"{col}.{fspec['path']}", out, fspec, tz, withhold=False)

    for rule in src.get("rules", []):                  # record-level rules
        rows = v.typed[v.valid].astype({c: "float64" for c, t in v.types.items()
                                        if t in ("integer", "decimal")})
        if rows.empty:
            continue
        applies = rows.eval(rule["when"]) if "when" in rule else pd.Series(True, index=rows.index)
        passed = rows.eval(rule["expression"])
        failed = (applies & ~passed).reindex(frame.index, fill_value=False)
        v.flag(failed, rule["error_code"], f"rule '{rule['name']}' violated")

    keys = [src["columns"].get(k, {}).get("rename", k) for k in src["business_key"]]
    dup = v.typed.loc[v.valid, keys].duplicated(keep="first").reindex(frame.index, fill_value=False)
    v.flag(dup, "DUPLICATE_KEY", "duplicate business key within file (first occurrence kept)")
    return v


# =====================================================================
# 5. Loading (single transaction per file)
# =====================================================================

def _py(value, kind: str | None):
    """Convert a pandas/numpy value into a psycopg2-adaptable Python object."""
    if value is None or (not isinstance(value, (dict, list)) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if kind == "integer":
        return int(value)
    if kind == "decimal":
        return Decimal(str(value))
    return value


def stage_rows(cur, frame: pd.DataFrame, types: dict[str, str], batch_size: int) -> list[str]:
    """Copy validated, pseudonymised rows into a session-local staging table."""
    cols = list(frame.columns)
    cur.execute(sql.SQL("CREATE TEMP TABLE stg (_rn INTEGER PRIMARY KEY, {}) ON COMMIT DROP").format(
        sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(PG_TYPES.get(types.get(c), "TEXT")))
                           for c in cols)))
    rows = [(int(rn), *(_py(val, types.get(c)) for c, val in zip(cols, rec)))
            for rn, rec in zip(frame.index, frame.itertuples(index=False, name=None))]
    execute_values(cur, sql.SQL("INSERT INTO stg (_rn, {}) VALUES %s").format(
        sql.SQL(", ").join(map(sql.Identifier, cols))).as_string(cur), rows, page_size=batch_size)
    return cols


def sync_dimension(cur, frame: pd.DataFrame, dim: str, dspec: dict, at: str, batch_size: int) -> None:
    """Apply SCD Type 2 (versioned) and Type 1 (overwritten) changes from one file."""
    bk, scd2 = dspec["business_key"], dspec.get("scd2_attributes", [])
    t1 = dspec.get("type1_attributes", [])
    sk = dim.split(".")[-1].removeprefix("dim_") + "_sk"
    table = sql.Identifier(*dim.split("."))
    rows = frame[[bk, at, *scd2, *t1]].dropna(subset=[bk]).sort_values(at, kind="stable")
    if rows.empty:
        return
    probe = rows[scd2].fillna("\x00")
    rows = rows[probe.ne(probe.groupby(rows[bk]).shift()).any(axis=1)]   # attribute changes only

    cur.execute(sql.SQL("SELECT {sk}, {bk}, {attrs}, valid_from FROM {t} WHERE is_current AND {bk} = ANY(%s)").format(
        sk=sql.Identifier(sk), bk=sql.Identifier(bk), t=table,
        attrs=sql.SQL(", ").join(map(sql.Identifier, scd2))), (rows[bk].unique().tolist(),))
    current = {r[1]: (r[0], tuple(r[2:-1]), r[-1]) for r in cur.fetchall()}

    closes, inserts, late = [], [], 0
    for key, grp in rows.groupby(bk, sort=False):
        chain: list[list] = []                            # [attrs, effective_from]
        for rec in grp.itertuples(index=False):
            attrs = tuple(_py(getattr(rec, a), None) for a in scd2)
            eff = getattr(rec, at).to_pydatetime()
            if chain and chain[-1][1] == eff:
                chain[-1][0] = attrs                      # same instant: last value wins
            elif not chain or chain[-1][0] != attrs:
                chain.append([attrs, eff])
        if key in current:
            cur_sk, cur_attrs, cur_from = current[key]
            if cur_from.tzinfo is not None:               # '-infinity' arrives as a naive datetime.min
                late += sum(1 for _, eff in chain if eff <= cur_from)
                chain = [c for c in chain if c[1] > cur_from]
            while chain and chain[0][0] == cur_attrs:
                chain.pop(0)
            if not chain:
                continue
            closes.append((chain[0][1], cur_sk))
        else:
            chain[0][1] = "-infinity"                     # first version covers all history
        for i, (attrs, eff) in enumerate(chain):
            last = i == len(chain) - 1
            inserts.append((key, *attrs, eff, "infinity" if last else chain[i + 1][1], last))

    if closes:
        execute_batch(cur, sql.SQL("UPDATE {t} SET valid_to = %s, is_current = FALSE WHERE {sk} = %s").format(
            t=table, sk=sql.Identifier(sk)).as_string(cur), closes, page_size=batch_size)
    if inserts:
        execute_values(cur, sql.SQL("INSERT INTO {t} ({bk}, {attrs}, valid_from, valid_to, is_current) VALUES %s").format(
            t=table, bk=sql.Identifier(bk), attrs=sql.SQL(", ").join(map(sql.Identifier, scd2))).as_string(cur),
            inserts, page_size=batch_size)
    for attr in t1:                                       # Type 1: overwrite on all versions
        latest = frame[[bk, attr]].dropna().groupby(bk)[attr].last()
        if not latest.empty:
            execute_values(cur, sql.SQL("UPDATE {t} d SET {a} = v.val FROM (VALUES %s) AS v(key, val) "
                                        "WHERE d.{bk} = v.key AND d.{a} IS DISTINCT FROM v.val").format(
                t=table, a=sql.Identifier(attr), bk=sql.Identifier(bk)).as_string(cur),
                list(latest.items()), page_size=batch_size)
    if late:
        log.warning("%s: %d attribute change(s) older than the current version were not applied", dim, late)
    log.info("%s: %d version(s) closed, %d inserted", dim, len(closes), len(inserts))


def insert_facts(cur, src: dict, staged: list[str], run_id: str) -> tuple[int, dict[int, tuple[str, str]]]:
    """INSERT ... SELECT from staging with surrogate-key lookups and ON CONFLICT DO NOTHING.

    Returns the number of rows inserted and, for every staged row that was not
    inserted, the (error_code, message) explaining why.
    """
    schema, table = src["target"].split(".")
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, table))
    target_cols = [r[0] for r in cur.fetchall()]
    ts = sql.Identifier(ts_column(src))
    keys = [src["columns"].get(k, {}).get("rename", k) for k in src["business_key"]]
    lookups = lookups_for(src)

    joins = [sql.SQL("JOIN dwh.dim_date dd ON dd.date_key = to_char(s.{} AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER").format(ts)]
    select, cols = [], []
    for col in target_cols:
        if col == "run_id":
            expr = sql.Literal(run_id) + sql.SQL("::UUID")
        elif col == "date_key":
            expr = sql.SQL("dd.date_key")
        elif col in lookups:
            lk = lookups[col]
            alias = sql.Identifier("l_" + col)
            at = sql.Identifier(src["columns"].get(lk["at"], {}).get("rename", lk["at"]) + "_utc")
            joins.append(sql.SQL("{kind} JOIN {dim} {a} ON {a}.{key} = s.{key} AND s.{at} >= {a}.valid_from "
                                 "AND s.{at} < {a}.valid_to").format(
                kind=sql.SQL("" if lk.get("required") else "LEFT"), dim=sql.Identifier(*lk["dimension"].split(".")),
                a=alias, key=sql.Identifier(lk["key"]), at=at))
            expr = sql.SQL("{}.{}").format(alias, sql.Identifier(col))
        elif col in staged:
            expr = sql.SQL("s.{}").format(sql.Identifier(col))
        else:
            continue                                      # column has a default or is nullable
        cols.append(sql.Identifier(col))
        select.append(expr)

    cur.execute(sql.SQL("INSERT INTO {t} ({cols}) SELECT {sel} FROM stg s {joins} "
                        "ON CONFLICT DO NOTHING RETURNING {keys}").format(
        t=sql.Identifier(schema, table), cols=sql.SQL(", ").join(cols), sel=sql.SQL(", ").join(select),
        joins=sql.SQL(" ").join(joins), keys=sql.SQL(", ").join(map(sql.Identifier, keys))))
    inserted = {tuple(map(str, r)) for r in cur.fetchall()}

    # Classify staged rows that were not inserted: already in the warehouse, or unresolved lookup/date.
    cur.execute(sql.SQL("SELECT s._rn, ({ks})::TEXT[], EXISTS (SELECT 1 FROM {t} f WHERE {match}) FROM stg s").format(
        ks=sql.SQL("ARRAY[{}]").format(sql.SQL(", ").join(sql.SQL("s.{}::TEXT").format(sql.Identifier(k)) for k in keys)),
        t=sql.Identifier(schema, table),
        match=sql.SQL(" AND ").join(sql.SQL("f.{k} = s.{k}").format(k=sql.Identifier(k)) for k in keys)))
    rejected = {}
    for rn, key_values, exists in cur.fetchall():
        if tuple(key_values) in inserted:
            continue
        rejected[rn] = (("DUPLICATE_KEY", "business key already present in the warehouse") if exists else
                        ("DOMAIN_VIOLATION", "date outside dim_date or no dimension version valid at event time"))
    return len(inserted), rejected


def write_quarantine(cur, rows: list[tuple], batch_size: int) -> None:
    execute_values(cur, "INSERT INTO etl.quarantine_staging (run_id, log_id, source_name, source_file, "
                        "source_row_number, error_code, error_message, raw_record) VALUES %s",
                   rows, page_size=batch_size)


# =====================================================================
# 6. Orchestration, logging and archiving
# =====================================================================

def process_file(conn, path: Path, name: str, src: dict, cfg: dict, pseudo: Pseudonymiser, run_id: str) -> None:
    batch = int(cfg["pipeline"]["load"]["batch_size"])
    started, t0 = datetime.now(timezone.utc), time.perf_counter()
    with conn.cursor() as cur:                            # log entry committed on its own
        cur.execute("INSERT INTO etl.pipeline_logs (run_id, source_name, source_file, started_at, status) "
                    "VALUES (%s, %s, %s, %s, 'RUNNING') RETURNING log_id", (run_id, name, path.name, started))
        log_id = cur.fetchone()[0]
    conn.commit()

    rows_read = 0
    try:
        raw = read_source(path, src, cfg["defaults"])      # may raise ContractViolation
        rows_read = len(raw)
        v = validate(raw, src, cfg["defaults"])
        pii = src.get("pii", {})
        safe_raw = pseudo.apply(raw, pii)                  # pseudonymise BEFORE any write
        typed = pseudo.apply(v.typed[v.valid], pii)
        types = {**v.types, **{r.get("output", c): "string" for c, r in pii.items()}}
        quarantine = [(rn, v.code[rn], v.message[rn]) for rn in raw.index[~v.valid]]

        with conn.cursor() as cur:                         # data transaction for this file
            for dim, dspec in src.get("dimensions", {}).items():
                at = src["columns"][dspec["effective_from"]].get("rename", dspec["effective_from"]) + "_utc"
                sync_dimension(cur, typed, dim, dspec, at, batch)
            inserted = 0
            if not typed.empty:
                staged = stage_rows(cur, typed, types, batch)
                inserted, rejected = insert_facts(cur, src, staged, run_id)
                quarantine += [(rn, code, msg) for rn, (code, msg) in rejected.items()]
            records = safe_raw.astype(object).where(safe_raw.notna(), None)
            write_quarantine(cur, [(run_id, log_id, name, path.name, int(rn), code, msg,
                                    Json(records.loc[rn].to_dict())) for rn, code, msg in quarantine], batch)
            if rows_read != inserted + len(quarantine):
                raise RuntimeError(f"reconciliation failed: {rows_read} != {inserted} + {len(quarantine)}")
            cur.execute("UPDATE etl.pipeline_logs SET status = 'SUCCESS', finished_at = %s, execution_time = %s, "
                        "rows_read = %s, rows_inserted = %s, rows_quarantined = %s WHERE log_id = %s",
                        (datetime.now(timezone.utc), round(time.perf_counter() - t0, 3),
                         rows_read, inserted, len(quarantine), log_id))
        conn.commit()                                      # data, quarantine and log commit together
        log.info("%s: read %d, inserted %d, quarantined %d", path.name, rows_read, inserted, len(quarantine))
    except Exception as exc:
        conn.rollback()                                    # nothing from this file persists
        if isinstance(exc, psycopg2.Error) and exc.diag.message_primary:
            reason = f"{exc.pgcode}: {exc.diag.message_primary}"   # no DETAIL: it can contain row values
        else:
            reason = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, ContractViolation):
            reason = f"CONTRACT_VIOLATION: {exc}"
        with conn.cursor() as cur:
            cur.execute("UPDATE etl.pipeline_logs SET status = 'FAILED', finished_at = %s, execution_time = %s, "
                        "rows_read = %s, rows_inserted = 0, rows_quarantined = 0, message = %s WHERE log_id = %s",
                        (datetime.now(timezone.utc), round(time.perf_counter() - t0, 3),
                         rows_read, reason[:1000], log_id))
        conn.commit()
        log.error("%s: FAILED - %s", path.name, reason)
        raise

    archive_dir = Path(cfg["pipeline"]["paths"]["archive"])
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    shutil.move(str(path), archive_dir / f"{stamp}_{path.name}")


def run(config_path: Path) -> int:
    try:
        cfg = load_config(config_path)
        pseudo = Pseudonymiser(cfg["pipeline"]["pii"]["salt_env"])
        dsn = os.environ.get(cfg["pipeline"]["database"]["dsn_env"])
        if not dsn:
            raise ConfigError(f"environment variable {cfg['pipeline']['database']['dsn_env']} is not set")
        conn = psycopg2.connect(dsn)
        with conn.cursor() as cur:
            verify_targets(cur, cfg)
        conn.commit()
    except (ConfigError, yaml.YAMLError, OSError) as exc:
        log.error("configuration error: %s", exc)
        return EXIT_CONFIG

    run_id, status = str(uuid.uuid4()), EXIT_OK
    inbox = Path(cfg["pipeline"]["paths"]["inbox"])
    log.info("run %s started", run_id)
    try:
        for name, src in cfg["sources"].items():          # order matters: dimensions before lookups
            for path in sorted(inbox.glob(src["file_pattern"])):
                try:
                    process_file(conn, path, name, src, cfg, pseudo, run_id)
                except ContractViolation:
                    log.critical("run %s halted: contract violation in %s", run_id, path.name)
                    return EXIT_CONTRACT
                except Exception:
                    status = EXIT_SOURCE_FAILED            # file stays in inbox for a retry
    finally:
        conn.close()
    log.info("run %s finished with exit code %d", run_id, status)
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description="Configuration-driven batch ETL")
    parser.add_argument("--config", type=Path, default=Path("pipeline_config.yaml"))
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(run(args.config))


if __name__ == "__main__":
    main()
