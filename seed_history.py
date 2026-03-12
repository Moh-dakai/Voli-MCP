"""
Seed local SQLite history store with 2+ years of daily session ranges.
Uses Alpha Vantage daily FX data and derives session ranges heuristically.
"""

from __future__ import annotations

import os
import sqlite3
import random
from datetime import datetime, timedelta
from typing import Dict, Optional

import pandas as pd

from src.data.alpha_vantage_client import AlphaVantageClient
from src.utils.formatters import price_to_pips


PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD"]
SESSIONS = ["asian", "london", "ny"]
DB_PATH = os.getenv("HISTORICAL_DB_PATH", os.path.join("data", "history.sqlite"))


def _is_first_friday(dt: datetime) -> bool:
    return dt.weekday() == 4 and 1 <= dt.day <= 7


def _is_mid_month(dt: datetime) -> bool:
    return 12 <= dt.day <= 15


def _third_wednesday(year: int, month: int) -> datetime:
    dt = datetime(year, month, 1)
    while dt.weekday() != 2:
        dt += timedelta(days=1)
    return dt + timedelta(days=14)


def _event_type_for_currency(currency: str, dt: datetime) -> Optional[str]:
    if currency == "USD":
        if _is_first_friday(dt):
            return "NFP"
        if _is_mid_month(dt):
            return "CPI"
        if dt.date() == _third_wednesday(dt.year, dt.month).date():
            return "FOMC"
    if currency == "EUR":
        if _is_mid_month(dt):
            return "ECB"
    if currency == "GBP":
        if _is_mid_month(dt):
            return "BOE"
    if currency == "JPY":
        if _is_mid_month(dt):
            return "BOJ"
    if currency == "AUD":
        if _is_mid_month(dt):
            return "RBA"
    return None


def _pick_event_type(base: str, quote: str, dt: datetime) -> Optional[str]:
    for currency in [base, quote]:
        event_type = _event_type_for_currency(currency, dt)
        if event_type:
            return event_type
    return None


def _daily_rows_from_fx_daily(pair: str, data: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    rows = []
    for date_str, values in data.items():
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        high = float(values["2. high"])
        low = float(values["3. low"])
        rows.append({"date": dt.date(), "high": high, "low": low})
    df = pd.DataFrame(rows)
    df.sort_values("date", inplace=True)
    return df


def _derive_session_ranges(pair: str, daily_df: pd.DataFrame) -> pd.DataFrame:
    rng = random.Random(42)
    base = pair[:3]
    quote = pair[3:]

    records = []
    for _, row in daily_df.iterrows():
        daily_range_pips = price_to_pips(row["high"] - row["low"], pair)
        if daily_range_pips <= 0:
            continue

        for session in SESSIONS:
            if session == "asian":
                weight = 0.25
            elif session == "london":
                weight = 0.40
            else:
                weight = 0.35

            session_range = max(daily_range_pips * weight * rng.uniform(0.8, 1.2), 8.0)
            pre_range = max(session_range * rng.uniform(0.35, 0.55), 5.0)

            event_type = _pick_event_type(base, quote, datetime.combine(row["date"], datetime.min.time()))
            has_event = 1 if event_type else 0

            records.append({
                "pair": pair,
                "session": session,
                "date": row["date"].isoformat(),
                "pre_range_pips": round(pre_range, 2),
                "session_range_pips": round(session_range, 2),
                "event_type": event_type,
                "has_event": has_event
            })

    df = pd.DataFrame(records)
    return df


def _add_rolling_averages(df: pd.DataFrame) -> pd.DataFrame:
    df["avg_pre_range_30d"] = (
        df.groupby(["pair", "session"])["pre_range_pips"]
        .transform(lambda s: s.rolling(30, min_periods=10).mean())
    )
    df["avg_session_range_30d"] = (
        df.groupby(["pair", "session"])["session_range_pips"]
        .transform(lambda s: s.rolling(30, min_periods=10).mean())
    )
    df["compression_ratio"] = df["pre_range_pips"] / df["avg_pre_range_30d"]
    df["compression_ratio"] = df["compression_ratio"].fillna(1.0)
    return df


def seed_history() -> None:
    client = AlphaVantageClient()

    all_rows = []
    for pair in PAIRS:
        base = pair[:3]
        quote = pair[3:]
        payload = client.get_fx_daily(base, quote, outputsize="full")
        ts = payload.get("Time Series FX (Daily)")
        if not ts:
            raise RuntimeError(f"Alpha Vantage returned no data for {pair}: {payload}")

        daily_df = _daily_rows_from_fx_daily(pair, ts)
        # Keep last 2 years
        cutoff = datetime.utcnow().date() - timedelta(days=730)
        daily_df = daily_df[daily_df["date"] >= cutoff]
        session_df = _derive_session_ranges(pair, daily_df)
        all_rows.append(session_df)

    combined = pd.concat(all_rows, ignore_index=True)
    combined = _add_rolling_averages(combined)

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS session_ranges (
                pair TEXT NOT NULL,
                session TEXT NOT NULL,
                date TEXT NOT NULL,
                pre_range_pips REAL NOT NULL,
                session_range_pips REAL NOT NULL,
                avg_pre_range_30d REAL,
                avg_session_range_30d REAL,
                compression_ratio REAL,
                event_type TEXT,
                has_event INTEGER DEFAULT 0,
                PRIMARY KEY (pair, session, date)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_session_ranges_pair_session_date "
            "ON session_ranges(pair, session, date)"
        )
        combined.to_sql("session_ranges", conn, if_exists="replace", index=False)

    print(f"Seeded {len(combined)} rows into {DB_PATH}")


if __name__ == "__main__":
    seed_history()
