from pathlib import Path

import pandas as pd

from config import (
    AGING_BUCKET_RULES,
    AGING_REPORT_DATE,
    ALLOWED_BANK_DEBIT_CREDIT,
    BANK_FILE,
    BANK_REQUIRED_COLUMNS,
    CURRENCY_SCOPE,
    ENTITY_SCOPE,
    LEDGER_FILE,
    LEDGER_REQUIRED_COLUMNS,
    OBLIGATIONS_FILE,
    OBLIGATIONS_REQUIRED_COLUMNS,
    PERIOD_END_DATE,
    PERIOD_START_DATE,
)


def load_csv_file(file_path: Path) -> pd.DataFrame:
    """Load a CSV file as a raw source dataframe."""
    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError(f"Source file is empty: {file_path.name}")

    return df


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: list[str],
    file_name: str,
) -> None:
    """Validate that all required columns exist in the dataframe."""
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Missing required columns in {file_name}: {missing_columns}"
        )

    if df.columns.duplicated().any():
        duplicated = df.columns[df.columns.duplicated()].tolist()
        raise ValueError(
            f"Duplicate column names found in {file_name}: {duplicated}"
        )


def _require_single_value(df: pd.DataFrame, column_name: str, expected_value: str, file_name: str) -> None:
    values = sorted({str(value).strip() for value in df[column_name].dropna().unique()})
    if values != [expected_value]:
        raise ValueError(
            f"{file_name} must contain one {column_name} value of {expected_value}; found {values}"
        )


def _parse_dates(series: pd.Series, column_name: str, file_name: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().any():
        raise ValueError(f"{file_name} contains invalid dates in {column_name}")
    return parsed


def _parse_numeric(series: pd.Series, column_name: str, file_name: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any():
        raise ValueError(f"{file_name} contains invalid numeric values in {column_name}")
    return numeric.round(2)


def _validate_period_columns(
    df: pd.DataFrame,
    start_column: str,
    end_column: str,
    file_name: str,
) -> None:
    start_dates = _parse_dates(df[start_column], start_column, file_name).dt.normalize()
    end_dates = _parse_dates(df[end_column], end_column, file_name).dt.normalize()
    expected_start = pd.Timestamp(PERIOD_START_DATE)
    expected_end = pd.Timestamp(PERIOD_END_DATE)

    if not start_dates.eq(expected_start).all():
        raise ValueError(f"{file_name} must be scoped to {PERIOD_START_DATE} in {start_column}")

    if not end_dates.eq(expected_end).all():
        raise ValueError(f"{file_name} must be scoped to {PERIOD_END_DATE} in {end_column}")


def _validate_month_dates(df: pd.DataFrame, date_column: str, file_name: str) -> None:
    dates = _parse_dates(df[date_column], date_column, file_name).dt.normalize()
    start = pd.Timestamp(PERIOD_START_DATE)
    end = pd.Timestamp(PERIOD_END_DATE)

    if not dates.between(start, end).all():
        raise ValueError(f"{file_name} contains out-of-period dates in {date_column}")


def _validate_rolling_balance(
    df: pd.DataFrame,
    opening_balance_column: str,
    amount_column: str,
    balance_column: str,
    date_column: str,
    file_name: str,
) -> None:
    working = df.copy()
    working["_sort_date"] = _parse_dates(working[date_column], date_column, file_name)
    working["_amount"] = _parse_numeric(working[amount_column], amount_column, file_name)
    working["_balance"] = _parse_numeric(working[balance_column], balance_column, file_name)
    opening_balance = _parse_numeric(working[opening_balance_column], opening_balance_column, file_name)

    working = working.sort_values(["_sort_date"]).reset_index(drop=True)
    expected_balance = (opening_balance.iloc[0] + working["_amount"].cumsum()).round(2)

    if not expected_balance.equals(working["_balance"].round(2)):
        raise ValueError(f"{file_name} failed rolling balance validation")


def _validate_bank_file(df: pd.DataFrame) -> None:
    file_name = BANK_FILE.name
    _require_single_value(df, "entity_name", ENTITY_SCOPE, file_name)
    _require_single_value(df, "currency", CURRENCY_SCOPE, file_name)
    _validate_period_columns(df, "statement_period_start", "statement_period_end", file_name)
    _validate_month_dates(df, "transaction_date", file_name)
    valid_dc = df["debit_credit"].astype(str).str.strip().str.upper().isin(ALLOWED_BANK_DEBIT_CREDIT)
    if not valid_dc.all():
        raise ValueError(f"{file_name} contains unsupported bank debit/credit labels")

    signed_amount = _parse_numeric(df["amount"], "amount", file_name)
    signed_amount = signed_amount.where(
        df["debit_credit"].astype(str).str.strip().str.upper().eq("CREDIT"),
        -signed_amount,
    )
    _validate_rolling_balance(
        df.assign(_cash_effect=signed_amount),
        opening_balance_column="statement_opening_balance",
        amount_column="_cash_effect",
        balance_column="balance_after_txn",
        date_column="transaction_date",
        file_name=file_name,
    )


def _validate_ledger_file(df: pd.DataFrame) -> None:
    file_name = LEDGER_FILE.name
    _require_single_value(df, "entity_name", ENTITY_SCOPE, file_name)
    _require_single_value(df, "currency", CURRENCY_SCOPE, file_name)
    _validate_period_columns(df, "ledger_period_start", "ledger_period_end", file_name)
    _validate_month_dates(df, "posting_date", file_name)

    debit_amount = _parse_numeric(df["debit_amount"], "debit_amount", file_name)
    credit_amount = _parse_numeric(df["credit_amount"], "credit_amount", file_name)
    cash_effect = (debit_amount - credit_amount).round(2)

    if cash_effect.eq(0).any():
        raise ValueError(f"{file_name} contains zero-net cash rows that are not valid for V1 cash-side matching")

    _validate_rolling_balance(
        df.assign(_cash_effect=cash_effect),
        opening_balance_column="ledger_opening_balance",
        amount_column="_cash_effect",
        balance_column="balance_after_posting",
        date_column="posting_date",
        file_name=file_name,
    )


def _expected_aging_bucket(days_past_due: int) -> str:
    for bucket, rule in AGING_BUCKET_RULES:
        if rule(days_past_due):
            return bucket
    return "UNMAPPED"


def _validate_obligations_file(df: pd.DataFrame) -> None:
    file_name = OBLIGATIONS_FILE.name
    _require_single_value(df, "entity_name", ENTITY_SCOPE, file_name)
    _require_single_value(df, "currency", CURRENCY_SCOPE, file_name)

    aging_report_dates = _parse_dates(df["aging_report_date"], "aging_report_date", file_name).dt.normalize()
    if not aging_report_dates.eq(pd.Timestamp(AGING_REPORT_DATE)).all():
        raise ValueError(f"{file_name} must be an aging support file as of {AGING_REPORT_DATE}")

    original_amount = _parse_numeric(df["original_amount"], "original_amount", file_name)
    paid_amount = _parse_numeric(df["paid_amount"], "paid_amount", file_name)
    open_amount = _parse_numeric(df["open_amount"], "open_amount", file_name)
    expected_open_amount = (original_amount - paid_amount).round(2)

    if not expected_open_amount.equals(open_amount):
        raise ValueError(f"{file_name} failed open_amount = original_amount - paid_amount validation")

    days_past_due = pd.to_numeric(df["days_past_due"], errors="coerce")
    if days_past_due.isna().any():
        raise ValueError(f"{file_name} contains invalid days_past_due values")

    expected_buckets = days_past_due.astype(int).apply(_expected_aging_bucket)
    actual_buckets = df["aging_bucket"].astype(str).str.strip()
    if not expected_buckets.equals(actual_buckets):
        raise ValueError(f"{file_name} failed aging bucket validation against days_past_due")


def load_raw_sources() -> dict[str, pd.DataFrame]:
    """Load and validate all official raw source files."""
    raw_bank = load_csv_file(BANK_FILE)
    raw_ledger = load_csv_file(LEDGER_FILE)
    raw_obligations = load_csv_file(OBLIGATIONS_FILE)

    validate_required_columns(raw_bank, BANK_REQUIRED_COLUMNS, BANK_FILE.name)
    validate_required_columns(raw_ledger, LEDGER_REQUIRED_COLUMNS, LEDGER_FILE.name)
    validate_required_columns(
        raw_obligations,
        OBLIGATIONS_REQUIRED_COLUMNS,
        OBLIGATIONS_FILE.name,
    )

    _validate_bank_file(raw_bank)
    _validate_ledger_file(raw_ledger)
    _validate_obligations_file(raw_obligations)

    return {
        "raw_bank": raw_bank,
        "raw_ledger": raw_ledger,
        "raw_obligations": raw_obligations,
    }
