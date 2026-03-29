import re
from decimal import Decimal, InvalidOperation

import pandas as pd

from config import (
    ALLOWED_BANK_DEBIT_CREDIT,
    ALLOWED_BANK_STANDARDIZED_DEBIT_CREDIT,
    ALLOWED_LEDGER_DEBIT_CREDIT,
    CURRENCY_SCOPE,
    ENTITY_SCOPE,
    REPORTING_MONTH,
)


def standardize_text(value):
    """Trim text and collapse repeated internal whitespace."""
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    text = re.sub(r"\s+", " ", text)

    if text.upper() in {"NULL", "N/A", "NA", "NONE"}:
        return None

    return text


def normalize_reference(value):
    """Normalize reference for comparison use."""
    text = standardize_text(value)
    if text is None:
        return None

    text = text.upper().replace(" ", "").replace("-", "")

    if text.isdigit():
        text = text.lstrip("0")

    return text or None


def normalize_name(value):
    """Normalize counterparty/vendor names for comparison."""
    text = standardize_text(value)
    if text is None:
        return None

    text = text.upper()
    text = re.sub(r"[&.,'/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def normalize_description(value):
    """Normalize free-text description for support use."""
    text = standardize_text(value)
    if text is None:
        return None

    text = text.upper()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def extract_ledger_counterparty(description):
    """Extract the business counterparty from the ledger description when present."""
    text = standardize_text(description)
    if text is None:
        return None

    if " - " in text:
        return text.rsplit(" - ", 1)[-1]

    return text


def parse_date(value):
    """Parse a raw date value into pandas datetime."""
    if pd.isna(value):
        return pd.NaT

    return pd.to_datetime(value, errors="coerce")


def to_amount_numeric(value):
    """Convert raw amount to rounded numeric."""
    if pd.isna(value):
        return None

    try:
        amount = Decimal(str(value))
        return float(round(amount, 2))
    except (InvalidOperation, ValueError):
        return None


def derive_bank_cash_effect(amount_numeric, debit_credit):
    """Preserve bank statement convention: CR is inflow and DR is outflow."""
    if amount_numeric is None or pd.isna(amount_numeric):
        return None

    dc = standardize_text(debit_credit)
    if dc not in ALLOWED_BANK_DEBIT_CREDIT:
        return None

    if dc == "CREDIT":
        return float(amount_numeric)

    if dc == "DEBIT":
        return float(-amount_numeric)

    return None


def standardize_bank_debit_credit(value):
    mapping = {
        "CREDIT": "CR",
        "DEBIT": "DR",
    }
    return mapping.get(standardize_text(value))


def derive_ledger_debit_credit(debit_amount, credit_amount):
    if pd.isna(debit_amount) or pd.isna(credit_amount):
        return None
    if float(debit_amount) > 0:
        return "DR"
    return "CR"


def _date_to_iso_string(series):
    """Convert datetime series to YYYY-MM-DD string."""
    return series.dt.strftime("%Y-%m-%d").where(series.notna(), None)


def _month_in_scope(date_series):
    """Check whether dates fall within the reporting month."""
    return date_series.dt.strftime("%Y-%m").eq(REPORTING_MONTH).fillna(False)


def _initialize_duplicate_fields(df):
    """Initialize duplicate control fields."""
    df["duplicate_flag"] = "No"
    df["duplicate_group_id"] = None
    df["duplicate_review_required"] = "No"
    return df


def clean_bank_transactions(raw_bank):
    """Create the cleaned bank transactions dataset."""
    df = raw_bank.copy()

    df["source_file_name"] = "bank_transactions_sample.csv"
    df["bank_txn_id"] = df.index.to_series().add(1).map(lambda value: f"BANK-{value:04d}")
    df["source_record_id"] = df["bank_txn_id"]
    df["cleaning_status"] = "CLEAN"
    df["cleaning_notes"] = None

    txn_dt = df["transaction_date"].apply(parse_date)
    value_dt = df["value_date"].apply(parse_date)

    df["txn_date"] = txn_dt
    df["standardized_txn_date"] = _date_to_iso_string(txn_dt)
    df["standardized_value_date"] = _date_to_iso_string(value_dt)

    df["amount_numeric"] = df["amount"].apply(to_amount_numeric)
    df["bank_debit_credit"] = df["debit_credit"].apply(standardize_bank_debit_credit)
    df["signed_amount"] = df.apply(
        lambda row: derive_bank_cash_effect(row["amount_numeric"], row["debit_credit"]),
        axis=1,
    )
    df["bank_cash_effect"] = df["signed_amount"]

    df["reference_number"] = df["bank_reference"]
    df["bank_description"] = df["description"]
    df["normalized_reference"] = df["bank_reference"].apply(normalize_reference)
    df["normalized_counterparty"] = df["counterparty_name"].apply(normalize_name)
    df["normalized_counterparty_fallback"] = df["description"].apply(normalize_name)
    df["normalized_description"] = df["description"].apply(normalize_description)

    df["reference_usable"] = df["normalized_reference"].notna().map({True: "Yes", False: "No"})
    df["counterparty_usable"] = df["normalized_counterparty"].notna().map({True: "Yes", False: "No"})
    df["description_usable"] = df["normalized_description"].notna().map({True: "Yes", False: "No"})

    df["in_scope_month_flag"] = _month_in_scope(txn_dt).map({True: "Yes", False: "No"})
    df["currency_in_scope_flag"] = df["currency"].eq(CURRENCY_SCOPE).map({True: "Yes", False: "No"})
    df["entity_in_scope_flag"] = df["entity_name"].eq(ENTITY_SCOPE).map({True: "Yes", False: "No"})

    opening_balance_mask = df["description"].apply(standardize_text).fillna("").str.upper().str.contains("OPENING BALANCE")
    invalid_mask = (
        txn_dt.isna()
        | df["amount_numeric"].isna()
        | df["signed_amount"].isna()
        | ~df["debit_credit"].astype(str).str.strip().str.upper().isin(ALLOWED_BANK_DEBIT_CREDIT)
        | ~df["bank_debit_credit"].isin(ALLOWED_BANK_STANDARDIZED_DEBIT_CREDIT)
    )
    insufficient_match_keys_mask = (
        df["normalized_reference"].isna()
        & df["normalized_counterparty"].isna()
    )

    out_of_scope_month_mask = ~_month_in_scope(txn_dt)
    out_of_scope_currency_mask = ~df["currency"].eq(CURRENCY_SCOPE)
    out_of_scope_entity_mask = ~df["entity_name"].eq(ENTITY_SCOPE)

    df["recon_candidate"] = "Yes"
    df["recon_exclusion_reason"] = None

    df.loc[opening_balance_mask, ["recon_candidate", "recon_exclusion_reason"]] = ["No", "OPENING_BALANCE"]
    df.loc[~opening_balance_mask & invalid_mask, ["recon_candidate", "recon_exclusion_reason"]] = ["No", "INVALID_RECORD"]
    df.loc[
        ~opening_balance_mask & ~invalid_mask & insufficient_match_keys_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "INSUFFICIENT_MATCH_KEYS"]
    df.loc[
        ~opening_balance_mask & ~invalid_mask & ~insufficient_match_keys_mask & out_of_scope_month_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "OUT_OF_SCOPE_MONTH"]
    df.loc[
        ~opening_balance_mask
        & ~invalid_mask
        & ~insufficient_match_keys_mask
        & ~out_of_scope_month_mask
        & out_of_scope_entity_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "OUT_OF_SCOPE_ENTITY"]
    df.loc[
        ~opening_balance_mask
        & ~invalid_mask
        & ~insufficient_match_keys_mask
        & ~out_of_scope_month_mask
        & ~out_of_scope_entity_mask
        & out_of_scope_currency_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "OUT_OF_SCOPE_CURRENCY"]

    return _initialize_duplicate_fields(df)


def clean_ledger_transactions(raw_ledger):
    """Create the cleaned ledger transactions dataset."""
    df = raw_ledger.copy()

    df["source_file_name"] = "ledger_transactions_sample.csv"
    df["ledger_txn_id"] = df.apply(
        lambda row: f"{standardize_text(row['journal_reference'])}-{int(row['line_number']):03d}",
        axis=1,
    )
    df["source_record_id"] = df["ledger_txn_id"]
    df["cleaning_status"] = "CLEAN"
    df["cleaning_notes"] = None

    posting_dt = df["posting_date"].apply(parse_date)
    df["standardized_posting_date"] = _date_to_iso_string(posting_dt)
    df["standardized_document_date"] = None

    df["debit_amount_numeric"] = df["debit_amount"].apply(to_amount_numeric)
    df["credit_amount_numeric"] = df["credit_amount"].apply(to_amount_numeric)
    df["ledger_amount_abs"] = df[["debit_amount_numeric", "credit_amount_numeric"]].max(axis=1)
    df["ledger_debit_credit"] = df.apply(
        lambda row: derive_ledger_debit_credit(row["debit_amount_numeric"], row["credit_amount_numeric"]),
        axis=1,
    )
    df["ledger_cash_effect"] = (
        df["debit_amount_numeric"].fillna(0) - df["credit_amount_numeric"].fillna(0)
    ).round(2)
    df["amount_numeric"] = df["ledger_amount_abs"]
    df["signed_amount"] = df["ledger_cash_effect"]
    df["debit_credit"] = df["ledger_debit_credit"]
    df["reference_number"] = df["source_reference"]
    df["ledger_description"] = df["description"]
    df["gl_account"] = df["gl_account_name"]

    df["normalized_reference"] = df["source_reference"].apply(normalize_reference)
    # Assumption for V1: the ledger extract has no dedicated vendor field, so the description suffix is the counterparty fallback.
    df["vendor_customer_name"] = df["description"].apply(extract_ledger_counterparty)
    df["normalized_counterparty"] = df["vendor_customer_name"].apply(normalize_name)
    df["normalized_description"] = df["description"].apply(normalize_description)

    df["reference_usable"] = df["normalized_reference"].notna().map({True: "Yes", False: "No"})
    df["counterparty_usable"] = df["normalized_counterparty"].notna().map({True: "Yes", False: "No"})
    df["description_usable"] = df["normalized_description"].notna().map({True: "Yes", False: "No"})

    df["in_scope_month_flag"] = _month_in_scope(posting_dt).map({True: "Yes", False: "No"})
    df["entity_in_scope_flag"] = df["entity_name"].eq(ENTITY_SCOPE).map({True: "Yes", False: "No"})
    df["currency_in_scope_flag"] = df["currency"].eq(CURRENCY_SCOPE).map({True: "Yes", False: "No"})

    desc_upper = df["description"].apply(standardize_text).fillna("").str.upper()
    gl_upper = df["gl_account_name"].apply(standardize_text).fillna("").str.upper()

    opening_balance_mask = desc_upper.str.contains("OPENING CASH BALANCE")
    internal_offset_mask = desc_upper.str.contains("OFFSET")
    accounting_offset_mask = desc_upper.str.contains("INVENTORY")
    non_cash_balancing_mask = gl_upper.str.contains("OFFSET") | gl_upper.str.contains("CLEARING")
    timing_only_mask = desc_upper.str.contains("OUTSTANDING CHECK") | desc_upper.str.contains("IN TRANSIT")
    # Assumption for V1: cash-side rows are identified from cash GL naming or the 101xxx cash account range.
    cash_gl_mask = gl_upper.str.contains("CASH") | df["gl_account_number"].astype(str).str.startswith("101")

    invalid_mask = (
        posting_dt.isna()
        | df["ledger_amount_abs"].isna()
        | df["signed_amount"].isna()
        | ~df["debit_credit"].isin(ALLOWED_LEDGER_DEBIT_CREDIT)
        | (
            df["debit_amount_numeric"].fillna(0).gt(0)
            & df["credit_amount_numeric"].fillna(0).gt(0)
        )
    )

    out_of_scope_month_mask = ~_month_in_scope(posting_dt)
    out_of_scope_entity_mask = ~df["entity_name"].eq(ENTITY_SCOPE)
    out_of_scope_currency_mask = ~df["currency"].eq(CURRENCY_SCOPE)

    bank_relevant_activity = ~(
        opening_balance_mask
        | internal_offset_mask
        | accounting_offset_mask
        | non_cash_balancing_mask
        | timing_only_mask
        | ~cash_gl_mask
    )
    df["bank_relevant_activity_flag"] = bank_relevant_activity.map({True: "Yes", False: "No"})

    df["recon_candidate"] = "Yes"
    df["recon_exclusion_reason"] = None

    df.loc[opening_balance_mask, ["recon_candidate", "recon_exclusion_reason"]] = ["No", "OPENING_BALANCE"]
    df.loc[~opening_balance_mask & internal_offset_mask, ["recon_candidate", "recon_exclusion_reason"]] = ["No", "INTERNAL_OFFSET"]
    df.loc[
        ~opening_balance_mask & ~internal_offset_mask & non_cash_balancing_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "NON_CASH_BALANCING_LINE"]
    df.loc[
        ~opening_balance_mask & ~internal_offset_mask & ~non_cash_balancing_mask & accounting_offset_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "ACCOUNTING_OFFSET"]
    df.loc[
        ~opening_balance_mask
        & ~internal_offset_mask
        & ~non_cash_balancing_mask
        & ~accounting_offset_mask
        & timing_only_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "TIMING_ONLY"]
    df.loc[
        ~opening_balance_mask
        & ~internal_offset_mask
        & ~non_cash_balancing_mask
        & ~accounting_offset_mask
        & ~timing_only_mask
        & ~cash_gl_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "NON_CASH_GL"]
    df.loc[
        ~opening_balance_mask
        & ~internal_offset_mask
        & ~non_cash_balancing_mask
        & ~accounting_offset_mask
        & ~timing_only_mask
        & cash_gl_mask
        & invalid_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "INVALID_RECORD"]
    df.loc[
        ~opening_balance_mask
        & ~internal_offset_mask
        & ~non_cash_balancing_mask
        & ~accounting_offset_mask
        & ~timing_only_mask
        & cash_gl_mask
        & ~invalid_mask
        & out_of_scope_month_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "OUT_OF_SCOPE_MONTH"]
    df.loc[
        ~opening_balance_mask
        & ~internal_offset_mask
        & ~non_cash_balancing_mask
        & ~accounting_offset_mask
        & ~timing_only_mask
        & cash_gl_mask
        & ~invalid_mask
        & ~out_of_scope_month_mask
        & out_of_scope_entity_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "OUT_OF_SCOPE_ENTITY"]
    df.loc[
        ~opening_balance_mask
        & ~internal_offset_mask
        & ~non_cash_balancing_mask
        & ~accounting_offset_mask
        & ~timing_only_mask
        & cash_gl_mask
        & ~invalid_mask
        & ~out_of_scope_month_mask
        & ~out_of_scope_entity_mask
        & out_of_scope_currency_mask,
        ["recon_candidate", "recon_exclusion_reason"],
    ] = ["No", "OUT_OF_SCOPE_CURRENCY"]

    return _initialize_duplicate_fields(df)


def clean_payment_obligations(raw_obligations):
    """Create the cleaned payment obligations dataset."""
    df = raw_obligations.copy()

    df["source_file_name"] = "payment_obligations_sample.csv"
    df["obligation_id"] = df.index.to_series().add(1).map(lambda value: f"OBL-{value:04d}")
    df["source_record_id"] = df["obligation_id"]
    df["cleaning_status"] = "CLEAN"
    df["cleaning_notes"] = None

    due_dt = df["due_date"].apply(parse_date)
    invoice_dt = df["invoice_date"].apply(parse_date)
    aging_dt = df["aging_report_date"].apply(parse_date)

    df["standardized_due_date"] = _date_to_iso_string(due_dt)
    df["standardized_invoice_date"] = _date_to_iso_string(invoice_dt)
    df["standardized_aging_report_date"] = _date_to_iso_string(aging_dt)

    df["original_amount_numeric"] = df["original_amount"].apply(to_amount_numeric)
    df["paid_amount_numeric"] = df["paid_amount"].apply(to_amount_numeric)
    df["open_amount_numeric"] = df["open_amount"].apply(to_amount_numeric)
    df["amount_numeric"] = df["open_amount_numeric"]
    df["normalized_counterparty"] = df["vendor_name"].apply(normalize_name)
    df["normalized_invoice_number"] = df["invoice_number"].apply(normalize_reference)

    df["obligation_in_scope_flag"] = aging_dt.dt.strftime("%Y-%m").eq(REPORTING_MONTH).map({True: "Yes", False: "No"})
    df["currency_in_scope_flag"] = df["currency"].eq(CURRENCY_SCOPE).map({True: "Yes", False: "No"})
    df["entity_in_scope_flag"] = df["entity_name"].eq(ENTITY_SCOPE).map({True: "Yes", False: "No"})
    df["open_amount_validation_flag"] = (
        (df["original_amount_numeric"].fillna(0) - df["paid_amount_numeric"].fillna(0)).round(2)
        .eq(df["open_amount_numeric"].fillna(0))
    ).map({True: "Yes", False: "No"})
    df["aging_bucket_validation_flag"] = "Yes"
    df["primary_recon_candidate"] = "No"
    df["primary_recon_exclusion_reason"] = "SUPPORT_LAYER_ONLY_V1"

    return df
