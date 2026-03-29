from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from config import REPORTING_MONTH_LABEL


AMOUNT_TOLERANCE = 250.00
DATE_TOLERANCE_DAYS = 5
DUPLICATE_DATE_WINDOW_DAYS = 3


@dataclass
class ReconciliationResult:
    reconciled_matches: pd.DataFrame
    exceptions_report: pd.DataFrame
    reconciliation_summary: pd.DataFrame
    bank_working: pd.DataFrame
    ledger_working: pd.DataFrame


def _safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _date_diff_days(left_date, right_date) -> Optional[int]:
    if pd.isna(left_date) or pd.isna(right_date):
        return None
    return abs((pd.to_datetime(left_date) - pd.to_datetime(right_date)).days)


def _is_missing_reference(ref: str) -> bool:
    return _safe_str(ref) == ""


def _append_note(existing_note: str, additional_note: str) -> str:
    existing = _safe_str(existing_note)
    additional = _safe_str(additional_note)

    if not existing:
        return additional
    if not additional or additional in existing:
        return existing
    return f"{existing} {additional}"


def _normalize_yes_no_to_bool(series: pd.Series, default: bool = False) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .map(
            {
                "YES": True,
                "NO": False,
                "TRUE": True,
                "FALSE": False,
                "1": True,
                "0": False,
            }
        )
        .fillna(default)
        .astype(bool)
    )


def _build_match_row(
    match_id: str,
    bank_row: pd.Series,
    ledger_row: pd.Series,
    match_type: str,
    confidence: str,
    review_required: str,
    info_flag: str = "",
    notes: str = "",
) -> Dict:
    bank_date = bank_row["txn_date"]
    ledger_date = ledger_row["posting_date"]
    date_difference_days = _date_diff_days(bank_date, ledger_date)
    bank_cash_effect = round(float(bank_row["bank_cash_effect"]), 2)
    ledger_cash_effect = round(float(ledger_row["ledger_cash_effect"]), 2)

    return {
        "match_id": match_id,
        "bank_txn_id": bank_row["bank_txn_id"],
        "ledger_txn_id": ledger_row["ledger_txn_id"],
        "match_type": match_type,
        "confidence": confidence,
        "bank_debit_credit": _safe_str(bank_row.get("bank_debit_credit")),
        "ledger_debit_credit": _safe_str(ledger_row.get("ledger_debit_credit")),
        "bank_amount": bank_cash_effect,
        "ledger_amount": ledger_cash_effect,
        "amount_variance": round(float(bank_cash_effect - ledger_cash_effect), 2),
        "bank_date": pd.to_datetime(bank_date).date().isoformat() if not pd.isna(bank_date) else "",
        "ledger_date": pd.to_datetime(ledger_date).date().isoformat() if not pd.isna(ledger_date) else "",
        "date_difference_days": date_difference_days,
        "bank_reference": _safe_str(bank_row.get("reference_number")),
        "ledger_reference": _safe_str(ledger_row.get("reference_number")),
        "normalized_reference": _safe_str(bank_row.get("normalized_reference")),
        "bank_counterparty": _safe_str(bank_row.get("counterparty_match_name")),
        "ledger_counterparty": _safe_str(ledger_row.get("counterparty_match_name")),
        "review_required": review_required,
        "info_flag": info_flag,
        "notes": notes,
    }


def select_recon_candidates(
    bank_df: pd.DataFrame,
    ledger_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    bank = bank_df.copy()
    ledger = ledger_df.copy()

    bank["recon_candidate"] = _normalize_yes_no_to_bool(bank.get("recon_candidate", True), default=False)
    ledger["recon_candidate"] = _normalize_yes_no_to_bool(ledger.get("recon_candidate", True), default=False)
    bank["recon_exclusion_reason"] = bank.get("recon_exclusion_reason", "")
    ledger["recon_exclusion_reason"] = ledger.get("recon_exclusion_reason", "")

    if "bank_relevant_activity_flag" in ledger.columns:
        ledger["recon_candidate"] = ledger["recon_candidate"] & _normalize_yes_no_to_bool(
            ledger["bank_relevant_activity_flag"],
            default=False,
        )

    if "counterparty_match_name" not in bank.columns:
        bank["counterparty_match_name"] = (
            bank.get("counterparty_name")
            .fillna(bank.get("normalized_counterparty"))
            .fillna(bank.get("normalized_counterparty_fallback"))
        )

    if "counterparty_match_name" not in ledger.columns:
        ledger["counterparty_match_name"] = ledger.get("normalized_counterparty")

    return bank, ledger


def flag_possible_duplicates(df: pd.DataFrame, source_type: str) -> pd.DataFrame:
    working = df.copy()
    working["duplicate_flag"] = False
    working["duplicate_group_key"] = ""
    working["duplicate_notes"] = ""

    candidates = working.loc[working["recon_candidate"]].copy()
    if candidates.empty:
        return working

    date_col = "txn_date" if source_type == "bank" else "posting_date"

    group_cols = ["signed_amount", "normalized_counterparty"]
    for _, grp in candidates.groupby(group_cols, dropna=False):
        if len(grp) < 2:
            continue

        grp = grp.sort_values(date_col).copy()
        grp_dates = pd.to_datetime(grp[date_col], errors="coerce")

        duplicate_indices = set()
        for i in range(len(grp)):
            for j in range(i + 1, len(grp)):
                if pd.isna(grp_dates.iloc[i]) or pd.isna(grp_dates.iloc[j]):
                    continue
                days = abs((grp_dates.iloc[j] - grp_dates.iloc[i]).days)
                if days <= DUPLICATE_DATE_WINDOW_DAYS:
                    duplicate_indices.add(grp.index[i])
                    duplicate_indices.add(grp.index[j])

        if duplicate_indices:
            group_key = (
                f"{source_type.upper()}|"
                f"{grp['signed_amount'].iloc[0]:.2f}|"
                f"{_safe_str(grp['normalized_counterparty'].iloc[0])}"
            )
            working.loc[list(duplicate_indices), "duplicate_flag"] = True
            working.loc[list(duplicate_indices), "duplicate_group_key"] = group_key
            working.loc[list(duplicate_indices), "duplicate_notes"] = (
                f"Possible duplicate within {source_type} source; review before matching."
            )

    return working


def _prepare_matching_pools(
    bank_df: pd.DataFrame,
    ledger_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    bank_pool = bank_df.loc[bank_df["recon_candidate"] & (~bank_df["duplicate_flag"])].copy()
    ledger_pool = ledger_df.loc[ledger_df["recon_candidate"] & (~ledger_df["duplicate_flag"])].copy()

    bank_pool["matched"] = False
    ledger_pool["matched"] = False

    return bank_pool, ledger_pool


def _candidate_pairs(
    bank_pool: pd.DataFrame,
    ledger_pool: pd.DataFrame,
) -> pd.DataFrame:
    left = bank_pool.copy()
    right = ledger_pool.copy()

    left["__key"] = 1
    right["__key"] = 1

    pairs = left.merge(right, on="__key", suffixes=("_bank", "_ledger")).drop(columns="__key")

    pairs["date_difference_days"] = (
        pd.to_datetime(pairs["txn_date"], errors="coerce")
        - pd.to_datetime(pairs["posting_date"], errors="coerce")
    ).abs().dt.days

    pairs["amount_difference"] = (
        pairs["bank_cash_effect"] - pairs["ledger_cash_effect"]
    ).abs().round(2)

    pairs["reference_match"] = (
        pairs["normalized_reference_bank"].fillna("")
        == pairs["normalized_reference_ledger"].fillna("")
    )
    pairs["counterparty_match"] = (
        pairs["normalized_counterparty_bank"].fillna("")
        == pairs["normalized_counterparty_ledger"].fillna("")
    )
    pairs["cash_effect_match"] = pairs["bank_cash_effect"].round(2) == pairs["ledger_cash_effect"].round(2)
    pairs["missing_reference_either"] = (
        pairs["normalized_reference_bank"].fillna("").eq("")
        | pairs["normalized_reference_ledger"].fillna("").eq("")
    )

    return pairs


def _choose_best_pair(rule_pairs: pd.DataFrame) -> pd.DataFrame:
    if rule_pairs.empty:
        return rule_pairs.copy()

    sorted_pairs = rule_pairs.sort_values(
        by=["date_difference_days", "amount_difference", "bank_txn_id", "ledger_txn_id"]
    ).copy()

    used_bank = set()
    used_ledger = set()
    selected_rows = []

    for _, row in sorted_pairs.iterrows():
        bank_id = row["bank_txn_id"]
        ledger_id = row["ledger_txn_id"]

        if bank_id in used_bank or ledger_id in used_ledger:
            continue

        used_bank.add(bank_id)
        used_ledger.add(ledger_id)
        selected_rows.append(row)

    if not selected_rows:
        return sorted_pairs.iloc[0:0].copy()

    return pd.DataFrame(selected_rows)


def reconcile_bank_vs_ledger(bank_df: pd.DataFrame, ledger_df: pd.DataFrame) -> ReconciliationResult:
    bank_working, ledger_working = select_recon_candidates(bank_df, ledger_df)
    bank_working = flag_possible_duplicates(bank_working, source_type="bank")
    ledger_working = flag_possible_duplicates(ledger_working, source_type="ledger")

    bank_pool, ledger_pool = _prepare_matching_pools(bank_working, ledger_working)

    matches: List[Dict] = []
    match_counter = 1

    def next_match_id() -> str:
        nonlocal match_counter
        value = f"MATCH-{match_counter:04d}"
        match_counter += 1
        return value

    while True:
        available_bank = bank_pool.loc[~bank_pool["matched"]].copy()
        available_ledger = ledger_pool.loc[~ledger_pool["matched"]].copy()

        if available_bank.empty or available_ledger.empty:
            break

        pairs = _candidate_pairs(available_bank, available_ledger)

        if pairs.empty:
            break

        rule1 = pairs[
            (
                (pairs["reference_match"] & (~pairs["missing_reference_either"]))
                | pairs["counterparty_match"]
            )
            & (pairs["cash_effect_match"])
            & (pairs["date_difference_days"] == 0)
        ]
        selected = _choose_best_pair(rule1)
        if not selected.empty:
            for _, row in selected.iterrows():
                bank_row = available_bank.loc[available_bank["bank_txn_id"] == row["bank_txn_id"]].iloc[0]
                ledger_row = available_ledger.loc[
                    available_ledger["ledger_txn_id"] == row["ledger_txn_id"]
                ].iloc[0]
                review_required = "Yes" if bool(row.get("missing_reference_either")) else "No"
                notes = _reference_review_note() if review_required == "Yes" else ""
                matches.append(
                    _build_match_row(
                        match_id=next_match_id(),
                        bank_row=bank_row,
                        ledger_row=ledger_row,
                        match_type="EXACT",
                        confidence="HIGH",
                        review_required=review_required,
                        notes=notes,
                    )
                )
                bank_pool.loc[bank_pool["bank_txn_id"] == bank_row["bank_txn_id"], "matched"] = True
                ledger_pool.loc[ledger_pool["ledger_txn_id"] == ledger_row["ledger_txn_id"], "matched"] = True
            continue

        rule1b = pairs[
            (pairs["cash_effect_match"])
            & (pairs["date_difference_days"] == 0)
        ]
        selected = _choose_best_pair(rule1b)
        if not selected.empty:
            for _, row in selected.iterrows():
                bank_row = available_bank.loc[available_bank["bank_txn_id"] == row["bank_txn_id"]].iloc[0]
                ledger_row = available_ledger.loc[
                    available_ledger["ledger_txn_id"] == row["ledger_txn_id"]
                ].iloc[0]
                notes = _append_note(
                    "Matched on same-day normalized cash effect after source-specific references/counterparties differed.",
                    _reference_review_note() if bool(row.get("missing_reference_either")) else "",
                )
                matches.append(
                    _build_match_row(
                        match_id=next_match_id(),
                        bank_row=bank_row,
                        ledger_row=ledger_row,
                        match_type="EXACT",
                        confidence="HIGH",
                        review_required="Yes",
                        info_flag="CASH_EFFECT_SAME_DAY",
                        notes=notes,
                    )
                )
                bank_pool.loc[bank_pool["bank_txn_id"] == bank_row["bank_txn_id"], "matched"] = True
                ledger_pool.loc[ledger_pool["ledger_txn_id"] == ledger_row["ledger_txn_id"], "matched"] = True
            continue

        rule2 = pairs[
            (
                (pairs["reference_match"] & (~pairs["missing_reference_either"]))
                | pairs["counterparty_match"]
            )
            & (pairs["cash_effect_match"])
            & (pairs["date_difference_days"].between(1, DATE_TOLERANCE_DAYS))
        ]
        selected = _choose_best_pair(rule2)
        if not selected.empty:
            for _, row in selected.iterrows():
                bank_row = available_bank.loc[available_bank["bank_txn_id"] == row["bank_txn_id"]].iloc[0]
                ledger_row = available_ledger.loc[
                    available_ledger["ledger_txn_id"] == row["ledger_txn_id"]
                ].iloc[0]
                review_required = "Yes" if bool(row.get("missing_reference_either")) else "No"
                notes = _reference_review_note() if review_required == "Yes" else ""
                matches.append(
                    _build_match_row(
                        match_id=next_match_id(),
                        bank_row=bank_row,
                        ledger_row=ledger_row,
                        match_type="DATE_DIFFERENCE",
                        confidence="HIGH",
                        review_required=review_required,
                        info_flag="DATE_GAP",
                        notes=notes,
                    )
                )
                bank_pool.loc[bank_pool["bank_txn_id"] == bank_row["bank_txn_id"], "matched"] = True
                ledger_pool.loc[ledger_pool["ledger_txn_id"] == ledger_row["ledger_txn_id"], "matched"] = True
            continue

        rule3 = pairs[
            (pairs["missing_reference_either"])
            & (pairs["cash_effect_match"])
            & (pairs["counterparty_match"])
            & (pairs["date_difference_days"] <= DATE_TOLERANCE_DAYS)
        ]
        selected = _choose_best_pair(rule3)
        if not selected.empty:
            for _, row in selected.iterrows():
                bank_row = available_bank.loc[available_bank["bank_txn_id"] == row["bank_txn_id"]].iloc[0]
                ledger_row = available_ledger.loc[
                    available_ledger["ledger_txn_id"] == row["ledger_txn_id"]
                ].iloc[0]
                matches.append(
                    _build_match_row(
                        match_id=next_match_id(),
                        bank_row=bank_row,
                        ledger_row=ledger_row,
                        match_type="LIKELY_MATCH_NO_REF",
                        confidence="MEDIUM",
                        review_required="Yes",
                        notes=_append_note(
                            "EXC-04 candidate: likely match without usable reference.",
                            _reference_review_note(),
                        ),
                    )
                )
                bank_pool.loc[bank_pool["bank_txn_id"] == bank_row["bank_txn_id"], "matched"] = True
                ledger_pool.loc[ledger_pool["ledger_txn_id"] == ledger_row["ledger_txn_id"], "matched"] = True
            continue

        rule4 = pairs[
            (
                (pairs["reference_match"] & (~pairs["missing_reference_either"]))
                | pairs["counterparty_match"]
            )
            & (pairs["date_difference_days"] <= DATE_TOLERANCE_DAYS)
            & (pairs["amount_difference"] > 0)
            & (pairs["amount_difference"] <= AMOUNT_TOLERANCE)
        ]
        selected = _choose_best_pair(rule4)
        if not selected.empty:
            for _, row in selected.iterrows():
                bank_row = available_bank.loc[available_bank["bank_txn_id"] == row["bank_txn_id"]].iloc[0]
                ledger_row = available_ledger.loc[
                    available_ledger["ledger_txn_id"] == row["ledger_txn_id"]
                ].iloc[0]
                matches.append(
                    _build_match_row(
                        match_id=next_match_id(),
                        bank_row=bank_row,
                        ledger_row=ledger_row,
                        match_type="AMOUNT_MISMATCH",
                        confidence="MEDIUM",
                        review_required="Yes",
                        notes="EXC-03 candidate: amount variance within Version 1 tolerance.",
                    )
                )
                bank_pool.loc[bank_pool["bank_txn_id"] == bank_row["bank_txn_id"], "matched"] = True
                ledger_pool.loc[ledger_pool["ledger_txn_id"] == ledger_row["ledger_txn_id"], "matched"] = True
            continue

        rule4b = pairs[
            (pairs["reference_match"])
            & (~pairs["missing_reference_either"])
            & (pairs["date_difference_days"] <= DATE_TOLERANCE_DAYS)
            & (pairs["amount_difference"] > AMOUNT_TOLERANCE)
        ]
        selected = _choose_best_pair(rule4b)
        if not selected.empty:
            for _, row in selected.iterrows():
                bank_row = available_bank.loc[available_bank["bank_txn_id"] == row["bank_txn_id"]].iloc[0]
                ledger_row = available_ledger.loc[
                    available_ledger["ledger_txn_id"] == row["ledger_txn_id"]
                ].iloc[0]
                matches.append(
                    _build_match_row(
                        match_id=next_match_id(),
                        bank_row=bank_row,
                        ledger_row=ledger_row,
                        match_type="AMOUNT_MISMATCH_OVER_TOLERANCE",
                        confidence="MEDIUM",
                        review_required="Yes",
                        notes="EXC-03 candidate: reference-linked amount variance above Version 1 tolerance.",
                    )
                )
                bank_pool.loc[bank_pool["bank_txn_id"] == bank_row["bank_txn_id"], "matched"] = True
                ledger_pool.loc[ledger_pool["ledger_txn_id"] == ledger_row["ledger_txn_id"], "matched"] = True
            continue

        break

    reconciled_matches = pd.DataFrame(matches)

    exceptions_report = build_exceptions_report(
        bank_working=bank_working,
        ledger_working=ledger_working,
        bank_pool=bank_pool,
        ledger_pool=ledger_pool,
        reconciled_matches=reconciled_matches,
    )

    reconciliation_summary = build_reconciliation_summary(
        bank_working=bank_working,
        ledger_working=ledger_working,
        reconciled_matches=reconciled_matches,
        exceptions_report=exceptions_report,
    )

    return ReconciliationResult(
        reconciled_matches=reconciled_matches,
        exceptions_report=exceptions_report,
        reconciliation_summary=reconciliation_summary,
        bank_working=bank_working,
        ledger_working=ledger_working,
    )


def build_exceptions_report(
    bank_working: pd.DataFrame,
    ledger_working: pd.DataFrame,
    bank_pool: pd.DataFrame,
    ledger_pool: pd.DataFrame,
    reconciled_matches: pd.DataFrame,
) -> pd.DataFrame:
    exception_rows: List[Dict] = []
    exception_counter = 1

    def next_exception_id() -> str:
        nonlocal exception_counter
        value = f"EX-{exception_counter:04d}"
        exception_counter += 1
        return value

    matched_bank_ids = set(reconciled_matches["bank_txn_id"]) if not reconciled_matches.empty else set()
    matched_ledger_ids = set(reconciled_matches["ledger_txn_id"]) if not reconciled_matches.empty else set()

    bank_dups = bank_working.loc[bank_working["duplicate_flag"]].copy()
    for _, row in bank_dups.iterrows():
        exception_rows.append(
            {
                "exception_id": next_exception_id(),
                "exception_category": "EXC-06",
                "source_file": "bank",
                "source_record_id": row["bank_txn_id"],
                "txn_date": pd.to_datetime(row["txn_date"]).date().isoformat() if not pd.isna(row["txn_date"]) else "",
                "amount": round(float(row["signed_amount"]), 2),
                "exception_amount_display": _unsigned_amount(row["signed_amount"]),
                "source_debit_credit": row["bank_debit_credit"],
                "standardized_cash_dc": _standardized_cash_direction_from_amount(row["signed_amount"]),
                "vendor_name": _safe_str(row.get("counterparty_match_name")),
                "reference_number": _safe_str(row.get("reference_number")),
                "cash_flow_relevance": "Yes",
                "review_required": "Yes",
                "recommended_action": "Review duplicate risk before allowing reconciliation.",
                "notes": _safe_str(row.get("duplicate_notes")),
            }
        )

    ledger_dups = ledger_working.loc[ledger_working["duplicate_flag"]].copy()
    for _, row in ledger_dups.iterrows():
        exception_rows.append(
            {
                "exception_id": next_exception_id(),
                "exception_category": "EXC-06",
                "source_file": "ledger",
                "source_record_id": row["ledger_txn_id"],
                "txn_date": pd.to_datetime(row["posting_date"]).date().isoformat() if not pd.isna(row["posting_date"]) else "",
                "amount": round(float(row["signed_amount"]), 2),
                "exception_amount_display": _unsigned_amount(row["signed_amount"]),
                "source_debit_credit": row["ledger_debit_credit"],
                "standardized_cash_dc": _standardized_cash_direction_from_amount(row["signed_amount"]),
                "vendor_name": _safe_str(row.get("counterparty_match_name")),
                "reference_number": _safe_str(row.get("reference_number")),
                "cash_flow_relevance": "Yes",
                "review_required": "Yes",
                "recommended_action": "Review duplicate risk before allowing reconciliation.",
                "notes": _safe_str(row.get("duplicate_notes")),
            }
        )

    ledger_timing_items = ledger_working.loc[
        ledger_working["recon_exclusion_reason"].eq("TIMING_ONLY")
    ].copy()
    for _, row in ledger_timing_items.iterrows():
        exception_rows.append(
            {
                "exception_id": next_exception_id(),
                "exception_category": "EXC-02",
                "source_file": "ledger",
                "source_record_id": row["ledger_txn_id"],
                "txn_date": pd.to_datetime(row["posting_date"]).date().isoformat() if not pd.isna(row["posting_date"]) else "",
                "amount": round(float(row["signed_amount"]), 2),
                "exception_amount_display": _unsigned_amount(row["signed_amount"]),
                "source_debit_credit": row["ledger_debit_credit"],
                "standardized_cash_dc": _standardized_cash_direction_from_amount(row["signed_amount"]),
                "vendor_name": _safe_str(row.get("counterparty_match_name")),
                "reference_number": _safe_str(row.get("reference_number")),
                "cash_flow_relevance": "Yes",
                "review_required": "Yes",
                "recommended_action": "Track ledger timing item until it clears the bank statement.",
                "notes": "Ledger-side timing reconciling item excluded from core matching.",
            }
        )

    if not reconciled_matches.empty:
        for _, row in reconciled_matches.iterrows():
            if row["match_type"] == "LIKELY_MATCH_NO_REF":
                exception_rows.append(
                    {
                        "exception_id": next_exception_id(),
                        "exception_category": "EXC-04",
                        "source_file": "matched_pair",
                        "source_record_id": f"{row['bank_txn_id']} | {row['ledger_txn_id']}",
                        "txn_date": row["bank_date"],
                        "amount": row["bank_amount"],
                        "exception_amount_display": _unsigned_amount(row["bank_amount"]),
                        "source_debit_credit": row.get("bank_debit_credit", ""),
                        "standardized_cash_dc": _standardized_cash_direction_from_amount(row["bank_amount"]),
                        "vendor_name": row["bank_counterparty"] or row["ledger_counterparty"],
                        "reference_number": row["bank_reference"] or row["ledger_reference"],
                        "cash_flow_relevance": "Low to medium",
                        "review_required": "Yes",
                        "recommended_action": "Review likely pair and confirm missing/unusable reference.",
                        "notes": row["notes"],
                    }
                )
            elif row["match_type"] in {"AMOUNT_MISMATCH", "AMOUNT_MISMATCH_OVER_TOLERANCE"}:
                exception_rows.append(
                    {
                        "exception_id": next_exception_id(),
                        "exception_category": "EXC-03",
                        "source_file": "matched_pair",
                        "source_record_id": f"{row['bank_txn_id']} | {row['ledger_txn_id']}",
                        "txn_date": row["bank_date"],
                        "amount": row["bank_amount"],
                        "exception_amount_display": _unsigned_amount(row["bank_amount"]),
                        "source_debit_credit": row.get("bank_debit_credit", ""),
                        "standardized_cash_dc": _standardized_cash_direction_from_amount(row["bank_amount"]),
                        "vendor_name": row["bank_counterparty"] or row["ledger_counterparty"],
                        "reference_number": row["bank_reference"] or row["ledger_reference"],
                        "cash_flow_relevance": "Yes",
                        "review_required": "Yes",
                        "recommended_action": "Review amount variance and determine root cause.",
                        "notes": row["notes"],
                    }
                )

    bank_unmatched = bank_pool.loc[
        (~bank_pool["matched"])
        & (~bank_pool["bank_txn_id"].isin(matched_bank_ids))
    ].copy()

    for _, row in bank_unmatched.iterrows():
        if row["bank_debit_credit"] == "DR":
            if _is_missing_reference(row.get("normalized_reference")) and _safe_str(row.get("normalized_counterparty")) == "":
                category = "EXC-07"
                action = "Investigate unidentified bank outflow."
            elif any(
                keyword in _safe_str(row.get("normalized_description"))
                for keyword in {"FEE", "SERVICE CHARGE"}
            ):
                category = "EXC-07"
                action = "Investigate bank-only fee or unknown outflow."
            else:
                category = "EXC-01"
                action = "Investigate bank transaction not found in ledger."
        else:
            category = "EXC-08"
            action = "Investigate unposted bank receipt."

        exception_rows.append(
            {
                "exception_id": next_exception_id(),
                "exception_category": category,
                "source_file": "bank",
                "source_record_id": row["bank_txn_id"],
                "txn_date": pd.to_datetime(row["txn_date"]).date().isoformat() if not pd.isna(row["txn_date"]) else "",
                "amount": round(float(row["signed_amount"]), 2),
                "exception_amount_display": _unsigned_amount(row["signed_amount"]),
                "source_debit_credit": row["bank_debit_credit"],
                "standardized_cash_dc": _standardized_cash_direction_from_amount(row["signed_amount"]),
                "vendor_name": _safe_str(row.get("counterparty_match_name")),
                "reference_number": _safe_str(row.get("reference_number")),
                "cash_flow_relevance": "Yes",
                "review_required": "Yes",
                "recommended_action": action,
                "notes": "Unmatched bank-side reconciliation candidate.",
            }
        )

    ledger_unmatched = ledger_pool.loc[
        (~ledger_pool["matched"])
        & (~ledger_pool["ledger_txn_id"].isin(matched_ledger_ids))
    ].copy()

    for _, row in ledger_unmatched.iterrows():
        exception_rows.append(
            {
                "exception_id": next_exception_id(),
                "exception_category": "EXC-02",
                "source_file": "ledger",
                "source_record_id": row["ledger_txn_id"],
                "txn_date": pd.to_datetime(row["posting_date"]).date().isoformat() if not pd.isna(row["posting_date"]) else "",
                "amount": round(float(row["signed_amount"]), 2),
                "exception_amount_display": _unsigned_amount(row["signed_amount"]),
                "source_debit_credit": row["ledger_debit_credit"],
                "standardized_cash_dc": _standardized_cash_direction_from_amount(row["signed_amount"]),
                "vendor_name": _safe_str(row.get("counterparty_match_name")),
                "reference_number": _safe_str(row.get("reference_number")),
                "cash_flow_relevance": "Yes",
                "review_required": "Yes",
                "recommended_action": "Investigate ledger transaction not found in bank.",
                "notes": "Unmatched ledger-side reconciliation candidate.",
            }
        )

    exceptions_report = pd.DataFrame(exception_rows)

    if exceptions_report.empty:
        return pd.DataFrame(
            columns=[
                "exception_id",
                "exception_category",
                "source_file",
                "source_record_id",
                "txn_date",
                "amount",
                "exception_amount_display",
                "source_debit_credit",
                "standardized_cash_dc",
                "vendor_name",
                "reference_number",
                "cash_flow_relevance",
                "review_required",
                "recommended_action",
                "notes",
            ]
        )

    return exceptions_report.sort_values(
        ["exception_category", "source_file", "source_record_id"]
    ).reset_index(drop=True)


def build_reconciliation_summary(
    bank_working: pd.DataFrame,
    ledger_working: pd.DataFrame,
    reconciled_matches: pd.DataFrame,
    exceptions_report: pd.DataFrame,
) -> pd.DataFrame:
    def count_in_scope_rows(df: pd.DataFrame) -> int:
        mask = pd.Series(True, index=df.index)
        for column_name in ["in_scope_month_flag", "entity_in_scope_flag", "currency_in_scope_flag"]:
            if column_name in df.columns:
                mask = mask & _normalize_yes_no_to_bool(df[column_name], default=False)
        return int(mask.sum())

    def count_matches(match_type: str) -> int:
        if reconciled_matches.empty:
            return 0
        return int((reconciled_matches["match_type"] == match_type).sum())

    def count_exception_rows(categories: set[str], source_file: Optional[str] = None) -> int:
        if exceptions_report.empty:
            return 0

        mask = exceptions_report["exception_category"].isin(categories)
        if source_file is not None:
            mask = mask & exceptions_report["source_file"].eq(source_file)
        return int(mask.sum())

    duplicate_risks = 0
    if not exceptions_report.empty:
        duplicate_risks = int((exceptions_report["exception_category"] == "EXC-06").sum())

    unresolved_exception_categories = {"EXC-01", "EXC-02", "EXC-03", "EXC-04", "EXC-06", "EXC-07", "EXC-08"}

    unresolved_exceptions = 0
    analytical_open_items_rollup = 0.0
    ending_bank_balance = None
    ending_ledger_balance = None
    adjusted_bank_balance = None
    adjusted_ledger_balance = None
    net_unresolved_cash_variance = None
    unresolved_variance_direction = ""

    if not bank_working.empty:
        ending_bank_balance = round(float(pd.to_numeric(bank_working["balance_after_txn"], errors="coerce").iloc[-1]), 2)

    if not ledger_working.empty:
        ending_ledger_balance = round(
            float(pd.to_numeric(ledger_working["balance_after_posting"], errors="coerce").iloc[-1]),
            2,
        )

    if not exceptions_report.empty:
        unresolved_mask = exceptions_report["exception_category"].isin(unresolved_exception_categories)
        unresolved_exceptions = int(unresolved_mask.sum())
        analytical_open_items_rollup = round(
            float(pd.to_numeric(exceptions_report["amount"], errors="coerce").sum()),
            2,
        )
        bank_only_amount = round(
            float(
                exceptions_report.loc[
                    exceptions_report["source_file"].eq("bank")
                    & exceptions_report["exception_category"].isin({"EXC-01", "EXC-07", "EXC-08"}),
                    "amount",
                ].sum()
            ),
            2,
        )
        ledger_only_amount = round(
            float(
                exceptions_report.loc[
                    exceptions_report["source_file"].eq("ledger")
                    & exceptions_report["exception_category"].eq("EXC-02"),
                    "amount",
                ].sum()
            ),
            2,
        )
        if ending_bank_balance is not None:
            adjusted_bank_balance = round(ending_bank_balance + ledger_only_amount, 2)
        if ending_ledger_balance is not None:
            adjusted_ledger_balance = round(ending_ledger_balance + bank_only_amount, 2)
    else:
        adjusted_bank_balance = ending_bank_balance
        adjusted_ledger_balance = ending_ledger_balance

    if adjusted_bank_balance is not None and adjusted_ledger_balance is not None:
        net_unresolved_cash_variance = round(adjusted_bank_balance - adjusted_ledger_balance, 2)
        if abs(net_unresolved_cash_variance) < 0.005:
            net_unresolved_cash_variance = 0.0
            unresolved_variance_direction = "BALANCED"
        elif net_unresolved_cash_variance > 0:
            unresolved_variance_direction = "BANK HIGHER"
        else:
            unresolved_variance_direction = "LEDGER HIGHER"

    summary_rows = [
        {"metric": "Period", "value": REPORTING_MONTH_LABEL},
        {"metric": "Bank ending balance", "value": ending_bank_balance},
        {"metric": "Adjusted bank balance", "value": adjusted_bank_balance},
        {"metric": "Ledger ending balance", "value": ending_ledger_balance},
        {"metric": "Adjusted ledger balance", "value": adjusted_ledger_balance},
        {"metric": "Net unresolved cash variance", "value": net_unresolved_cash_variance},
        {"metric": "Unresolved variance direction", "value": unresolved_variance_direction},
        {"metric": "Total bank transactions in scope", "value": count_in_scope_rows(bank_working)},
        {"metric": "Total ledger transactions in scope", "value": count_in_scope_rows(ledger_working)},
        {"metric": "Exact matches", "value": count_matches("EXACT")},
        {"metric": "Date-tolerance matches", "value": count_matches("DATE_DIFFERENCE")},
        {"metric": "Likely no-reference matches", "value": count_matches("LIKELY_MATCH_NO_REF")},
        {
            "metric": "Amount mismatches",
            "value": count_matches("AMOUNT_MISMATCH") + count_matches("AMOUNT_MISMATCH_OVER_TOLERANCE"),
        },
        {"metric": "Duplicate risks", "value": duplicate_risks},
        {
            "metric": "Unmatched bank items",
            "value": count_exception_rows({"EXC-01", "EXC-07", "EXC-08"}, source_file="bank"),
        },
        {
            "metric": "Unmatched ledger items",
            "value": count_exception_rows({"EXC-02"}, source_file="ledger"),
        },
        {"metric": "Total unresolved exceptions", "value": unresolved_exceptions},
        {"metric": "Open-item analytical rollup", "value": analytical_open_items_rollup},
    ]

    return pd.DataFrame(summary_rows)


def _yes_no(series: pd.Series) -> pd.Series:
    return _normalize_yes_no_to_bool(series, default=False).map({True: "Yes", False: "No"})


def _match_label(row: pd.Series) -> str:
    if _safe_str(row.get("info_flag")) == "CASH_EFFECT_SAME_DAY":
        return "Reviewed same-day match"

    mapping = {
        "EXACT": "Exact match",
        "DATE_DIFFERENCE": "Date-tolerance match",
        "LIKELY_MATCH_NO_REF": "Likely match without reference",
        "AMOUNT_MISMATCH": "Amount mismatch within tolerance",
        "AMOUNT_MISMATCH_OVER_TOLERANCE": "Amount mismatch over tolerance",
    }
    return mapping.get(_safe_str(row.get("match_type")), _safe_str(row.get("match_type")))


def _reference_review_note() -> str:
    return "Reference missing on one side — verify source document."


def _standardized_cash_direction_from_amount(amount) -> str:
    numeric_amount = pd.to_numeric(pd.Series([amount]), errors="coerce").iloc[0]
    if pd.isna(numeric_amount):
        return ""
    return "DR" if float(numeric_amount) > 0 else "CR"


def _unsigned_amount(amount) -> float | None:
    numeric_amount = pd.to_numeric(pd.Series([amount]), errors="coerce").iloc[0]
    if pd.isna(numeric_amount):
        return None
    return round(abs(float(numeric_amount)), 2)


def _exception_category_description(category: str) -> str:
    mapping = {
        "EXC-02": "Ledger-side reconciling item not yet reflected on the bank statement.",
        "EXC-07": "Bank-side outflow requiring review or recording support.",
        "EXC-08": "Bank-side inflow requiring review or recording support.",
        "EXC-01": "Bank transaction not found in the ledger.",
        "EXC-03": "Matched item with an amount variance requiring review.",
        "EXC-04": "Potential match requiring reviewer confirmation.",
        "EXC-06": "Possible duplicate item requiring review before reconciliation.",
    }
    return mapping.get(category, "Reconciliation exception requiring review.")


def _bank_display_signed_amount(amount, debit_credit: str) -> float | None:
    numeric_amount = pd.to_numeric(pd.Series([amount]), errors="coerce").iloc[0]
    if pd.isna(numeric_amount):
        return None

    direction = {
        "DR": -1,
        "DEBIT": -1,
        "CR": 1,
        "CREDIT": 1,
    }.get(_safe_str(debit_credit).upper())

    if direction is None:
        return round(float(numeric_amount), 2)

    return round(abs(float(numeric_amount)) * direction, 2)


def build_final_summary_view(
    reconciled_matches: pd.DataFrame,
    exceptions_report: pd.DataFrame,
    bank_working: pd.DataFrame,
    ledger_working: pd.DataFrame,
) -> pd.DataFrame:
    technical_summary = build_reconciliation_summary(
        bank_working=bank_working,
        ledger_working=ledger_working,
        reconciled_matches=reconciled_matches,
        exceptions_report=exceptions_report,
    )
    technical_map = dict(zip(technical_summary["metric"], technical_summary["value"]))

    reviewed_same_day_matches = 0
    exact_matches = 0
    if not reconciled_matches.empty:
        reviewed_same_day_matches = int(reconciled_matches["info_flag"].eq("CASH_EFFECT_SAME_DAY").sum())
        exact_matches = int(
            (
                reconciled_matches["match_type"].eq("EXACT")
                & ~reconciled_matches["info_flag"].eq("CASH_EFFECT_SAME_DAY")
            ).sum()
        )

    summary_rows = [
        {"metric": "Period", "value": technical_map.get("Period"), "note": ""},
        {"metric": "Bank ending balance", "value": technical_map.get("Bank ending balance"), "note": "Ending book value from the January 2024 bank data in scope."},
        {"metric": "Adjusted bank balance", "value": technical_map.get("Adjusted bank balance"), "note": "Bank ending balance adjusted for ledger-side reconciling items not yet on the bank statement."},
        {"metric": "Ledger ending balance", "value": technical_map.get("Ledger ending balance"), "note": "Ending cash-book value from the January 2024 ledger data in scope."},
        {"metric": "Adjusted ledger balance", "value": technical_map.get("Adjusted ledger balance"), "note": "Ledger ending balance adjusted for bank-side reconciling items not yet recorded in the ledger."},
        {"metric": "Net unresolved cash variance", "value": technical_map.get("Net unresolved cash variance"), "note": "Remaining difference between adjusted bank and adjusted ledger balances."},
        {"metric": "Unresolved variance direction", "value": technical_map.get("Unresolved variance direction"), "note": "Shows BALANCED when the reconciliation closes."},
        {"metric": "Total bank transactions in scope", "value": technical_map.get("Total bank transactions in scope"), "note": ""},
        {"metric": "Total ledger transactions in scope", "value": technical_map.get("Total ledger transactions in scope"), "note": ""},
        {"metric": "Exact matches", "value": exact_matches, "note": ""},
        {
            "metric": "Reviewed same-day matches",
            "value": reviewed_same_day_matches,
            "note": "Same-day cash-effect matches reviewed where source-side references or naming differ.",
        },
        {"metric": "Date-tolerance matches", "value": technical_map.get("Date-tolerance matches"), "note": ""},
        {"metric": "Likely no-reference matches", "value": technical_map.get("Likely no-reference matches"), "note": ""},
        {"metric": "Amount mismatches", "value": technical_map.get("Amount mismatches"), "note": ""},
        {"metric": "Duplicate risks", "value": technical_map.get("Duplicate risks"), "note": ""},
        {"metric": "Unmatched bank items", "value": technical_map.get("Unmatched bank items"), "note": ""},
        {"metric": "Unmatched ledger items", "value": technical_map.get("Unmatched ledger items"), "note": ""},
        {"metric": "Total unresolved exceptions", "value": technical_map.get("Total unresolved exceptions"), "note": ""},
        {
            "metric": "Open-item analytical rollup",
            "value": technical_map.get("Open-item analytical rollup"),
            "note": "Supporting signed rollup of listed open items; not the primary closing variance.",
        },
    ]

    return pd.DataFrame(summary_rows)


def build_final_reconciled_matches_view(
    reconciled_matches: pd.DataFrame,
    bank_working: pd.DataFrame,
    ledger_working: pd.DataFrame,
) -> pd.DataFrame:
    if reconciled_matches.empty:
        return pd.DataFrame(
            columns=[
                "Match ID",
                "Match Result",
                "Bank Transaction Date",
                "Bank Value Date",
                "Bank Description",
                "Bank Counterparty",
                "Bank Reference Number",
                "Bank Debit / Credit",
                "Bank Amount",
                "Ledger Posting Date",
                "Ledger Journal Reference",
                "Ledger Description",
                "Ledger Source Document",
                "Ledger Source Reference",
                "Ledger Debit / Credit",
                "Ledger Amount",
                "Review Required",
                "Notes",
            ]
        )

    bank_columns = [
        "bank_txn_id",
        "standardized_txn_date",
        "standardized_value_date",
        "description",
        "counterparty_name",
        "reference_number",
        "debit_credit",
        "amount_numeric",
    ]
    ledger_columns = [
        "ledger_txn_id",
        "standardized_posting_date",
        "journal_reference",
        "description",
        "source_document",
        "source_reference",
        "ledger_debit_credit",
        "ledger_amount_abs",
    ]

    bank_view = (
        bank_working[bank_columns]
        .drop_duplicates(subset=["bank_txn_id"])
        .rename(
            columns={
                "standardized_txn_date": "bank_transaction_date_display",
                "standardized_value_date": "bank_value_date_display",
                "description": "bank_description_display",
                "counterparty_name": "bank_counterparty_display",
                "reference_number": "bank_reference_display",
                "debit_credit": "bank_debit_credit_display",
                "amount_numeric": "bank_amount_display",
            }
        )
    )
    ledger_view = (
        ledger_working[ledger_columns]
        .drop_duplicates(subset=["ledger_txn_id"])
        .rename(
            columns={
                "standardized_posting_date": "ledger_posting_date_display",
                "journal_reference": "ledger_journal_reference_display",
                "description": "ledger_description_display",
                "source_document": "ledger_source_document_display",
                "source_reference": "ledger_source_reference_display",
                "ledger_debit_credit": "ledger_debit_credit_display",
                "ledger_amount_abs": "ledger_amount_display",
            }
        )
    )

    final_view = reconciled_matches.merge(bank_view, on="bank_txn_id", how="left")
    final_view = final_view.merge(ledger_view, on="ledger_txn_id", how="left")

    return pd.DataFrame(
        {
            "Match ID": final_view["match_id"],
            "Match Result": final_view.apply(_match_label, axis=1),
            "Bank Transaction Date": final_view["bank_transaction_date_display"],
            "Bank Value Date": final_view["bank_value_date_display"],
            "Bank Description": final_view["bank_description_display"],
            "Bank Counterparty": final_view["bank_counterparty_display"],
            "Bank Reference Number": final_view["bank_reference_display"],
            "Bank Debit / Credit": final_view["bank_debit_credit_display"],
            "Bank Amount": final_view["bank_amount_display"],
            "Ledger Posting Date": final_view["ledger_posting_date_display"],
            "Ledger Journal Reference": final_view["ledger_journal_reference_display"],
            "Ledger Description": final_view["ledger_description_display"],
            "Ledger Source Document": final_view["ledger_source_document_display"],
            "Ledger Source Reference": final_view["ledger_source_reference_display"],
            "Ledger Debit / Credit": final_view["ledger_debit_credit_display"],
            "Ledger Amount": final_view["ledger_amount_display"],
            "Review Required": final_view["review_required"],
            "Notes": final_view["notes"],
        }
    )


def build_final_exceptions_view(exceptions_report: pd.DataFrame) -> pd.DataFrame:
    if exceptions_report.empty:
        return pd.DataFrame(
            columns=[
                "Exception ID",
                "Exception Category",
                "Exception Category Description",
                "Source Side",
                "Source Record ID",
                "Transaction Date",
                "Cash Movement DC",
                "Exception Amount",
                "Vendor Name",
                "Reference Number",
                "Recommended Action",
                "Notes",
            ]
        )

    final_view = exceptions_report.copy()
    final_view["Exception Category Description"] = final_view["exception_category"].apply(_exception_category_description)
    final_view["Source Side"] = final_view["source_file"].str.title()

    return pd.DataFrame(
        {
            "Exception ID": final_view["exception_id"],
            "Exception Category": final_view["exception_category"],
            "Exception Category Description": final_view["Exception Category Description"],
            "Source Side": final_view["Source Side"],
            "Source Record ID": final_view["source_record_id"],
            "Transaction Date": final_view["txn_date"],
            "Cash Movement DC": final_view["standardized_cash_dc"],
            "Exception Amount": final_view["exception_amount_display"].where(
                final_view["exception_amount_display"].notna(),
                final_view["amount"].abs(),
            ),
            "Vendor Name": final_view["vendor_name"],
            "Reference Number": final_view["reference_number"],
            "Recommended Action": final_view["recommended_action"],
            "Notes": final_view["notes"],
        }
    )


def build_legend_view() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Legend Item": "Exact match",
                "Type": "Match",
                "Description": "Bank and ledger items reconciled directly without reviewer escalation.",
            },
            {
                "Legend Item": "Reviewed same-day match",
                "Type": "Match",
                "Description": "Same-day cash-effect match reviewed because source-side references or naming differ.",
            },
            {
                "Legend Item": "EXC-02",
                "Type": "Exception",
                "Description": _exception_category_description("EXC-02"),
            },
            {
                "Legend Item": "EXC-07",
                "Type": "Exception",
                "Description": _exception_category_description("EXC-07"),
            },
            {
                "Legend Item": "EXC-08",
                "Type": "Exception",
                "Description": _exception_category_description("EXC-08"),
            },
        ]
    )


def build_clean_bank_view(bank_working: pd.DataFrame) -> pd.DataFrame:
    reference_missing = bank_working["reference_number"].fillna("").astype(str).str.strip().eq("")

    return pd.DataFrame(
        {
            "Bank Transaction ID": bank_working["bank_txn_id"],
            "Entity Name": bank_working["entity_name"],
            "Transaction Date": bank_working["standardized_txn_date"],
            "Value Date": bank_working["standardized_value_date"],
            "Description": bank_working["description"],
            "Counterparty Name": bank_working["counterparty_name"],
            "Reference Number": bank_working["reference_number"],
            "Reference Missing in Source": reference_missing.map({True: "Yes", False: "No"}),
            "Debit / Credit": bank_working["debit_credit"],
            "Amount": bank_working["amount_numeric"],
            "Balance After Transaction": pd.to_numeric(bank_working["balance_after_txn"], errors="coerce"),
            "Currency": bank_working["currency"],
            "Reconciliation Candidate": _yes_no(bank_working["recon_candidate"]),
            "Reconciliation Exclusion Reason": bank_working["recon_exclusion_reason"].fillna(""),
        }
    )


def build_clean_ledger_view(ledger_working: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Ledger Transaction ID": ledger_working["ledger_txn_id"],
            "Entity Name": ledger_working["entity_name"],
            "Posting Date": ledger_working["standardized_posting_date"],
            "Journal Reference": ledger_working["journal_reference"],
            "Line Number": pd.to_numeric(ledger_working["line_number"], errors="coerce"),
            "GL Account Number": ledger_working["gl_account_number"],
            "GL Account Name": ledger_working["gl_account_name"],
            "Description": ledger_working["description"],
            "Vendor / Customer Name": ledger_working["vendor_customer_name"],
            "Source Document": ledger_working["source_document"],
            "Source Reference": ledger_working["source_reference"],
            "Debit Amount": ledger_working["debit_amount_numeric"],
            "Credit Amount": ledger_working["credit_amount_numeric"],
            "Balance After Posting": pd.to_numeric(ledger_working["balance_after_posting"], errors="coerce"),
            "Currency": ledger_working["currency"],
            "Reconciliation Candidate": _yes_no(ledger_working["recon_candidate"]),
            "Reconciliation Exclusion Reason": ledger_working["recon_exclusion_reason"].fillna(""),
        }
    )


def build_final_output_views(
    result: ReconciliationResult,
) -> Dict[str, pd.DataFrame]:
    return {
        "Reconciliation Summary": build_final_summary_view(
            reconciled_matches=result.reconciled_matches,
            exceptions_report=result.exceptions_report,
            bank_working=result.bank_working,
            ledger_working=result.ledger_working,
        ),
        "Reconciled Matches": build_final_reconciled_matches_view(
            reconciled_matches=result.reconciled_matches,
            bank_working=result.bank_working,
            ledger_working=result.ledger_working,
        ),
        "Exceptions Report": build_final_exceptions_view(result.exceptions_report),
        "Legend": build_legend_view(),
        "Clean Bank Data": build_clean_bank_view(result.bank_working),
        "Clean Ledger Data": build_clean_ledger_view(result.ledger_working),
    }


def save_final_output_views(output_views: Dict[str, pd.DataFrame], output_dir: str | Path) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_map = {
        "Reconciliation Summary": "reconciliation_summary.csv",
        "Reconciled Matches": "reconciled_matches.csv",
        "Exceptions Report": "exceptions_report.csv",
        "Legend": "legend.csv",
        "Clean Bank Data": "clean_bank_data.csv",
        "Clean Ledger Data": "clean_ledger_data.csv",
    }

    for sheet_name, dataframe in output_views.items():
        dataframe.to_csv(output_dir / file_map[sheet_name], index=False)
