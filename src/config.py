from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"
OUTPUT_DIR = BASE_DIR / "output"
DOCS_DIR = BASE_DIR / "docs"

BANK_FILE = RAW_DIR / "bank_transactions_sample.csv"
LEDGER_FILE = RAW_DIR / "ledger_transactions_sample.csv"
OBLIGATIONS_FILE = RAW_DIR / "payment_obligations_sample.csv"

BANK_CLEAN_FILE = CLEAN_DIR / "bank_transactions_cleaned.csv"
LEDGER_CLEAN_FILE = CLEAN_DIR / "ledger_transactions_cleaned.csv"
OBLIGATIONS_CLEAN_FILE = CLEAN_DIR / "payment_obligations_cleaned.csv"

REPORTING_MONTH = "2024-01"
REPORTING_MONTH_LABEL = "January 2024"
ENTITY_SCOPE = "MedSupplyCo"
CURRENCY_SCOPE = "USD"
PERIOD_START_DATE = "2024-01-01"
PERIOD_END_DATE = "2024-01-31"
AGING_REPORT_DATE = "2024-01-31"

ALLOWED_BANK_DEBIT_CREDIT = {"DEBIT", "CREDIT"}
ALLOWED_BANK_STANDARDIZED_DEBIT_CREDIT = {"DR", "CR"}
ALLOWED_LEDGER_DEBIT_CREDIT = {"DR", "CR"}

RULE2_DATE_GAP_DAYS = 5
RULE3_DATE_GAP_DAYS = 5
RULE4_VARIANCE_TOLERANCE = 250.00
DUPLICATE_WINDOW_DAYS = 3

BANK_REQUIRED_COLUMNS = [
    "statement_period_start",
    "statement_period_end",
    "statement_opening_balance",
    "entity_name",
    "transaction_date",
    "value_date",
    "description",
    "bank_reference",
    "counterparty_name",
    "debit_credit",
    "amount",
    "balance_after_txn",
    "bank_account_number",
    "currency",
]

LEDGER_REQUIRED_COLUMNS = [
    "ledger_period_start",
    "ledger_period_end",
    "ledger_opening_balance",
    "entity_name",
    "posting_date",
    "journal_reference",
    "line_number",
    "gl_account_number",
    "gl_account_name",
    "description",
    "source_document",
    "source_reference",
    "debit_amount",
    "credit_amount",
    "balance_after_posting",
    "currency",
]

OBLIGATIONS_REQUIRED_COLUMNS = [
    "aging_report_date",
    "entity_name",
    "vendor_name",
    "invoice_number",
    "invoice_date",
    "due_date",
    "original_amount",
    "paid_amount",
    "open_amount",
    "days_past_due",
    "aging_bucket",
    "status",
    "currency",
]

AGING_BUCKET_RULES = [
    ("Current", lambda days: days <= 0),
    ("1-30 days", lambda days: 1 <= days <= 30),
    ("31-60 days", lambda days: 31 <= days <= 60),
    ("61-90 days", lambda days: 61 <= days <= 90),
    ("90+ days", lambda days: days >= 91),
]


def ensure_directories() -> None:
    """Create expected project output directories if they do not already exist."""
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
