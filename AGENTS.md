# Finance Reconciliation Project Rules

## Scope
- Version 1 only
- January 2024 only
- One entity only: MedSupplyCo
- One currency only: USD
- No prior-period carry-forward
- No opening balance bridge

## Official source foundations
- data_raw/bank_transactions_sample.csv
- data_raw/ledger_transactions_sample.csv
- data_raw/payment_obligations_sample.csv
- docs/Finance_Reconciliation_V1_Final_Logic_Updated.docx

## Official workflow
raw CSV -> clean / standardize -> reconcile -> classify exceptions / variance -> review next steps

## Core logic rules
- Primary reconciliation is Bank vs Ledger
- Payment Obligations is a later support / cash-flow extension layer, not the core matching engine
- Treat the raw CSVs as the official starting inputs
- Do not assume prepared or transformed files are the starting point
- Any cleaned, prepared, or intermediate files are workflow outputs, not source foundations
- Follow the Final Version 1 Reconciliation Logic document unless explicitly revised

## Final Version 1 output rules
- The reconciliation must read clearly to an accountant or finance reviewer without requiring interpretation
- Reconciled matches may remain matched while still being marked Review Required when documentation quality is weak
- Missing or unusable references must not be silently auto-cleared as fully supported matches
- The Exceptions Report should be human-facing and reviewable
- Exception display amounts should be positive unsigned values in the final report
- Exception cash direction should be shown in a separate standardized field based on cash movement perspective
- Internal signed cash-effect logic may remain separate from final display fields
- The Reconciliation Summary must prove the close through adjusted bank balance versus adjusted ledger balance
- Net unresolved cash variance is the remaining difference between adjusted bank and adjusted ledger
- Open-item rollups may be shown as supporting analysis, but not as the primary close result

## Working style rules
- Keep changes manageable and targeted
- Do not redesign the project unless required to fix a real bug or approved business-rule issue
- Do not invent new business rules
- Prefer minimal, realistic fixes
- Preserve business realism and portfolio quality
- Keep logic readable and easy to review
- Be careful with pandas merge suffix assumptions
- Normalize Yes/No flags safely before boolean filtering
- Keep output labels business-friendly and defensible
- Any accounting-facing output must read clearly to a finance or accounting reviewer without requiring interpretation

## Implementation guardrails
- Fix root causes before changing downstream outputs
- Do not hardcode around bad logic if the underlying issue can be corrected cleanly
- Do not rename output columns unless required by the approved Version 1 logic or final presentation rules
- Preserve existing folder structure unless a real issue requires change
- Prefer explicit column checks before sorting, grouping, or merging
- Keep bank-specific and ledger-specific fields clearly separated
- Use source_record_id only where a shared exception/output identifier is appropriate
- Keep internal logic fields separate from final human-facing presentation fields
- Distinguish clearly between raw source conventions, normalized comparison fields, and final report display fields

## Current implementation path
- Main run command: python src/main.py
- Main code files are under src/
- Outputs are written to output/
- Cleaned files are written to data_clean/

## Definition of done for Version 1 tasks
- The script runs without traceback
- Outputs regenerate successfully
- Reconciliation logic stays within Version 1 scope
- The workbook opens cleanly and presents results clearly
- Summary reporting proves the reconciliation close in accountant-friendly terms
- Exception reporting remains realistic, reviewable, and traceable

## Codex usage guidance
- Codex is being used as an implementation assistant, not as the decision-maker on accounting logic
- Use Codex for targeted debugging, controlled code edits, traceback resolution, refactors that preserve logic, and output generation fixes
- Do not let Codex redefine reconciliation policy, accounting treatment, or Version 1 business rules unless explicitly instructed
- If a problem appears to involve accounting interpretation, stop and flag it for review before changing logic
- Prefer asking Codex for minimal root-cause fixes over broad rewrites
- When helpful, use Codex to draft code patches, validate likely bug locations, or tighten workbook/output formatting
- If a task would benefit from Codex, explicitly say so and define the task narrowly before applying changes
