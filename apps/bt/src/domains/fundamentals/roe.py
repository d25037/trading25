"""ROE domain calculations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


_QUARTER_MULTIPLIER: dict[str, float] = {
    "1Q": 4.0,
    "2Q": 2.0,
    "3Q": 4.0 / 3.0,
}


@dataclass(frozen=True)
class ROEComputationMetadata:
    code: str
    period_type: str
    period_end: str
    is_consolidated: bool
    accounting_standard: str | None
    is_annualized: bool


@dataclass(frozen=True)
class ROEComputationResult:
    roe: float
    net_profit: float
    equity: float
    metadata: ROEComputationMetadata


def normalize_period_type(cur_per_type: str) -> str:
    if not cur_per_type:
        return "FY"
    t = cur_per_type.strip()
    if t in ("FY", "1Q", "2Q", "3Q"):
        return t
    upper = t.upper()
    if "1Q" in upper or "Q1" in upper:
        return "1Q"
    if "2Q" in upper or "Q2" in upper or "HALF" in upper:
        return "2Q"
    if "3Q" in upper or "Q3" in upper:
        return "3Q"
    return "FY"


def is_quarterly(period_type: str) -> bool:
    return period_type in ("1Q", "2Q", "3Q")


def is_consolidated_doc(doc_type: str | None) -> bool:
    if not doc_type:
        return True
    return "consolidated" in doc_type.lower()


def extract_accounting_standard(doc_type: str | None) -> str | None:
    if not doc_type:
        return "JGAAP"
    lower = doc_type.lower()
    if "ifrs" in lower:
        return "IFRS"
    if "us" in lower and "gaap" in lower:
        return "US GAAP"
    return "JGAAP"


def calculate_single_roe(
    stmt: dict[str, Any],
    annualize: bool = True,
    prefer_consolidated: bool = True,
    min_equity: float = 1000,
) -> ROEComputationResult | None:
    if prefer_consolidated:
        net_profit = stmt.get("NP") or stmt.get("NCNP")
        equity = stmt.get("Eq") or stmt.get("NCEq")
    else:
        net_profit = stmt.get("NCNP") or stmt.get("NP")
        equity = stmt.get("NCEq") or stmt.get("Eq")

    if net_profit is None or equity is None:
        return None
    if abs(equity) < min_equity or equity <= 0:
        return None

    period_type = normalize_period_type(stmt.get("CurPerType", ""))
    adjusted_profit = net_profit
    is_annualized = False
    if annualize and is_quarterly(period_type):
        adjusted_profit = net_profit * _QUARTER_MULTIPLIER.get(period_type, 1.0)
        is_annualized = True

    roe = (adjusted_profit / equity) * 100
    code = str(stmt.get("Code", ""))[:4]

    return ROEComputationResult(
        roe=round(roe, 4),
        net_profit=adjusted_profit,
        equity=equity,
        metadata=ROEComputationMetadata(
            code=code,
            period_type=period_type,
            period_end=stmt.get("CurPerEn", ""),
            is_consolidated=is_consolidated_doc(stmt.get("DocType")),
            accounting_standard=extract_accounting_standard(stmt.get("DocType")),
            is_annualized=is_annualized,
        ),
    )


def should_prefer(new_stmt: dict[str, Any], current_stmt: dict[str, Any]) -> bool:
    new_type = normalize_period_type(new_stmt.get("CurPerType", ""))
    cur_type = normalize_period_type(current_stmt.get("CurPerType", ""))

    if new_type == "FY" and cur_type != "FY":
        return True
    if new_type != "FY" and cur_type == "FY":
        return False

    new_end = new_stmt.get("CurPerEn", "")
    cur_end = current_stmt.get("CurPerEn", "")
    return new_end > cur_end
