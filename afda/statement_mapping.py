"""Statement item matching helpers shared by rebuild modules."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd


LEADING_MARKERS = (
    "*",
    "加:",
    "加：",
    "减:",
    "减：",
    "其中:",
    "其中：",
)


def normalize_item_name(name: object) -> str:
    if pd.isna(name):
        return ""
    text = str(name).strip().replace("\ufeff", "")
    text = re.sub(r"\s+", "", text)
    return text


def item_match_key(name: object) -> str:
    text = normalize_item_name(name)
    changed = True
    while changed:
        changed = False
        for marker in LEADING_MARKERS:
            if text.startswith(marker):
                text = text[len(marker) :]
                changed = True
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"\((元|万元|亿元|人民币元|人民币万元)\)$", "", text)
    text = text.replace("、", "").replace(",", "").replace("，", "")
    return text.lower()


def build_item_lookup(items: Iterable[object]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for item in items:
        normalized = normalize_item_name(item)
        if not normalized:
            continue
        lookup.setdefault(normalized, normalized)
        lookup.setdefault(item_match_key(normalized), normalized)
    return lookup


def resolve_item_name(available_items: Iterable[object], candidate: object) -> str | None:
    lookup = build_item_lookup(available_items)
    normalized = normalize_item_name(candidate)
    return lookup.get(normalized) or lookup.get(item_match_key(normalized))


def resolve_source_items(available_items: Iterable[object], candidates: Iterable[object]) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        match = resolve_item_name(available_items, candidate)
        if match is not None and match not in seen:
            resolved.append(match)
            seen.add(match)
    return resolved


def sum_source_items(
    df: pd.DataFrame,
    item_col: str,
    year_cols: list[str],
    candidates: Iterable[object],
) -> pd.Series:
    resolved = resolve_source_items(df[item_col].tolist(), candidates)
    if not resolved:
        return pd.Series([0.0] * len(year_cols), index=year_cols)
    return df.loc[df[item_col].isin(resolved), year_cols].sum()


def describe_source_matches(
    available_items: Iterable[object],
    candidates: Iterable[object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for candidate in candidates:
        match = resolve_item_name(available_items, candidate)
        rows.append(
            {
                "requested_item": normalize_item_name(candidate),
                "matched_item": match or "",
                "exists": match is not None,
                "match_type": "exact" if match == normalize_item_name(candidate) else "alias" if match else "missing",
            }
        )
    return rows


def load_mapping_rules(config_name: str) -> List[Dict]:
    """Load mapping rules from a JSON config file.

    Looks for ``configs/mapping_{config_name}.json`` relative to the project
    root (the parent of the ``afda/`` package directory).  Falls back to the
    built-in defaults defined in the rebuild modules if the file is missing.
    """
    config_path = Path(__file__).resolve().parent.parent / "configs" / f"mapping_{config_name}.json"
    if not config_path.exists():
        raise FileNotFoundError(
            f"Mapping config file not found: {config_path}. "
            f"Expected config_name one of: 'balance_sheet', 'income_statement', 'cash_flow'."
        )
    with config_path.open(encoding="utf-8") as f:
        rules = json.load(f)
    if not isinstance(rules, list):
        raise ValueError(f"Mapping config {config_path} must contain a JSON list of rule dicts.")
    return rules
