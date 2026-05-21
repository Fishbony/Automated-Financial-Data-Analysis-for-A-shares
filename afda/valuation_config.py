"""Configuration loader for valuation assumptions."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_DIR / "configs" / "default_valuation.json"


DEFAULT_CONFIG: dict[str, Any] = {
    "industry_profile": "general_industrial",
    "dcf": {
        "wacc": 0.10,
        "terminal_growth": 0.03,
        "dcf_weight": 0.60,
        "relative_weight": 0.40,
    },
    "relative_valuation": {
        "multiples": {
            "PE": {"low": 18.0, "mid": 22.0, "high": 26.0},
            "PB": {"low": 3.0, "mid": 3.8, "high": 4.5},
            "EV/EBIT": {"low": 16.0, "mid": 20.0, "high": 24.0},
            "EV/EBITDA": {"low": 13.0, "mid": 16.0, "high": 19.0},
            "PS": {"low": 2.0, "mid": 2.5, "high": 3.0},
        }
    },
    "sensitivity": {
        "wacc": [0.08, 0.09, 0.10, 0.11, 0.12],
        "terminal_growth": [0.01, 0.02, 0.03, 0.04, 0.05],
    },
}


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_update(out[key], value)
        else:
            out[key] = value
    return out


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _flatten_keys(value: Any, prefix: str = "") -> set[str]:
    if not isinstance(value, dict):
        return {prefix} if prefix else set()
    keys: set[str] = set()
    for key, child in value.items():
        child_prefix = f"{prefix}.{key}" if prefix else str(key)
        keys.update(_flatten_keys(child, child_prefix))
    return keys


def load_valuation_config(data_dir: Path | str | None = None) -> dict[str, Any]:
    """Load project defaults, then optionally override with data-dir config."""

    config = deepcopy(DEFAULT_CONFIG)
    if DEFAULT_CONFIG_PATH.exists():
        config = _deep_update(config, _read_json(DEFAULT_CONFIG_PATH))

    if data_dir is not None:
        local_path = Path(data_dir) / "valuation_config.json"
        if local_path.exists():
            config = _deep_update(config, _read_json(local_path))

    return config


def valuation_config_source_map(data_dir: Path | str | None = None) -> dict[str, str]:
    """Return where each valuation config leaf came from."""

    sources = {key: "builtin defaults" for key in _flatten_keys(DEFAULT_CONFIG)}
    if DEFAULT_CONFIG_PATH.exists():
        sources.update({key: f"project default: {DEFAULT_CONFIG_PATH.name}" for key in _flatten_keys(_read_json(DEFAULT_CONFIG_PATH))})

    if data_dir is not None:
        local_path = Path(data_dir) / "valuation_config.json"
        if local_path.exists():
            sources.update({key: f"local override: {local_path.name}" for key in _flatten_keys(_read_json(local_path))})

    return sources


def get_multiple(config: dict[str, Any], name: str) -> dict[str, float]:
    multiples = config.get("relative_valuation", {}).get("multiples", {})
    value = multiples.get(name, DEFAULT_CONFIG["relative_valuation"]["multiples"][name])
    return {
        "low": float(value["low"]),
        "mid": float(value["mid"]),
        "high": float(value["high"]),
    }
