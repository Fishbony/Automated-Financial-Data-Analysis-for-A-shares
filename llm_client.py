from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional
from urllib import error, request


DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MODEL = "deepseek-v4-flash"
DEFAULT_TIMEOUT = 120


def _strip_wrapping_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_project_env(env_path: Optional[Path] = None) -> None:
    """
    Load simple KEY=VALUE pairs from the project's .env file into os.environ.

    Existing environment variables win so shell-provided overrides still work.
    """
    target = env_path or Path(__file__).resolve().with_name(".env")
    if not target.exists():
        return

    for raw_line in target.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        os.environ.setdefault(key, _strip_wrapping_quotes(value.strip()))


load_project_env()


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def deepseek_enabled() -> bool:
    return os.getenv("ENABLE_DEEPSEEK_ANALYSIS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def deepseek_configured() -> bool:
    return bool(os.getenv("DEEPSEEK_API_KEY", "").strip())


def generate_deepseek_analysis(
    *,
    report_context: str,
    model: Optional[str] = None,
    system_prompt: Optional[str] = None,
    temperature: float = 0.2,
) -> str:
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is not set.")

    base_url = _normalize_base_url(os.getenv("DEEPSEEK_BASE_URL", DEFAULT_BASE_URL))
    target_model = (model or os.getenv("DEEPSEEK_MODEL", DEFAULT_MODEL)).strip() or DEFAULT_MODEL
    timeout = int(os.getenv("DEEPSEEK_TIMEOUT", str(DEFAULT_TIMEOUT)))

    payload = {
        "model": target_model,
        "temperature": temperature,
        "messages": [
            {
                "role": "system",
                "content": system_prompt
                or (
                    "You are a careful A-share equity research analyst. "
                    "Write in Chinese. Stay grounded in the provided numbers, "
                    "do not invent facts, and clearly separate observation from inference."
                ),
            },
            {"role": "user", "content": report_context},
        ],
    }

    req = request.Request(
        url=f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"DeepSeek API request failed: HTTP {exc.code} {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"DeepSeek API network error: {exc.reason}") from exc

    data = json.loads(body)
    try:
        return data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected DeepSeek API response: {body}") from exc
