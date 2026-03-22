"""Configuration loading for pharmacogenomics tool."""

import json
import os
from pathlib import Path

from pydantic import BaseModel


class ToolConfig(BaseModel):
    cpic_base_url: str = "https://api.cpicpgx.org/v1"
    pharmcat_docker_image: str = "pgkb/pharmcat"
    pharmcat_timeout: int = 600
    threads: int = 4


def _deep_merge(base: dict, override: dict) -> dict:
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_defaults() -> dict:
    defaults_path = Path(__file__).resolve().parent.parent.parent / "config.json"
    if defaults_path.exists():
        with open(defaults_path) as f:
            return json.load(f)
    return {}


def load_config(config_path: str | None = None) -> ToolConfig:
    data = _load_defaults()

    if config_path:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with open(path) as f:
            overrides = json.load(f)
        data = _deep_merge(data, overrides)

    if os.environ.get("CPIC_BASE_URL"):
        data["cpic_base_url"] = os.environ["CPIC_BASE_URL"]
    if os.environ.get("PHARMCAT_DOCKER_IMAGE"):
        data["pharmcat_docker_image"] = os.environ["PHARMCAT_DOCKER_IMAGE"]

    return ToolConfig.model_validate(data)
