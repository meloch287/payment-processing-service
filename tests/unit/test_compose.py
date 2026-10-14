import re
from pathlib import Path
from typing import Any

import yaml

from app.config import Settings

ROOT = Path(__file__).resolve().parents[2]
NOT_FORWARDED = {
    "DATABASE_URL",
    "RABBITMQ_URL",
    "POSTGRES_PORT",
    "RABBITMQ_PORT",
    "RABBITMQ_VHOST",
}


def compose() -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    return data


def test_every_setting_reaches_app_containers() -> None:
    services = compose()["services"]
    expected = {name.upper() for name in Settings.model_fields} - NOT_FORWARDED
    for service in ("api", "consumer"):
        assert expected <= set(services[service]["environment"]), service


def test_every_forwarded_setting_is_documented_in_env_example() -> None:
    environment = compose()["services"]["api"]["environment"]
    example = {
        line.split("=", 1)[0]
        for line in (ROOT / ".env.example").read_text().splitlines()
        if "=" in line
    }
    forwarded = {
        match for value in environment.values() for match in re.findall(r"\$\{(\w+)", str(value))
    }
    assert forwarded <= example


def test_api_port_is_configurable() -> None:
    assert compose()["services"]["api"]["ports"] == ["${API_PORT:-8000}:8000"]
