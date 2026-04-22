from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from tidydag.node import Configuration

ASSETS = Path(__file__).parent.parent / "assets"


class ServerConfig(Configuration):
    host: str
    port: int
    retries: int = 3


def test_valid_yaml_loads():
    config = ServerConfig.from_yaml(ASSETS / "server_valid.yaml")

    assert config.host == "localhost"
    assert config.port == 8080


def test_from_yaml_accepts_string_path():
    config = ServerConfig.from_yaml(str(ASSETS / "server_valid.yaml"))

    assert config.host == "localhost"
    assert config.port == 8080


def test_optional_field_uses_default_when_absent():
    config = ServerConfig.from_yaml(ASSETS / "server_valid.yaml")

    assert config.retries == 3


def test_optional_field_overridden_in_yaml():
    config = ServerConfig.from_yaml(ASSETS / "server_with_retries.yaml")

    assert config.retries == 10


def test_wrong_type_raises_validation_error():
    with pytest.raises(ValidationError):
        ServerConfig.from_yaml(ASSETS / "server_wrong_port_type.yaml")


def test_missing_required_field_raises_validation_error():
    with pytest.raises(ValidationError):
        ServerConfig.from_yaml(ASSETS / "server_missing_port.yaml")


def test_nested_configuration_loads():
    class DatabaseConfig(Configuration):
        url: str
        pool_size: int

    class AppConfig(Configuration):
        name: str
        database: DatabaseConfig

    config = AppConfig.from_yaml(ASSETS / "app_valid.yaml")

    assert config.name == "myapp"
    assert config.database.url == "postgres://localhost/db"
    assert config.database.pool_size == 5


def test_wrong_type_in_nested_config_raises_validation_error():
    class DatabaseConfig(Configuration):
        url: str
        pool_size: int

    class AppConfig(Configuration):
        name: str
        database: DatabaseConfig

    with pytest.raises(ValidationError):
        AppConfig.from_yaml(ASSETS / "app_wrong_pool_size.yaml")
