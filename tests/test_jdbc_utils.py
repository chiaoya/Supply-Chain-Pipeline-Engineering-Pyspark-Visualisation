from ml_engineering_pyspark.utils.jdbc import (
    normalize_optional_path,
    normalize_table_name,
    resolve_db_password,
    resolve_env_or_value,
)


def test_resolve_db_password_prefers_explicit_password(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "from-env")

    assert resolve_db_password("from-arg", "DB_PASSWORD") == "from-arg"


def test_resolve_db_password_reads_from_env(monkeypatch):
    monkeypatch.setenv("MY_DB_PASSWORD", "super-secret")

    assert resolve_db_password(None, "MY_DB_PASSWORD") == "super-secret"


def test_normalize_optional_path_handles_none_and_path():
    assert normalize_optional_path(None) is None
    assert normalize_optional_path("driver.jar") == "driver.jar"


def test_resolve_env_or_value_reads_explicit_value_first(monkeypatch):
    monkeypatch.setenv("DB_USER", "from-env")

    assert resolve_env_or_value("from-arg", "DB_USER") == "from-arg"


def test_resolve_env_or_value_reads_from_env(monkeypatch):
    monkeypatch.setenv("JDBC_URL", "jdbc:postgresql://localhost:5432/demo")

    assert resolve_env_or_value(None, "JDBC_URL") == "jdbc:postgresql://localhost:5432/demo"


def test_normalize_table_name_adds_public_schema_by_default():
    assert normalize_table_name("orders") == "public.orders"
    assert normalize_table_name("mart.orders") == "mart.orders"
