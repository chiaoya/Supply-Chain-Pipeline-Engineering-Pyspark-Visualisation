from ml_engineering_pyspark.utils.config_loader import load_config


def test_load_config_reads_expected_sections():
    config = load_config("configs/base.yaml")

    assert "spark" in config
    assert "database" in config
    assert "tables" in config
    assert "paths" in config
