from tempfile import NamedTemporaryFile as NTF

import pytest

import nedrexdb
from nedrexdb.exceptions import ConfigError


def test_version() -> None:
    assert nedrexdb.__version__ == "0.1.0"


@pytest.fixture
def example_config():
    example_config = '[test]\nname="James"\n[test.inner]\nvar=2'

    with NTF(suffix=".toml", mode="w") as f:
        f.write(example_config)
        f.flush()
        nedrexdb.parse_config(f.name)
        yield

    # Reset config
    nedrexdb.config.data = None


class TestConfig:
    def test_parse_config(self, example_config):
        expected = {"test": {"name": "James", "inner": {"var": 2}}}
        assert nedrexdb.config.data == expected

    def test_config_successful_get(self, example_config):
        assert nedrexdb.config.get("test.name") == "James"
        assert nedrexdb.config.get("test.inner.var") == 2

    def test_config_unsuccessful_get(self, example_config):
        assert nedrexdb.config.get("test.none") is None

    def test_config_successful_getitem(self, example_config):
        assert nedrexdb.config["test.name"] == "James"
        assert nedrexdb.config["test.inner.var"] == 2

    def test_config_unsuccessful_getitem(self, example_config):
        with pytest.raises(ConfigError):
            nedrexdb.config["test.none"]

    def test_unparsed_config(self):
        # Config hasn't been parsed.
        assert nedrexdb.config.data is None
        with pytest.raises(ConfigError):
            nedrexdb.config["test.name"]
