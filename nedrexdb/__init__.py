__version__ = "0.1.0"

from dataclasses import dataclass as _dataclass
from pprint import pformat as _pformat
from typing import Any as _Any, Optional as _Optional

import toml as _toml  # type: ignore

from nedrexdb.exceptions import ConfigError as _ConfigError
from nedrexdb.logger import logger


@_dataclass
class _Config:
    data: _Optional[dict[_Any, _Any]] = None

    def __repr__(self):
        return _pformat(self.data)

    def from_file(self, infile):
        with open(infile, "r") as f:
            self.data = _toml.load(f)

    def __getitem__(self, path):
        if self.data is None:
            raise _ConfigError("config has not been parsed (currently None)")

        split_path = path.split(".")
        current = self.data

        for idx, val in enumerate(split_path):
            current = current.get(val)
            if current is None:
                failed_path = ".".join(split_path[: idx + 1])
                raise _ConfigError(f"{failed_path!r} is not in config")

        return current

    def get(self, path):
        try:
            return self[path]
        except _ConfigError:
            return None


config = _Config()


def parse_config(infile):
    global config
    logger.info("Parsing config file: %s" % infile)
    config.from_file(infile)
