from dataclasses import dataclass as _dataclass

from pymongo import MongoClient as _MongoClient

from nedrexdb import config as _config
from nedrexdb.db.models.nodes import disorder as _disorder


@_dataclass
class MongoInstance:
    CLIENT = None
    DB = None

    @classmethod
    def connect(cls, version):
        if version not in ("live", "dev"):
            raise ValueError(f"version given ({version!r}) should be 'live' or 'dev")

        port = _config[f"db.{version}.mongo_port"]
        host = "localhost"
        dbname = _config["db.mongo_db"]

        cls.CLIENT = _MongoClient(host=host, port=port)
        cls.DB = cls.CLIENT[dbname]

    @classmethod
    def set_indexes(cls):
        if not cls.DB:
            raise ValueError("run nedrexdb.db.connect() first to connect to MongoDB")
        _disorder.Disorder.set_indexes(cls.DB)
