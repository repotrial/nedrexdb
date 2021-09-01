from mongoengine import connect as _connect, disconnect as _disconnect

from nedrexdb import config as _config


def connect(version):
    if version not in ("live", "dev"):
        raise ValueError(f"version given ({version!r}) should be 'live' or 'dev")

    port = _config[f"db.{version}.port"]
    host = _config[f"db.{version}.url"]
    dbname = _config[f"db.mongo_db"]

    # NOTE: This doesn't seem to complain if a connection with the alias already exists.
    _disconnect(alias="nedrexdb")
    _connect(db=dbname, alias="nedrexdb", host=host, port=port)
