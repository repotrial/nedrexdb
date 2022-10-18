import os
from typing import Literal
from pathlib import Path
from datetime import datetime

from nedrexdb import config as _config
from nedrexdb.logger import logger
from nedrexdb.exceptions import NeDRexError

from pymongo import MongoClient  # type: ignore
from pymongo.database import Database  # type: ignore
from pymongo.errors import ServerSelectionTimeoutError  # type: ignore


def get_nedrex_version(db: Database, default_version="2.0.0"):
    metadata = list(db["metadata"].find())
    if len(metadata) > 1:
        raise NeDRexError("metadata collection has more than one document in it")

    if not metadata:
        return default_version
    metadata_doc = metadata[0]
    if not metadata_doc.get("version"):
        raise NeDRexError("metadata document does not have a version")
    return metadata_doc["version"]


def check_mongo_instance_exists(client: MongoClient):
    logger.debug(f"Checking {client} for connection")
    try:
        client.server_info()
    except ServerSelectionTimeoutError:
        logger.debug("Got a SeverSelectionTimeoutError")
        return False
    logger.debug("Successfully connected to client")
    return True


def update_version(version, part_to_update: Literal["major", "minor", "patch", None], pre_release=None, build=None):
    version = version.split(".")

    if part_to_update == "major":
        version[0] = f"{int(version[0]) + 1}"
    elif part_to_update == "minor":
        version[1] = f"{int(version[1]) + 1}"
    elif part_to_update == "patch":
        version[2] = f"{int(version[2]) + 1}"

    # join version as a string
    version = ".".join(version)

    if pre_release:
        version = f"{version}-{pre_release}"
    if build:
        version = f"{version}+{build}"

    return version


def generate_update_document(version, data_directory):
    sources = list(_config["sources"].keys())
    sources.remove("directory")

    metadata = {"version": version, "source_databases": {}}
    for source in sources:
        dir = data_directory / source
        earliest_date = datetime.now()

        for _, options in _config[f"sources.{source}"].items():
            if "filename" in options:
                filename = options["filename"]
            else:
                filename = options["url"].rsplit("/", 1)[1]

            path = dir / filename
            ts = datetime.fromtimestamp(os.path.getctime(path))
            if ts < earliest_date:
                earliest_date = ts

            metadata["source_databases"][source] = {"version": None, "date": f"{earliest_date.date()}"}

    return metadata


def update_db_version(default_version="2.0.0"):
    logger.info("Updating NeDRexDB version number")

    mongo_dbname = _config["db.mongo_db"]
    mongo_dev_port = _config["db.dev.mongo_port"]
    mongo_live_port = _config["db.live.mongo_port"]

    download_directory = Path(_config["db.root_directory"]) / _config["sources.directory"]

    logger.debug("Creating client for live DB")
    live_client = MongoClient(port=mongo_live_port)
    logger.debug(f"Created client with port {mongo_live_port}")
    live_db = live_client[mongo_dbname]
    logger.debug(f"Created DB instance with DB name {mongo_dbname}")

    if not check_mongo_instance_exists(live_client):
        logger.debug(f"Live version of NeDRexDB not found, using default version number of {default_version!r}")
        current_version = default_version
    else:
        logger.debug("Live version of NeDRexDB exists - getting version of the current live instance")
        current_version = get_nedrex_version(live_db, default_version=default_version)
        logger.debug(f"Current version found: {current_version!r}")

    logger.debug("Closing client to live version of NeDRexDB")
    live_client.close()

    logger.debug("Incrementing version number for NeDRexDB")
    next_version = update_version(current_version, "minor")
    logger.debug(f"New version number is {next_version!r}")
    logger.debug("Generating metadata")
    metadata = generate_update_document(next_version, download_directory)

    logger.debug("Creating development client")
    dev_client = MongoClient(port=mongo_dev_port)
    dev_db = dev_client[mongo_dbname]
    logger.debug("Setting metadata in the development")
    dev_db["metadata"].replace_one({}, metadata, upsert=True)
