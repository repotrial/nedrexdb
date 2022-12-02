#!/usr/bin/env python3

import datetime
import os

import click
import toml  # type: ignore
from pymongo import MongoClient


@click.command()
@click.option("--config", required=True, type=click.Path(exists=True))
@click.option("--version", required=True, type=click.Choice(["live", "dev"], case_sensitive=False))
def update(config, version):
    """Updates the metadata collection based on the given config"""
    with open(config) as f:
        config = toml.load(f)

    mongo_port = config["db"][version.lower()]["mongo_port"]
    db_name = config["db"]["mongo_db"]
    data_directory_root = config["db"]["root_directory"]
    download_directory = f"{data_directory_root}/{config['sources']['directory']}"

    sources = list(config["sources"].keys())
    sources.remove("directory")
    try:
        client = MongoClient(port=mongo_port)
        db = client[db_name]

        metadata = db["metadata"].find_one()
    except:
        metadata = None
    metadata = metadata if metadata is not None else {"version": "0.0.0"}
    if "source_databases" not in metadata:
        metadata["source_databases"] = {}

    for source in sources:
        dir = f"{download_directory}/{source}"
        earliest_date = datetime.datetime.now()
        for _, options in config["sources"][source].items():
            if "filename" in options:
                filename = options["filename"]
            else:
                filename = options["url"].rsplit("/", 1)[1]

            path = f"{dir}/{filename}"
            ts = datetime.datetime.fromtimestamp(os.path.getctime(path))
            if ts < earliest_date:
                ts = earliest_date

        metadata["source_databases"][source] = {"version": None, "date": f"{earliest_date.date()}"}


    db["metadata"].replace_one({}, metadata, upsert=True)


if __name__ == "__main__":
    update()
