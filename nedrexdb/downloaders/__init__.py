from pathlib import Path as _Path

from nedrexdb import config as _config
from nedrexdb.common import Downloader
from nedrexdb.downloaders.biogrid import download_biogrid as _download_biogrid
from nedrexdb.downloaders.drugbank import (
    download_drugbank as _download_drugbank,
)


def download_all():
    base = _Path(_config["db.root_directory"])
    download_dir = base / _config["sources.directory"]
    download_dir.mkdir(exist_ok=True, parents=True)

    sources = _config["sources"]
    # Remove the source keys
    exclude_keys = {"directory", "username", "password"}

    for source in filter(lambda i: i not in exclude_keys, sources):
        # Catch case to skip sources with bespoke downloaders.
        if source in {"biogrid", "drugbank"}:
            pass

        # TODO: Implement force-overwrites
        (download_dir / source).mkdir(exist_ok=True)

        data = sources[source]
        username = data.get("username")
        password = data.get("password")

        for _, download in filter(lambda i: i[0] not in exclude_keys, data.items()):
            url = download.get("url")
            filename = download.get("filename")
            if filename is None:
                filename = url.rsplit("/", 1)[1]

            d = Downloader(
                url=url,
                target=download_dir / source / filename,
                username=username,
                password=password,
            )
            d.download()

    _download_biogrid()
    _download_drugbank()
