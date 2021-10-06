import subprocess as _sp
from pathlib import Path as _Path

from requests.exceptions import HTTPError as _HTTPError

from nedrexdb import config as _config
from nedrexdb.common import Downloader, change_directory
from nedrexdb.logger import logger


def download_drugbank():
    root = _Path(_config["db.root_directory"]) / _config["sources.directory"]
    target_dir = root / "drugbank"
    target_dir.mkdir(exist_ok=True, parents=True)

    biogrid = _config["sources.drugbank"]
    biogrid_all = biogrid["all"]

    url = biogrid_all["url"]
    zip_fname = target_dir / "all.zip"
    target_fname = (target_dir / biogrid_all["filename"]).resolve()

    d = Downloader(
        url=url,
        target=zip_fname,
        username=biogrid["username"],
        password=biogrid["password"],
    )
    try:
        d.download()
    except _HTTPError as E:
        logger.warning(f"Unable to download DrugBank: {E}")
        return

    # Unzip the zip
    with change_directory(target_dir):
        _sp.call(["unzip", f"{zip_fname.resolve()}"])
        zip_fname.unlink()

    # Move the unzipped file to the desired target fname.
    files = list(target_dir.iterdir())
    assert len(files) == 1
    files.pop().rename(target_fname)
