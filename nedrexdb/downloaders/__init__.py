from pathlib import Path as _Path
from typing import Optional as _Optional

import requests as _requests  # type: ignore
from pydantic import BaseModel as _BaseModel, validator as _validator
from tqdm import tqdm as _tqdm

from nedrexdb import config as _config
from nedrexdb.logger import logger as _logger
from nedrexdb.downloaders.biogrid import download_biogrid as _download_biogrid


class Downloader(_BaseModel):
    url: str
    target: _Path
    username: _Optional[str]
    password: _Optional[str]

    @_validator("url")
    def url_https_or_http(cls, v):
        if any(v.startswith(i) for i in ("http://", "https://")):
            return v
        else:
            raise ValueError(f"url {v!r} is not http(s)")

    def download(self):
        if self.username is None and self.password is None:
            auth = None
        elif self.username is not None and self.password is not None:
            auth = (self.username, self.password)
        else:
            raise ValueError("either both or none of 'username' and 'password' must be set")

        _logger.info("Downloading %s" % self.url)
        with _requests.get(self.url, stream=True, auth=auth) as response:
            response.raise_for_status()

            with self.target.open(mode="wb") as f:
                for chunk in _tqdm(response.iter_content(chunk_size=8_192), leave=False):
                    f.write(chunk)


def download_all():
    base = _Path(_config["db.root_directory"])
    download_dir = base / _config["sources.directory"]
    download_dir.mkdir(exist_ok=True, parents=True)

    sources = _config["sources"]
    # Remove the source keys
    exclude_keys = {"directory", "username", "password"}

    for source in filter(lambda i: i not in exclude_keys, sources):
        # TODO: Implement force-overwrites
        (download_dir / source).mkdir(exist_ok=True)

        data = sources[source]
        username = data.get("username")
        password = data.get("password")

        for _, download in filter(lambda i: i[0] not in exclude_keys, data.items()):
            url = download.get("url")
            # Catch case for BioGRID
            if url is None:
                continue

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
