import os
import time
from contextlib import contextmanager
from pathlib import Path as _Path
from typing import Optional as _Optional

import requests as _requests  # type: ignore
from pydantic import BaseModel as _BaseModel, validator as _validator
from tqdm import tqdm as _tqdm

from nedrexdb.logger import logger as _logger


@contextmanager
def change_directory(directory: str):
    """Context manager to temporarily change to a specified directory"""
    current_directory = os.path.abspath(".")
    os.chdir(directory)
    yield
    os.chdir(current_directory)


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
        for _ in range(3):
            try:
                self._download()
            except _requests.ConnectionError:
                _logger.warning(f"failed to download {self.url!r}")
                time.sleep(10)
            else:
                return
        _logger.critical(f"failed to download {self.url!r} three times, aborting!")

    def _download(self):
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
