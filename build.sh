#!/bin/bash

./build.py update --conf .licensed_config.toml --download
./set_metadata.py --config .licensed_config.toml --version live

./build.py update --conf .open_config.toml
./set_metadata.py --config .open_config.toml --version live
