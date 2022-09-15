#!/usr/bin/env python

import argparse
from pathlib import Path
from typing import Optional, Protocol, Literal


parser = argparse.ArgumentParser(description="CLI for starting and stopping NeDRex instances")
parser.add_argument("--libdir", required=False, default=None)
parser.add_argument("--config", required=True)
parser.add_argument("--version", required=True, choices={"live", "dev"})
parser.add_argument("action", choices={"start", "stop", "restart"})


class ParsedArgs(Protocol):
    libdir: Optional[str]
    config: str
    version: Literal["live", "dev"]
    action: Literal["start", "stop", "restart"]


class Instance(Protocol):
    def set_up(self, use_existing_volume: bool, neo4j_mode=Literal["db", "import"]):
        ...

    def remove(self):
        ...


def update_path_if_necessary(args: ParsedArgs):
    if args.libdir is None:
        return

    import sys

    full_path = str(Path(args.libdir).absolute())
    sys.path.insert(0, full_path)


def check_config_exists(args: ParsedArgs):
    if not Path(args.config).exists():
        raise FileNotFoundError(f"{args.config} does not exist")


if __name__ == "__main__":
    args = parser.parse_args()
    check_config_exists(args)
    update_path_if_necessary(args)

    try:
        import nedrexdb  # type: ignore
        import nedrexdb.control.docker as _docker  # type: ignore
    except ModuleNotFoundError as excinfo:
        if excinfo.msg == "No module named 'docker'":
            print("The python 'docker' library is required -- please run 'python -m pip install docker' and retry")
            exit(1)
        elif excinfo.msg.startswith("No module named 'nedrexdb."):
            print("The nedrexdb library could not be found. Either:")
            print("    - Install the nedrexdb library")
            print("    - Pass the --libdir argument")

    nedrexdb.parse_config(args.config)

    if args.version == "dev":
        instance: Instance = _docker.NeDRexDevInstance()
    else:
        instance = _docker.NeDRexLiveInstance()

    if args.action == "start":
        instance.set_up(use_existing_volume=True, neo4j_mode="db")
    elif args.action == "stop":
        instance.remove()
    elif args.action == "restart":
        instance.remove()
        instance.set_up(use_existing_volume=True, neo4j_mode="db")
