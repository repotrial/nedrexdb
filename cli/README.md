# NeDRex CLI

This is a simple CLI for quickly starting, stopping, and restarting a NeDRex DB instance.

The CLI can be invoked simply by invoking the script with `./nedrex_cli.py` or `python nedrex_cli.py`. The arguments / options for the script are detailed below.

- `--libdir`: If the `nedrexdb` library is not installed in your current environment, this argument allows you to pass the path of a directory containing it. In this instance, the required directory would be the parent directory.
- `--config`: A config file for a NeDRex instance, which contains details of a NeDRex instance (e.g., ports).
- `--version`: Whether the CLI should work on a dev version or a live version of a NeDRex instance
- As an argument, the action: one of `start`, `stop`, or `restart`.


This script is useful in cases where an instance of NeDRex has unexpectedly stopped. This can be started again, for example, by using the command:

    ./nedrex_cli.py --libdir ~/path/to/nedrexdb --config open.toml --version live start
