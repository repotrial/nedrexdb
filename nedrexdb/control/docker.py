import time as _time
from abc import ABC as _ABC, abstractmethod as _abstractmethod

import docker as _docker

from nedrexdb import config as _config

_client = _docker.from_env()


def get_mongo_image():
    return _config["db.mongo_image"]


def get_mongo_express_image():
    return _config["db.mongo_express_image"]


def get_neo4j_image():
    return _config["db.neo4j_image"]


def generate_mongo_volume_name():
    timestamp = _time.time_ns() // 1_000_000  # time in ms
    volume_name = f"{_config['db.volume_root']}_mongo_{timestamp}"
    return volume_name


def generate_new_mongo_volume():
    name = generate_mongo_volume_name()
    _client.volumes.create(name=name)
    return name


def get_mongo_volumes():
    volume_root = _config["db.volume_root"]
    volumes = _client.volumes.list()
    volumes = [vol for vol in volumes if vol.name.startswith(f"{volume_root}_mongo")]
    volumes.sort(key=lambda i: i.name, reverse=True)
    return volumes


def generate_neo4j_volume_name():
    timestamp = _time.time_ns() // 1_000_000  # time in ms
    volume_name = f"{_config['db.volume_root']}_neo4j_{timestamp}"
    return volume_name


def generate_new_neo4j_volume():
    name = generate_neo4j_volume_name()
    _client.volumes.create(name=name)
    return name


def get_neo4j_volumes():
    volume_root = _config["db.volume_root"]
    volumes = _client.volumes.list()
    volumes = [vol for vol in volumes if vol.name.startswith(f"{volume_root}_neo4j")]
    volumes.sort(key=lambda i: i.name, reverse=True)
    return volumes


class _NeDRexInstance(_ABC):
    @_abstractmethod
    def set_up(self):
        pass

    @_abstractmethod
    def remove(self):
        pass


class _NeDRexBaseInstance(_NeDRexInstance):
    @property
    def mongo_container_name(self):
        return _config[f"db.{self.version}.container_name"]

    @property
    def neo4j_container_name(self):
        return f'{_config[f"db.{self.version}.container_name"]}_neo4j'

    @property
    def neo4j_http_port(self):
        return _config[f"db.{self.version}.neo4j_http_port"]

    @property
    def neo4j_bolt_port(self):
        return _config[f"db.{self.version}.neo4j_bolt_port"]

    @property
    def mongo_port(self):
        return _config[f"db.{self.version}.mongo_port"]

    @property
    def network_name(self):
        return _config[f"db.{self.version}.network"]

    @property
    def express_port(self):
        return _config[f"db.{self.version}.mongo_express_port"]

    @property
    def express_container_name(self):
        return _config[f"db.{self.version}.express_container_name"]

    @property
    def mongo_container(self):
        try:
            return _client.containers.get(self.mongo_container_name)
        except _docker.errors.NotFound:
            return None

    @property
    def express_container(self):
        try:
            return _client.containers.get(self.express_container_name)
        except _docker.errors.NotFound:
            return None

    @property
    def neo4j_container(self):
        try:
            return _client.containers.get(self.neo4j_container_name)
        except _docker.errors.NotFound:
            return None

    def _set_up_network(self):
        try:
            _client.networks.get(self.network_name)
        except _docker.errors.NotFound:
            _client.networks.create(self.network_name)

    def _set_up_neo4j(self, neo4j_mode, use_existing_volume):
        if self.neo4j_container:
            return

        if use_existing_volume:
            volumes = get_neo4j_volumes()
            if not volumes:
                raise ValueError("use_existing_volume set to True but no volume already exists")
            volume = volumes[0].name
        else:
            volume = generate_neo4j_volume_name()

        kwargs = {
            "image": get_neo4j_image(),
            "detach": True,
            "name": self.neo4j_container_name,
            "volumes": {volume: {"mode": "rw", "bind": "/data"}, "/tmp": {"mode": "ro", "bind": "/import"}},
            "ports": {7474: ("127.0.0.1", self.neo4j_http_port), 7687: ("127.0.0.1", self.neo4j_bolt_port)},
            "environment": {"NEO4J_AUTH": "none"},
            "network": self.network_name,
            "remove": True,
        }

        if neo4j_mode == "import":
            kwargs["stdin_open"] = True
            kwargs["tty"] = True
            kwargs["entrypoint"] = "/bin/bash"

        elif neo4j_mode == "db":
            kwargs["environment"]["NEO4J_dbms_read__only"] = "true"
        else:
            raise Exception(f"neo4j_mode {neo4j_mode!r} is invalid")

        _client.containers.run(**kwargs)

    def _set_up_mongo(self, use_existing_volume):
        if self.mongo_container:  # if the container already exists, nothing to do
            return

        if use_existing_volume:
            volumes = get_mongo_volumes()
            if not volumes:
                raise ValueError("use_existing_volume set to True but no volume already exists")
            volume = volumes[0].name
        else:
            volume = generate_new_mongo_volume()

        _client.containers.run(
            image=get_mongo_image(),
            detach=True,
            name=self.mongo_container_name,
            volumes={volume: {"mode": "rw", "bind": "/data/db"}},
            ports={27017: ("127.0.0.1", self.mongo_port)},
            network=self.network_name,
            remove=True,
        )

    def _set_up_express(self):
        if self.express_container:  # if the container already exists, nothing to do
            return

        _client.containers.run(
            image=get_mongo_express_image(),
            detach=True,
            name=self.express_container_name,
            ports={8081: ("127.0.0.1", self.express_port)},
            network=self.network_name,
            environment={"ME_CONFIG_MONGODB_SERVER": self.mongo_container_name},
            remove=True,
        )

    def _remove_neo4j(self, remove_db_volume=False):
        if not self.neo4j_container:
            return

        mounts = self.neo4j_container.attrs["Mounts"]
        volumes_to_remove = ["/logs"]
        if remove_db_volume:
            volumes_to_remove.append("/data")

        volumes_to_remove = [
            mount["Name"] for mount in mounts if mount["Type"] == "volume" and mount["Destination"] in volumes_to_remove
        ]

        self.neo4j_container.remove(force=True)

        for vol_name in volumes_to_remove:
            _client.volumes.get(vol_name).remove(force=True)

    def _remove_mongo(self, remove_db_volume=False, remove_configdb_volume=True):
        if not self.mongo_container:
            return

        mounts = self.mongo_container.attrs["Mounts"]

        volumes_to_remove = []
        if remove_configdb_volume:
            volumes_to_remove.append("/data/configdb")
        if remove_db_volume:
            volumes_to_remove.append("/data/db")

        volumes_to_remove = [
            mount["Name"] for mount in mounts if mount["Type"] == "volume" and mount["Destination"] in volumes_to_remove
        ]

        self.mongo_container.remove(force=True)

        for vol_name in volumes_to_remove:
            _client.volumes.get(vol_name).remove(force=True)

    def _remove_express(self):
        if not self.express_container:
            return

        self.express_container.remove(force=True)

    def _remove_network(self):
        try:
            _client.networks.get(self.network_name).remove()
        except _docker.errors.NotFound:
            pass

    def set_up(self, use_existing_volume=True, neo4j_mode="db"):
        self._set_up_network()
        self._set_up_mongo(use_existing_volume=use_existing_volume)
        self._set_up_neo4j(use_existing_volume=use_existing_volume, neo4j_mode=neo4j_mode)
        self._set_up_express()

    def remove(self, remove_db_volume=False, remove_configdb_volume=True):
        self._remove_mongo(
            remove_db_volume=remove_db_volume,
            remove_configdb_volume=remove_configdb_volume,
        )
        self._remove_neo4j(remove_db_volume=remove_db_volume)
        self._remove_express()
        self._remove_network()


class NeDRexLiveInstance(_NeDRexBaseInstance):
    @property
    def version(self):
        return "live"


class NeDRexDevInstance(_NeDRexBaseInstance):
    @property
    def version(self):
        return "dev"
