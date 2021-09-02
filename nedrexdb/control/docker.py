import time as _time
from abc import ABC as _ABC, abstractmethod as _abstractmethod

import docker as _docker

from nedrexdb import config as _config

_client = _docker.from_env()


def get_mongo_image():
    return _config["db.mongo_image"]


def get_mongo_express_image():
    return _config["db.mongo_express_image"]


def generate_volume_name():
    timestamp = _time.time_ns() // 1_000_000  # time in ms
    volume_name = f"{_config['db.volume_root']}_{timestamp}"
    return volume_name


def generate_new_volume():
    name = generate_volume_name()
    _client.volumes.create(name=name)
    return name


def get_nedrex_volumes():
    volume_root = _config["db.volume_root"]
    volumes = _client.volumes.list()
    volumes = [vol for vol in volumes if vol.name.startswith(volume_root)]
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
    def container_name(self):
        return _config[f"db.{self.version}.container_name"]

    @property
    def port(self):
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
    def container(self):
        try:
            return _client.containers.get(self.container_name)
        except _docker.errors.NotFound:
            return None

    @property
    def express_container(self):
        try:
            return _client.containers.get(self.express_container_name)
        except _docker.errors.NotFound:
            return None

    def _set_up_network(self):
        try:
            _client.networks.get(self.network_name)
        except _docker.errors.NotFound:
            _client.networks.create(self.network_name)

    def _set_up_mongo(self, use_existing_volume):
        if self.container:  # if the container already exists, nothing to do
            return

        if use_existing_volume:
            volumes = get_nedrex_volumes()
            if not volumes:
                raise ValueError("use_existing_volume set to True but no volume already exists")
            volume = volumes[0].name
        else:
            volume = generate_new_volume()

        _client.containers.run(
            image=get_mongo_image(),
            detach=True,
            name=self.container_name,
            volumes={volume: {"mode": "rw", "bind": "/data/db"}},
            ports={27017: ("127.0.0.1", self.port)},
            network=self.network_name,
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
            environment={"ME_CONFIG_MONGODB_SERVER": self.container_name},
        )

    def _remove_mongo(self, remove_db_volume=False, remove_configdb_volume=True):
        if not self.container:
            return

        mounts = self.container.attrs["Mounts"]

        volumes_to_remove = []
        if remove_configdb_volume:
            volumes_to_remove.append("/data/configdb")
        if remove_db_volume:
            volumes_to_remove.append("/data/db")

        volumes_to_remove = [
            mount["Name"] for mount in mounts if mount["Type"] == "volume" and mount["Destination"] in volumes_to_remove
        ]

        self.container.remove(force=True)

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

    def set_up(self, use_existing_volume=True):
        self._set_up_network()
        self._set_up_mongo(use_existing_volume=use_existing_volume)
        self._set_up_express()

    def remove(self, remove_db_volume=False, remove_configdb_volume=True):
        self._remove_mongo(remove_db_volume=remove_db_volume, remove_configdb_volume=remove_configdb_volume)
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
