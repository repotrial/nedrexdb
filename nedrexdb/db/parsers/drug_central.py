from collections import defaultdict as _defaultdict
from dataclasses import dataclass as _dataclass
from itertools import product as _product
import secrets as _secrets
import socket as _socket
import string as _string
import time as _time
import subprocess as _subprocess
from typing import Optional as _Optional

import docker as _docker
import pandas as _pd
from more_itertools import chunked as _chunked
from sqlalchemy import create_engine as _create_engine
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.edges.drug_has_contraindication import DrugHasContraindication
from nedrexdb.db.models.edges.drug_has_indication import DrugHasIndication
from nedrexdb.db.models.nodes.disorder import Disorder
from nedrexdb.db.models.nodes.drug import Drug
from nedrexdb.logger import logger as _logger

get_file_location = _get_file_location_factory("drug_central")

_client = _docker.from_env()


def _generate_snomed_to_nedrex_map():
    d = _defaultdict(list)
    for dis in Disorder.find(MongoInstance.DB):
        snomed_ids = [acc for acc in dis["domainIds"] if acc.startswith("snomedct.")]
        for snomed_id in snomed_ids:
            d[snomed_id].append(dis["primaryDomainId"])

    return d


@_dataclass
class PostgresContainer:
    def __init__(self):
        self._password: _Optional[str] = None
        self._container_name: _Optional[str] = None
        self._port: _Optional[str] = None
        self._container: _Optional[_docker.models.container.Container] = None
        self._engine = None

    @property
    def engine(self):
        if not self._engine:
            self._engine = _create_engine(self._address)
        return self._engine

    @property
    def _address(self):
        return f"postgresql://postgres:{self._password}@localhost:{self._port}"

    @property
    def is_ready(self):
        result = self._container.exec_run("pg_isready")
        return result

    @staticmethod
    def generate_random_string(length: int):
        alphabet = _string.digits + _string.ascii_letters
        string = "".join(_secrets.choice(alphabet) for _ in range(length))
        return string

    @staticmethod
    def get_free_port():
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        s.bind(("", 0))
        _, port = s.getsockname()
        s.close()

        return port

    def start(self):
        if self._container:
            raise Exception("must stop existing postgres container first")

        self._container_name = self.generate_random_string(16)
        self._password = self.generate_random_string(64)
        self._port = self.get_free_port()

        self._container = _client.containers.run(
            image="postgres",
            environment={"POSTGRES_PASSWORD": self._password},
            ports={5432: self._port},
            name=self._container_name,
            remove=True,
            detach=True,
        )

        while self.is_ready.output != b"/var/run/postgresql:5432 - accepting connections\n":
            continue
        _time.sleep(1)

    def list_tables(self):
        engine = _create_engine(self._address)
        q = engine.execute("SELECT pg_tables.tablename FROM pg_catalog.pg_tables;")
        return q.fetchall()

    def restore_from_sql_dump(self, infile):
        command = f"cat {infile} | "
        if f"{infile}".endswith("gz"):
            command += "gzip -d | "
        command += f"docker exec -i {self._container_name} psql -U postgres"

        _logger.debug("Restoring Drug Central from postgres dump (this may take a while)...")
        p = _subprocess.Popen(command, shell=True, stdout=_subprocess.PIPE, stderr=_subprocess.PIPE)
        p.communicate(input=True)

    def stop(self):
        self._container.stop()
        self._container = None
        self._port = None
        self._container_name = None
        self._engine = None

    def _get_drug_central_to_drugbank_map(self):
        df = _pd.read_sql_query('select * from "identifier"', con=self.engine)

        d = _defaultdict(list)
        for _, row in df.iterrows():
            if row["id_type"] != "DRUGBANK_ID":
                continue
            drug_central_id = row["struct_id"]
            drugbank_id = row["identifier"]
            d[drug_central_id].append(drugbank_id)

        return d

    def iter_indications(self):
        dc_to_db_map = self._get_drug_central_to_drugbank_map()
        snomed_to_nedrex_map = _generate_snomed_to_nedrex_map()
        nedrex_drugs = {drug["primaryDomainId"] for drug in Drug.find(MongoInstance.DB)}

        df = _pd.read_sql_query('select * from "omop_relationship"', con=self.engine)
        df = df[~_pd.isnull(df.snomed_conceptid)]
        df = df[~_pd.isnull(df.struct_id)]

        for _, row in df.iterrows():
            if row["relationship_name"] != "indication":
                continue

            db_ids = dc_to_db_map.get(row["struct_id"], [])
            drugs = [f"drugbank.{db_id}" for db_id in db_ids]
            drugs = [i for i in drugs if i in nedrex_drugs]

            sct_id = f"snomedct.{int(row['snomed_conceptid'])}"
            indications = [mondo_id for mondo_id in snomed_to_nedrex_map.get(sct_id, [])]

            for drug, indication in _product(drugs, indications):
                dhi = DrugHasIndication(
                    sourceDomainId=drug,
                    targetDomainId=indication,
                )
                yield dhi

    def iter_contraindications(self):
        dc_to_db_map = self._get_drug_central_to_drugbank_map()
        snomed_to_nedrex_map = _generate_snomed_to_nedrex_map()
        nedrex_drugs = {drug["primaryDomainId"] for drug in Drug.find(MongoInstance.DB)}

        df = _pd.read_sql_query('select * from "omop_relationship"', con=self.engine)
        df = df[~_pd.isnull(df.snomed_conceptid)]
        df = df[~_pd.isnull(df.struct_id)]

        for _, row in df.iterrows():
            if row["relationship_name"] != "contraindication":
                continue

            db_ids = dc_to_db_map.get(row["struct_id"], [])
            drugs = [f"drugbank.{db_id}" for db_id in db_ids]
            drugs = [i for i in drugs if i in nedrex_drugs]

            sct_id = f"snomedct.{int(row['snomed_conceptid'])}"
            contraindications = [mondo_id for mondo_id in snomed_to_nedrex_map.get(sct_id, [])]

            for drug, contraindication in _product(drugs, contraindications):
                dhc = DrugHasContraindication(
                    sourceDomainId=drug,
                    targetDomainId=contraindication,
                )
                yield dhc


def parse_drug_central():
    fname = get_file_location("postgres_dump").absolute()

    p = PostgresContainer()
    p.start()
    p.restore_from_sql_dump(fname)

    # NOTE: NeDRexDB does not include cross-references to the DrugCentral IDs.
    #       This should be added for quality of life and tracking.

    # NOTE: Should add an 'assertedBy' property to indications.
    updates = (dhi.generate_update() for dhi in p.iter_indications())
    for chunk in _tqdm(_chunked(updates, 1_000)):
        MongoInstance.DB[DrugHasIndication.collection_name].bulk_write(chunk)

    # NOTE: Should add an 'assertedBy' property to contraindications.
    updates = (dhc.generate_update() for dhc in p.iter_contraindications())
    for chunk in _tqdm(_chunked(updates, 1_000)):
        MongoInstance.DB[DrugHasContraindication.collection_name].bulk_write(chunk)

    p.stop()
