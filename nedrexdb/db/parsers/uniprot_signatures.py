"""
NOTE: This is included as separate file as the uniprot.py file is already rather bloated. Although this file uses the
same data files, the parsing method is different and requires some bespoke parsers.
"""

import gzip as _gzip
from dataclasses import dataclass
from datetime import datetime
from io import StringIO as _StringIO
from itertools import chain as _chain
from typing import Final as _Final

from more_itertools import chunked as _chunked
from pymongo import UpdateOne
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.protein import Protein


_INTERPRO_DATABASES: _Final = {
    "InterPro",
    "Gene3D",
    "CDD",
    "HAMAP",
    "PANTHER",
    "Pfam",
    "PIRSF",
    "PRINTS",
    "PROSITE",
    "SFLD",
    "SMART",
    "SUPFAM",
    "TIGRFAMs",
}

get_file_location = _get_file_location_factory("uniprot")


def iter_records(fname):
    # NOTE: The file is expected to be gzipped.
    with _gzip.open(fname, "rt") as f:
        text = ""

        for line in f:
            text += line
            if line.strip() == "//":
                yield _StringIO(text)
                text = ""


@dataclass
class Signature:
    domain_id: str
    database: str
    display_name: str

    def to_update(self):
        timestamp = datetime.utcnow()
        return UpdateOne(
            {"primaryDomainId": self.domain_id},
            {
                "$set": {
                    "database": self.database,
                    "displayName": self.display_name,
                    "updated": timestamp,
                },
                "$setOnInsert": {"created": timestamp, "type": "Signature"},
            },
            upsert=True,
        )


@dataclass
class SwissRecordParser:
    data: _StringIO

    @property
    def id(self):
        self.data.seek(0)
        for line in self.data:
            if not line.startswith("AC"):
                continue
            return f"uniprot.{line.strip().split()[1][:-1]}"

    @property
    def signatures(self):
        signatures = []

        self.data.seek(0)
        for line in self.data:
            if not line.startswith("DR"):
                continue

            db, acc, desc = [i[:-1] for i in line.strip().split()[1:4]]

            if db not in _INTERPRO_DATABASES:
                continue

            if not desc or desc == "-":
                desc = None

            sig = Signature(f"{db.lower()}.{acc}", db, desc)
            signatures.append(sig)

        return signatures


def generate_protein_signature_update(protein_id, signature_id):
    tnow = datetime.utcnow()

    return UpdateOne(
        {"sourceDomainId": protein_id, "targetDomainId": signature_id},
        {"$set": {"updated": tnow}, "$setOnInsert": {"created": tnow, "type": "ProteinHasSignature"}},
        upsert=True,
    )


def parse():
    signature_coll = MongoInstance.DB["signature"]
    signature_coll.create_index("primaryDomainId")

    protein_has_sig_coll = MongoInstance.DB["protein_has_signature"]
    protein_has_sig_coll.create_index("sourceDomainId")
    protein_has_sig_coll.create_index("targetDomainId")
    protein_has_sig_coll.create_index([("sourceDomainId", 1), ("targetDomainId", 1)])

    records_iter = _chain(iter_records(get_file_location("swissprot")), iter_records(get_file_location("trembl")))

    protein_ids = {doc["primaryDomainId"] for doc in Protein.find(MongoInstance.DB)}

    for chunk in _tqdm(_chunked(records_iter, 1_000), desc="Parsing signatures from UniProt", leave=False):
        signatures = []
        relationships = []

        records = [SwissRecordParser(data) for data in chunk]

        for record in records:
            if record.id not in protein_ids:
                raise Exception("Expected ID to be parsed already")

            signatures += [sig.to_update() for sig in record.signatures]
            relationships += [generate_protein_signature_update(record.id, sig.domain_id) for sig in record.signatures]

        signature_coll.bulk_write(signatures)
        protein_has_sig_coll.bulk_write(relationships)
