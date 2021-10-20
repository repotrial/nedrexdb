import gzip as _gzip
from csv import DictReader as _DictReader
from pathlib import Path as _Path

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.edges.protein_interacts_with_protein import ProteinInteractsWithProtein as _PPI
from nedrexdb.db.models.nodes.protein import Protein as _Protein
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("iid")


class IIDRow:
    def __init__(self, row):
        self._row = row

    def get_member_one(self) -> str:
        return f"uniprot.{self._row['uniprot1']}"

    def get_member_two(self) -> str:
        return f"uniprot.{self._row['uniprot2']}"

    def get_methods(self) -> list[str]:
        if self._row["methods"] == "-":
            return []
        return [i.strip() for i in self._row["methods"].split(";")]

    def get_databases(self) -> list[str]:
        return ["iid"]

    # def get_databases(self) -> list[str]:
    #     if self._row["dbs"] == "-":
    #         return []
    #     return [i.strip() for i in self._row["dbs"].split(";")]

    def get_evidence_types(self) -> list[str]:
        return [i.strip() for i in self._row["evidence_type"].split(";")]

    def parse(self) -> _PPI:
        ppi = _PPI(
            memberOne=self.get_member_one(),
            memberTwo=self.get_member_two(),
            methods=self.get_methods(),
            databases=self.get_databases(),
            evidenceTypes=self.get_evidence_types(),
        )
        return ppi


class IIDParser:
    def __init__(self, f):
        self.f: _Path = f

        if self.f.name.endswith(".gz") or self.f.name.endswith(".gzip"):
            self.gzipped = True
        else:
            self.gzipped = False

    def parse(self):
        if self.gzipped:
            f = _gzip.open(self.f, "rt")
        else:
            f = self.f.open()

        proteins = {i["primaryDomainId"] for i in _Protein.find(MongoInstance.DB)}

        fieldnames = next(f).strip().split("\t")
        reader = _DictReader(f, delimiter="\t", fieldnames=fieldnames)
        updates = (IIDRow(row).parse() for row in reader)
        updates = (ppi for ppi in updates if ppi.memberOne in proteins and ppi.memberTwo in proteins)
        updates = (ppi.generate_update() for ppi in updates)

        for chunk in _tqdm(_chunked(updates, 1_000)):
            MongoInstance.DB[_PPI.collection_name].bulk_write(chunk)

        f.close()


def parse_ppis():
    filename = get_file_location("human")
    IIDParser(filename).parse()
