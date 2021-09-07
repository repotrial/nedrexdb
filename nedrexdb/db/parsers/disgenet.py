import gzip as _gzip
from collections import defaultdict as _defaultdict
from csv import DictReader as _DictReader
from itertools import chain as _chain
from pathlib import Path as _Path

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.nodes.disorder import Disorder
from nedrexdb.db.models.nodes.gene import Gene
from nedrexdb.db.models.edges.gene_associated_with_disorder import GeneAssociatedWithDisorder
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("disgenet")


def _umls_to_nedrex_map() -> dict[str, list[str]]:
    d = _defaultdict(list)

    for dis in Disorder.find(MongoInstance.DB):
        umls_ids = [acc for acc in dis["domainIds"] if acc.startswith("umls.")]
        for umls_id in umls_ids:
            d[umls_id].append(dis["primaryDomainId"])

    return d


class DisGeNetRow:
    def __init__(self, row):
        self._row = row

    def get_gene_id(self):
        return f"entrez.{self._row['geneId'].strip()}"

    def get_disorder_id(self):
        return f"umls.{self._row['diseaseId'].strip()}"

    def get_score(self) -> float:
        return float(self._row["score"])

    def parse(self, umls_nedrex_map: dict[str, list[str]]) -> list[GeneAssociatedWithDisorder]:
        sourceDomainId = self.get_gene_id()
        score = self.get_score()
        asserted_by = ["disgenet"]
        disorders = umls_nedrex_map.get(self.get_disorder_id(), [])

        gawds = [
            GeneAssociatedWithDisorder(
                sourceDomainId=sourceDomainId, targetDomainId=disorder, score=score, assertedBy=asserted_by
            )
            for disorder in disorders
        ]

        return gawds


class DisGeNetParser:
    def __init__(self, f: _Path):
        self.f = f

        if self.f.name.endswith(".gz") or self.f.name.endswith(".gzip"):
            self.gzipped = True
        else:
            self.gzipped = False

    def parse(self):
        if self.gzipped:
            f = _gzip.open(self.f, "rt")
        else:
            f = self.f.open()

        reader = _DictReader(f, delimiter="\t")

        umls_nedrex_map = _umls_to_nedrex_map()
        genes = {gene["primaryDomainId"] for gene in Gene.find(MongoInstance.DB)}

        updates = (DisGeNetRow(row).parse(umls_nedrex_map) for row in reader)
        for chunk in _tqdm(_chunked(updates, 1_000)):
            chunk = list(_chain(*chunk))
            if not chunk:
                continue
            chunk = [gawd.generate_update() for gawd in chunk if gawd.sourceDomainId in genes]
            MongoInstance.DB[GeneAssociatedWithDisorder.collection_name].bulk_write(chunk)

        f.close()


def parse_gene_disease_associations():
    fname = get_file_location("gene_disease_associations")
    DisGeNetParser(fname).parse()
