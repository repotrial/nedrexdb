import re as _re
from collections import defaultdict as _defaultdict
from csv import DictReader as _DictReader
from itertools import chain as _chain
from pathlib import Path as _Path
from typing import Optional as _Optional

from more_itertools import chunked as _chunked

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.edges.gene_associated_with_disorder import GeneAssociatedWithDisorder
from nedrexdb.db.models.nodes.gene import Gene
from nedrexdb.db.models.nodes.disorder import Disorder
from nedrexdb.db.parsers import _get_file_location_factory


get_file_location = _get_file_location_factory("omim")


class OMIMRow:
    MIM_MAP_REGEX = _re.compile(r"([0-9]{6} \([0-9]\))")

    def __init__(self, row):
        self.row = row

    def parse(self, omim_nedrex_map: dict[str, list[str]]) -> _Optional[list[GeneAssociatedWithDisorder]]:
        if not self.row["Entrez Gene ID"]:
            return None
        gene = f"entrez.{self.row['Entrez Gene ID']}"

        gawd_edges = []

        omim_phenotypes = [i.strip() for i in self.row["Phenotypes"].split(";")]
        for phenotype in omim_phenotypes:
            mim_map = self.MIM_MAP_REGEX.findall(phenotype)
            if not mim_map:
                continue
            assert len(mim_map) == 1

            mim_number, evidence = mim_map.pop().split()
            evidence = int(evidence[1:-1])

            flags = []
            if "{" in phenotype:
                flags.append("susceptibility")
            if "?" in phenotype:
                flags.append("provisional")

            for disorder in omim_nedrex_map.get(f"omim.{mim_number}", []):
                gawd = GeneAssociatedWithDisorder(
                    sourceDomainId=gene,
                    targetDomainId=disorder,
                    omimMappingCode=evidence,
                    omimFlags=flags,
                    assertedBy=["omim"],
                )
                gawd_edges.append(gawd)

        return gawd_edges


def _generate_omim_to_nedrex_map() -> dict[str, list[str]]:
    d = _defaultdict(list)

    for disorder in Disorder.find(MongoInstance.DB):
        omim_accs = [acc for acc in disorder["domainIds"] if acc.startswith("omim.")]
        for acc in omim_accs:
            d[acc].append(disorder["primaryDomainId"])

    return d


class GeneMap2Parser:
    columns = (
        "Chromosome",
        "Genomic Position Start",
        "Genomic Position End",
        "Cyto Location",
        "Computed Cyto Location",
        "MIM Number",
        "Gene Symbols",
        "Gene Name",
        "Approved Symbol",
        "Entrez Gene ID",
        "Ensembl Gene ID",
        "Comments",
        "Phenotypes",
        "Mouse Gene Symbol/ID",
    )
    delimiter = "\t"
    comment_char = "#"

    def __init__(self, path):
        self.path = path

        if not self.path.exists():
            raise Exception(f"{self.path} does not exist")

    def parse(self):
        with self.path.open() as f:
            reader = _DictReader(
                filter(lambda row: row[0] != self.comment_char, f), delimiter=self.delimiter, fieldnames=self.columns
            )

            omim_nedrex_map = _generate_omim_to_nedrex_map()
            genes = {gene["primaryDomainId"] for gene in Gene.find(MongoInstance.DB)}

            updates = (OMIMRow(row).parse(omim_nedrex_map) for row in reader)
            updates = (update for update in updates if update is not None)
            for chunk in _chunked(updates, 1_000):
                chunk = [assoc.generate_update() for assoc in _chain(*updates) if assoc.sourceDomainId in genes]
                if not chunk:
                    continue
                MongoInstance.DB[GeneAssociatedWithDisorder.collection_name].bulk_write(chunk)


def parse_gene_disease_associations():
    gm2_file = _Path(get_file_location("genemap2"))
    parser = GeneMap2Parser(gm2_file)
    parser.parse()
