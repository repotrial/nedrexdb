import gzip as _gzip
from csv import DictReader as _DictReader
from typing import Optional as _Optional

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.gene import Gene

get_file_location = _get_file_location_factory("ncbi")


class GeneInfoRow:
    def __init__(self, row):
        self._row = row

    def parse(self) -> Gene:
        g = Gene()
        g.primaryDomainId = self.get_primary_id()
        g.domainIds = [g.primaryDomainId] + self.get_ensembl_xrefs()

        g.approvedSymbol = self.get_approved_symbol()
        if g.approvedSymbol:
            g.displayName = g.approvedSymbol
        else:
            g.displayName = g.primaryDomainId

        g.symbols = self.get_symbols()
        g.description = self.get_description()
        g.chromosome = self.get_chromosome()
        g.mapLocation = self.get_location()
        g.geneType = self.get_gene_type()
        g.synonyms = self.get_synonyms()

        return g

    def get_primary_id(self) -> str:
        return f"entrez.{self._row['GeneID']}"

    def get_ensembl_xrefs(self) -> list[str]:
        ensembl_xrefs = [
            f'ensembl.{i.replace("Ensembl:", "")}' for i in self._row["dbXrefs"].split("|") if i.startswith("Ensembl:")
        ]
        return ensembl_xrefs

    def get_approved_symbol(self) -> _Optional[str]:
        approved_symbol = self._row["Symbol_from_nomenclature_authority"].strip()
        if approved_symbol == "-":
            return None
        else:
            return approved_symbol

    def get_symbols(self) -> list[str]:
        symbols = self._row["Synonyms"].split("|")
        return [symbol for symbol in symbols if symbol != "-"]

    def get_description(self) -> str:
        return self._row["description"]

    def get_synonyms(self) -> list[str]:
        synonyms = self._row["Other_designations"].split("|")
        full_name = self._row["Full_name_from_nomenclature_authority"].strip()
        if full_name != "-" and full_name not in synonyms:
            synonyms.append(full_name)

        return [synonym for synonym in synonyms if synonym != "-"]

    def get_chromosome(self) -> str:
        return self._row["chromosome"]

    def get_location(self) -> _Optional[str]:
        location = self._row["map_location"]
        if location == "-":
            return None
        return location

    def get_gene_type(self) -> str:
        return self._row["type_of_gene"]


def parse_gene_info():
    columns = (
        "tax_id",
        "GeneID",
        "Symbol",
        "LocusTag",
        "Synonyms",
        "dbXrefs",
        "chromosome",
        "map_location",
        "description",
        "type_of_gene",
        "Symbol_from_nomenclature_authority",
        "Full_name_from_nomenclature_authority",
        "Nomenclature_status",
        "Other_designations",
        "Modification_date",
        "Feature_type",
    )

    filename = get_file_location("gene_info")
    comment_char = "#"
    delimiter = "\t"

    with _gzip.open(filename, "rt") as f:
        filtered_f = filter(lambda row: row[0] != comment_char, f)
        reader = _DictReader(filtered_f, delimiter=delimiter, fieldnames=columns)
        updates = (GeneInfoRow(row).parse().generate_update() for row in reader)

        for chunk in _tqdm(
            _chunked(updates, 1_000),
            desc="Parsing NCBI gene info",
            leave=False,
        ):
            MongoInstance.DB[Gene.collection_name].bulk_write(chunk)
