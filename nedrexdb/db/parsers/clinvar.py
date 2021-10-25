import gzip as _gzip
import re as _re
from csv import DictReader as _DictReader

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.nodes.genomic_variant import GenomicVariant
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("clinvar")


def disorder_mapper(string: str):
    if string.startswith("MONDO:MONDO:"):
        return string.replace("MONDO:MONDO:", "mondo.")
    elif string.startswith("OMIM:"):
        return string.replace("OMIM:", "omim.")
    elif string.startswith("MedGen:"):
        return string.replace("MedGen:", "medgen.")
    elif string.startswith("Orphanet:ORPHA"):
        return string.replace("Orphanet:ORPHA", "orpha.")
    elif string.startswith("MeSH:"):
        return string.replace("MeSH:", "mesh.")
    elif string.startswith("SNOMED_CT:"):
        return string.replace("SNOMED_CT:", "snomed.")
    else:
        # NOTE: May be worth checking periodically to see if there are any
        # other identifiers we can handle.
        pass


class ClinVarParser:
    fieldnames = (
        "CHROM",
        "POS",
        "ID",
        "REF",
        "ALT",
        "QUAL",
        "FILTER",
        "INFO",
    )

    def __init__(self, fname):
        self.fname = fname

    def iter_rows(self):
        with _gzip.open(self.fname, "rt") as f:
            f = (line for line in f if not line.startswith("#"))
            reader = _DictReader(f, fieldnames=self.fieldnames, delimiter="\t")
            for row in reader:
                row["INFO"] = {k: v for k, v in [i.split("=", 1) for i in row["INFO"].split(";")]}

                if row["INFO"].get("CLNDISDB"):
                    row["INFO"]["CLNDISDB"] = _re.split(r",|\|", row["INFO"]["CLNDISDB"])
                yield row


class ClinVarRow:
    def __init__(self, row):
        self._row = row

    @property
    def identifier(self):
        return f"clinvar.{self._row['ID']}"

    def get_rs(self):
        if self._row["INFO"].get("RS"):
            return [f"dbsnp.{self._row['INFO']['RS']}"]
        else:
            return []

    @property
    def chromosome(self):
        return self._row["CHROM"]

    @property
    def position(self):
        return int(self._row["POS"])

    @property
    def reference(self):
        return self._row["REF"]

    @property
    def alternative(self):
        return self._row["ALT"]

    @property
    def clinical_significance(self):
        if self._row["INFO"].get("CLNSIG"):
            return self._row["INFO"].get("CLNSIG").split(",_")
        return []

    @property
    def associated_genes(self):
        return [f"entrez.{entrez_id}" for _, entrez_id in [i.split(":") for i in self._row["INFO"].get("GENEINFO", [])]]

    @property
    def associated_disorders(self):
        disorders = []
        for disorder in self._row["INFO"].get("CLNDISDB", []):
            formatted_disorder = disorder_mapper(disorder)
            if formatted_disorder:
                disorders.append(formatted_disorder)
        return disorders

    def parse(self):
        return GenomicVariant(
            primaryDomainId=self.identifier,
            domainIds=[self.identifier] + self.get_rs(),
            chromosome=self.chromosome,
            position=self.position,
            clinicalSignificance=self.clinical_significance,
            referenceSequence=self.reference,
            alternativeSequence=self.alternative,
        )


def parse():
    fname = get_file_location("human_data")
    parser = ClinVarParser(fname)

    updates = (ClinVarRow(i).parse().generate_update() for i in parser.iter_rows())
    for chunk in _tqdm(_chunked(updates, 1_000)):
        MongoInstance.DB[GenomicVariant.collection_name].bulk_write(chunk)
