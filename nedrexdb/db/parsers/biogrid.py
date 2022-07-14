from csv import DictReader as _DictReader
from itertools import chain as _chain, product as _product

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.protein import Protein
from nedrexdb.db.models.edges.protein_interacts_with_protein import ProteinInteractsWithProtein

get_file_location = _get_file_location_factory("biogrid")


class BioGridRow:
    def __init__(self, row):
        self._row = row

    def interactor_a_ids(self, proteins_allowed=None):
        accs = []
        for db in ["SWISS-PROT", "TREMBL"]:
            if self._row[f"{db} Accessions Interactor A"] != "-":
                accs += self._row[f"{db} Accessions Interactor A"].split("|")

        accs = [f"uniprot.{acc}" for acc in accs]
        if proteins_allowed:
            accs = [acc for acc in accs if acc in proteins_allowed]

        return accs

    def interactor_b_ids(self, proteins_allowed=None):
        accs = []
        for db in ["SWISS-PROT", "TREMBL"]:
            if self._row[f"{db} Accessions Interactor B"] != "-":
                accs += self._row[f"{db} Accessions Interactor B"].split("|")

        accs = [f"uniprot.{acc}" for acc in accs]
        if proteins_allowed:
            accs = [acc for acc in accs if acc in proteins_allowed]

        return accs

    @property
    def methods(self):
        return self._row["Experimental System"]

    def parse(self, proteins_allowed=None):
        ppis = []

        for a, b in _product(self.interactor_a_ids(proteins_allowed), self.interactor_b_ids(proteins_allowed)):
            a, b = sorted([a, b])
            ppi = ProteinInteractsWithProtein(
                memberOne=a,
                memberTwo=b,
                dataSources=["biogrid"],
                evidenceTypes=["exp"],
                methods=[self.methods],
            )
            ppis.append(ppi)

        return ppis


class BioGridParser:
    fieldnames = (
        "BioGRID Interaction ID",
        "Entrez Gene Interactor A",
        "Entrez Gene Interactor B",
        "BioGRID ID Interactor A",
        "BioGRID ID Interactor B",
        "Systematic Name Interactor A",
        "Systematic Name Interactor B",
        "Official Symbol Interactor A",
        "Official Symbol Interactor B",
        "Synonyms Interactor A",
        "Synonyms Interactor B",
        "Experimental System",
        "Experimental System Type",
        "Author",
        "Publication Source",
        "Organism ID Interactor A",
        "Organism ID Interactor B",
        "Throughput",
        "Score",
        "Modification",
        "Qualifications",
        "Tags",
        "Source Database",
        "SWISS-PROT Accessions Interactor A",
        "TREMBL Accessions Interactor A",
        "REFSEQ Accessions Interactor A",
        "SWISS-PROT Accessions Interactor B",
        "TREMBL Accessions Interactor B",
        "REFSEQ Accessions Interactor B",
        "Ontology Term IDs",
        "Ontology Term Names",
        "Ontology Term Categories",
        "Ontology Term Qualifier IDs",
        "Ontology Term Qualifier Names",
        "Ontology Term Types",
        "Organism Name Interactor A",
        "Organism Name Interactor B",
    )

    def __init__(self, f):
        self._f = f

    def parse(self):
        proteins = {i["primaryDomainId"] for i in Protein.find(MongoInstance.DB)}

        with open(self._f, "r") as f:
            reader = _DictReader(f, fieldnames=self.fieldnames, delimiter="\t")
            members = (BioGridRow(row).parse(proteins_allowed=proteins) for row in reader)

            for chunk in _tqdm(_chunked(members, 1_000), leave=False, desc="Parsing BioGRID"):
                updates = [ppi.generate_update() for ppi in _chain(*chunk)]
                if updates:
                    MongoInstance.DB[ProteinInteractsWithProtein.collection_name].bulk_write(updates)


def parse_ppis():
    filename = get_file_location("human_data")
    BioGridParser(filename).parse()
