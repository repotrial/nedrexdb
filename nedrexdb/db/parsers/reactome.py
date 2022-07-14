import gzip as _gzip
from csv import DictReader as _DictReader

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.edges.protein_in_pathway import ProteinInPathway
from nedrexdb.db.models.nodes.pathway import Pathway
from nedrexdb.db.models.nodes.protein import Protein
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("reactome")


class ReactomeRow:
    def __init__(self, row):
        self._row = row
        self.__reactome_id = None
        self.__uniprot_id = None

    @property
    def is_human(self):
        return self._row["Species"] == "Homo sapiens"

    @property
    def reactome_id(self):
        if not self.__reactome_id:
            self.__reactome_id = f"reactome.{self._row['Reactome Pathway Stable identifier']}"

        return self.__reactome_id

    @property
    def uniprot_id(self):
        if not self.__uniprot_id:
            self.__uniprot_id = f"uniprot.{self._row['Source database identifier']}"

        return self.__uniprot_id

    @property
    def display_name(self):
        return self._row["Event Name"]

    def parse_pathway(self):
        if not self.is_human:
            return None

        pathway = Pathway(
            primaryDomainId=self.reactome_id,
            domainIds=[self.reactome_id],
            displayName=self.display_name,
            species="Homo sapiens",
            taxid=9606,
            dataSources=["reactome"],
        )

        return pathway

    def parse_protein_pathway_link(self):
        if not self.is_human:
            return None

        link = ProteinInPathway(
            sourceDomainId=self.uniprot_id, targetDomainId=self.reactome_id, dataSources=["reactome"]
        )

        return link


class ReactomeParser:
    columns = (
        "Source database identifier",
        "Reactome Physical Entity Stable identifier",
        "Reactome Physical Entity Name",
        "Reactome Pathway Stable identifier",
        "URL",
        "Event Name",
        "Evidence code",
        "Species",
    )
    delimiter = "\t"

    def __init__(self, f):
        self.f = f
        if self.f.name.endswith(".gz") or self.f.name.endswith(".gzip"):
            self.gzipped = True
        else:
            self.gzipped = False

    def parse_pathways(self):
        if self.gzipped:
            f = _gzip.open(self.f, "rt")
        else:
            f = self.f.open()

        reader = _DictReader(f, fieldnames=self.columns, delimiter=self.delimiter)

        updates = (ReactomeRow(row).parse_pathway() for row in reader)
        updates = (pathway.generate_update() for pathway in updates if pathway is not None)

        # NOTE: The number of iterations and chunks doesn't match what ends up
        #       NeDRexDB. This is because each row is a *relation* and, thus,
        #       a single pathway can appear multiple times (as part of many)
        #       relations).
        for chunk in _tqdm(_chunked(updates, 1_000), leave=False, desc="Parsing pathways"):
            MongoInstance.DB[Pathway.collection_name].bulk_write(chunk)

        f.close()

    def parse_protein_pathway_links(self):
        if self.gzipped:
            f = _gzip.open(self.f, "rt")
        else:
            f = self.f.open()

        reader = _DictReader(f, fieldnames=self.columns, delimiter=self.delimiter)

        protein_ids = {i["primaryDomainId"] for i in Protein.find(MongoInstance.DB)}
        pathway_ids = {i["primaryDomainId"] for i in Pathway.find(MongoInstance.DB)}

        updates = (ReactomeRow(row).parse_protein_pathway_link() for row in reader)
        updates = (update for update in updates if update is not None)
        updates = (update for update in updates if update.sourceDomainId in protein_ids)
        updates = (update for update in updates if update.targetDomainId in pathway_ids)
        updates = (update.generate_update() for update in updates)

        for chunk in _tqdm(
            _chunked(updates, 1_000), leave=False, desc="Parsing protein-pathway relationships from Reactome"
        ):
            MongoInstance.DB[ProteinInPathway.collection_name].bulk_write(chunk)

        f.close()


def parse():
    f = get_file_location("uniprot_annotations")
    r = ReactomeParser(f)
    r.parse_pathways()
    r.parse_protein_pathway_links()
