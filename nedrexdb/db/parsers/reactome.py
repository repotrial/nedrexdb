import gzip as _gzip
from csv import DictReader as _DictReader

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.nodes.pathway import Pathway
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("reactome")


class ReactomeRow:
    def __init__(self, row):
        self._row = row
        self.__primary_id = None

    @property
    def is_human(self):
        return self._row["Species"] == "Homo sapiens"

    @property
    def primary_id(self):
        if not self.__primary_id:
            self.__primary_id = "reactome." f"{self._row['Reactome Pathway Stable identifier']}"

        return self.__primary_id

    @property
    def display_name(self):
        return self._row["Event Name"]

    def parse_pathway(self):
        if not self.is_human:
            return None

        pathway = Pathway(
            primaryDomainId=self.primary_id,
            domainIds=[self.primary_id],
            displayName=self.display_name,
            species="Homo sapiens",
            taxid=9606,
        )

        return pathway


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
        for chunk in _tqdm(_chunked(updates, 1_000), leave=False):
            MongoInstance.DB[Pathway.collection_name].bulk_write(chunk)

        f.close()


def parse():
    f = get_file_location("uniprot_annotations")
    ReactomeParser(f).parse_pathways()
