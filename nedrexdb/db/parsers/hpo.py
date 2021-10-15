import warnings as _warnings
from csv import DictReader as _DictReader
from functools import lru_cache as _lru_cache

import obonet
from more_itertools import chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.disorder import Disorder
from nedrexdb.db.models.nodes.phenotype import Phenotype
from nedrexdb.db.models.edges.disorder_has_phenotype import DisorderHasPhenotype

get_file_location = _get_file_location_factory("hpo")


@_lru_cache(maxsize=None)
def get_disorder_by_domain_id(domain_id):
    return [disorder["primaryDomainId"] for disorder in Disorder.find(MongoInstance.DB, {"domainIds": domain_id})]


class HPONode:
    def __init__(self, node_id, data):
        self._node_id = node_id.replace("HP:", "hpo.")
        self._data = data

    @property
    def primary_domain_id(self) -> str:
        return self._node_id

    @property
    def domain_ids(self) -> list[str]:
        domain_ids = [hpo_id.replace("HP:", "hpo.") for hpo_id in self._data.get("alt_id", [])]
        domain_ids.append(self.primary_domain_id)
        return domain_ids

    @property
    def display_name(self) -> str:
        return self._data.get("name")

    @property
    def description(self) -> str:
        definition = self._data.get("def")
        if definition:
            return definition.split('"')[1]
        return ""

    @property
    def synonyms(self) -> list[str]:
        hpo_syns = self._data.get("synonym", [])
        synonyms = [syn.split('"')[1] for syn in hpo_syns if "EXACT" in syn]
        return synonyms

    def parse(self):
        return Phenotype(
            primaryDomainId=self.primary_domain_id,
            domainIds=self.domain_ids,
            displayName=self.display_name,
            synonyms=self.synonyms,
            description=self.description,
        )


class HPOAParser:
    fieldnames = (
        "DatabaseID",
        "DiseaseName",
        "Qualifier",
        "HPO_ID",
        "Reference",
        "Evidence",
        "Onset",
        "Frequency",
        "Sex",
        "Modifier",
        "Aspect",
        "Biocuration",
    )

    def __init__(self, f):
        self._f = f

    def rows(self):
        with self._f.open() as f:
            f = (line for line in f if not line.startswith("#"))
            for row in _DictReader(f, fieldnames=self.fieldnames, delimiter="\t"):
                yield HPOARow(row)


class HPOARow:
    def __init__(self, row):
        self._row = row

    @property
    def source_domain_ids(self):
        disorder = self._row["DatabaseID"]
        if disorder.startswith("OMIM"):
            d = disorder.replace("OMIM:", "omim.")
        elif disorder.startswith("ORPHA"):
            d = disorder.replace("ORPHA", "orpha.")
        elif disorder.startswith("DECIPHER"):
            return []
        else:
            _warnings.warn("disorder encountered without prefix handler in HPOA parser")
            return []

        return get_disorder_by_domain_id(d)

    @property
    def target_domain_id(self):
        return self._row["HPO_ID"].replace("HP:", "hpo.")

    def parse(self):
        return [
            DisorderHasPhenotype(sourceDomainId=source, targetDomainId=self.target_domain_id, assertedBy=["hpo"])
            for source in self.source_domain_ids
        ]


def parse_phenotypes():
    g = obonet.read_obo(get_file_location("obo"))
    for node, data in g.nodes(data=True):
        yield HPONode(node, data).parse()


def parse_hpoa():
    f = get_file_location("annotations")
    for row in HPOAParser(f).rows():
        yield from row.parse()


def parse():
    for chunk in _tqdm(chunked(parse_phenotypes(), 1_000), leave=False):
        updates = [node.generate_update() for node in chunk]
        MongoInstance.DB[Phenotype.collection_name].bulk_write(updates)

    for chunk in _tqdm(chunked(parse_hpoa(), 1_000), leave=False):
        updates = [rel.generate_update() for rel in chunk]
        MongoInstance.DB[DisorderHasPhenotype.collection_name].bulk_write(updates)
    get_disorder_by_domain_id.cache_clear()
