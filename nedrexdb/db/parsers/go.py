from collections import defaultdict as _defaultdict
from itertools import chain as _chain

from more_itertools import chunked as _chunked
from rdflib import Graph as _Graph
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.go import GO
from nedrexdb.db.models.edges.go_is_subtype_of_go import GOIsSubtypeOfGO
from nedrexdb.logger import logger

get_file_location = _get_file_location_factory("go")


class GORelations:
    def __init__(self, po):
        # po refers to 'predicate object'
        self._po = po

    @property
    def primary_id(self):
        for p, o in self._po:
            if str(p) == "http://www.geneontology.org/formats/oboInOwl#id":
                return str(o).replace("GO:", "go.")

    @property
    def display_name(self):
        for p, o in self._po:
            if str(p) == "http://www.w3.org/2000/01/rdf-schema#label":
                return str(o)

    @property
    def synonyms(self):
        return [str(o) for p, o in self._po if str(p) == "http://www.geneontology.org/formats/oboInOwl#hasExactSynonym"]

    @property
    def description(self):
        for p, o in self._po:
            if str(p) == "http://purl.obolibrary.org/obo/IAO_0000115":
                return str(o)

    @property
    def is_a(self):
        subclass_url = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        go_prefix = "http://purl.obolibrary.org/obo/GO_"
        return [
            str(o).replace("http://purl.obolibrary.org/obo/GO_", "go.")
            for p, o in self._po
            if str(p) == subclass_url and str(o).startswith(go_prefix)
        ]

    def parse_go_term(self):
        go = GO()
        go.primaryDomainId = self.primary_id
        go.domainIds = [self.primary_id]
        go.displayName = self.display_name
        go.synonyms = self.synonyms
        go.description = self.description

        return go

    def parse_go_relationships(self):
        return [GOIsSubtypeOfGO(sourceDomainId=self.primary_id, targetDomainId=target) for target in self.is_a]


def get_go_details(g):
    go_details = _defaultdict(list)

    for s, p, o in _tqdm(g, total=len(g)):
        if str(s).startswith("http://purl.obolibrary.org/obo/GO_"):
            go_details[s].append(
                (
                    p,
                    o,
                )
            )

    return go_details


def parse_go():
    g = _Graph()
    logger.info("Parsing OWL core")
    g.parse(get_file_location("go_core_owl"))
    logger.info("Consolidating relationships")
    details = get_go_details(g)

    logger.info("Parsing and storing GO terms")
    updates = (GORelations(value).parse_go_term().generate_update() for value in details.values())
    for chunk in _chunked(updates, 1_000):
        MongoInstance.DB[GO.collection_name].bulk_write(chunk)

    logger.info("Parsing and storing relationships between GO terms")
    updates = (GORelations(value).parse_go_relationships() for value in details.values())
    for chunk in _chunked(updates, 1_000):
        chunk = [rel.generate_update() for rel in _chain(*chunk)]
        MongoInstance.DB[GOIsSubtypeOfGO.collection_name].bulk_write(chunk)
