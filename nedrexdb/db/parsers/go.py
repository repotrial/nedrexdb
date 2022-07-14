import gzip as _gzip
from collections import defaultdict as _defaultdict
from csv import DictReader as _DictReader
from itertools import chain as _chain

from more_itertools import chunked as _chunked
from rdflib import Graph as _Graph, term as _term
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.go import GO
from nedrexdb.db.models.nodes.protein import Protein
from nedrexdb.db.models.edges.go_is_subtype_of_go import GOIsSubtypeOfGO
from nedrexdb.db.models.edges.protein_has_go_annotation import ProteinHasGOAnnotation
from nedrexdb.logger import logger

get_file_location = _get_file_location_factory("go")


def iter_go_associations(f):
    fieldnames = (
        "DB",
        "DB Object ID",
        "DB Object Symbol",
        "Qualifier",
        "GO ID",
        "DB:Reference (IDB:Reference)",
        "Evidence Code",
        "With (or) From",
        "Aspect",
        "DB Object Name",
        "DB Object Synonym (ISynonym)",
        "DB Object Type",
        "Taxon(Itaxon)" "Date",
        "Assigned By",
        "Annotation Extension",
        "Gene Product Form ID",
    )

    with _gzip.open(f, "rt") as f:
        f = filter(lambda line: not line.startswith("!"), f)
        reader = _DictReader(f, fieldnames=fieldnames, delimiter="\t")
        for row in reader:
            yield row


class GOAssociation:
    def __init__(self, row):
        self._row = row

    @property
    def source_domain_id(self):
        return f"uniprot.{self._row['DB Object ID']}"

    @property
    def target_domain_id(self):
        return self._row["GO ID"].replace("GO:", "go.")

    @property
    def qualifiers(self):
        return self._row["Qualifier"].split("|")

    def parse(self):
        return ProteinHasGOAnnotation(
            sourceDomainId=self.source_domain_id,
            targetDomainId=self.target_domain_id,
            qualifiers=self.qualifiers,
            dataSources=["go"],
        )


class GORelations:
    def __init__(self, po):
        # po refers to 'predicate object'
        self._po = po

    @property
    def is_deprecated(self):
        for p, o in self._po:
            if p == _term.URIRef("http://www.w3.org/2002/07/owl#deprecated"):
                if o == _term.Literal("true", datatype=_term.URIRef("http://www.w3.org/2001/XMLSchema#boolean")):
                    return True
        return False

    @property
    def primary_id(self):
        for p, o in self._po:
            if str(p) == "http://www.geneontology.org/formats/oboInOwl#id":
                return str(o).replace("GO:", "go.")
        raise Exception(f"{[(p,o,) for p, o in self._po]}")

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
        go.dataSources = ["go"]

        return go

    def parse_go_relationships(self):
        return [
            GOIsSubtypeOfGO(sourceDomainId=self.primary_id, targetDomainId=target, dataSources=["go"])
            for target in self.is_a
        ]


def get_go_details(g):
    go_details = _defaultdict(list)

    for s, p, o in g:
        if str(s).startswith("http://purl.obolibrary.org/obo/GO_"):
            go_details[s].append((p, o))

    return go_details


def parse_go():
    g = _Graph()
    logger.info("Parsing OWL core")
    g.parse(get_file_location("go_core_owl"))
    logger.info("Consolidating relationships")
    details = get_go_details(g)

    logger.info("Parsing and storing GO terms")
    updates = (GORelations(value) for value in details.values())
    updates = (go_rel.parse_go_term().generate_update() for go_rel in updates if not go_rel.is_deprecated)
    for chunk in _tqdm(_chunked(updates, 1_000), leave=False, desc="Parsing GO terms"):
        MongoInstance.DB[GO.collection_name].bulk_write(chunk)

    logger.info("Parsing and storing relationships between GO terms")
    updates = (GORelations(value).parse_go_relationships() for value in details.values())
    for chunk in _tqdm(_chunked(updates, 1_000), leave=False, desc="Parsing relationships between GO terms"):
        chunk = [rel.generate_update() for rel in _chain(*chunk)]
        MongoInstance.DB[GOIsSubtypeOfGO.collection_name].bulk_write(chunk)


def parse_goa():
    go_terms = {doc["primaryDomainId"] for doc in GO.find(MongoInstance.DB)}
    proteins = {doc["primaryDomainId"] for doc in Protein.find(MongoInstance.DB)}

    file = get_file_location("go_annotations")

    go_associations = iter_go_associations(file)
    go_associations = (GOAssociation(assoc) for assoc in go_associations if assoc["DB"] == "UniProtKB")
    go_associations = (assoc for assoc in go_associations if assoc.source_domain_id in proteins)
    go_associations = (assoc for assoc in go_associations if assoc.target_domain_id in go_terms)

    for chunk in _tqdm(_chunked(go_associations, 1_000), leave=False, desc="Parsing GO annotations for proteins"):
        update = [assoc.parse().generate_update() for assoc in chunk]
        MongoInstance.DB[ProteinHasGOAnnotation.collection_name].bulk_write(update)
