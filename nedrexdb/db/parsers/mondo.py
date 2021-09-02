import json as _json

from more_itertools import chunked as _chunked

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.disorder import Disorder
from nedrexdb.db.models.edges.disorder_is_subtype_of_disorder import DisorderIsSubtypeOfDisorder

get_file_location = _get_file_location_factory("mondo")


class MondoRecord:
    _ID_NAMESPACES = {
        "http://purl.obolibrary.org/obo/DOID_": "doid.",
        "http://linkedlifedata.com/resource/umls/id/": "umls.",
        "http://purl.obolibrary.org/obo/NCIT_": "ncit.",
        "http://identifiers.org/mesh/": "mesh.",
        "http://identifiers.org/omim/": "omim.",
        "http://identifiers.org/snomedct/": "snomedct.",
        "http://www.orpha.net/ORDO/Orphanet_": "orpha.",
        "http://identifiers.org/meddra/": "meddra.",
        "http://identifiers.org/medgen/": "medgen.",
    }

    def __init__(self, record):
        self._record = record

    def get_id(self) -> str:
        return self._record["id"].replace("http://purl.obolibrary.org/obo/MONDO_", "mondo.")

    def get_description(self) -> str:
        try:
            return self._record["meta"]["definition"]["val"]
        except KeyError:
            return ""

    def get_domain_ids(self) -> list[str]:
        domain_ids: list[str] = []
        exact_match = "http://www.w3.org/2004/02/skos/core#exactMatch"

        try:
            bpv = self._record["meta"]["basicPropertyValues"]
        except KeyError:
            return domain_ids

        for item in bpv:
            if not item["pred"] == exact_match:
                continue

            val = item["val"]
            for k, v in self._ID_NAMESPACES.items():
                if val.startswith(k):
                    domain_id = val.replace(k, v)
                    domain_ids.append(domain_id)

        return domain_ids

    def get_display_name(self) -> str:
        return self._record["lbl"]

    def get_icd10_codes(self) -> list[str]:
        icd10_codes: list[str] = []

        try:
            xrefs = self._record["meta"]["xrefs"]
        except KeyError:
            return icd10_codes

        icd10_codes += [xref["val"].replace("ICD10:", "") for xref in xrefs if xref["val"].startswith("ICD10:")]
        return icd10_codes

    def get_synonyms(self) -> list[str]:
        synonyms: list[str] = []

        try:
            syns = self._record["meta"]["synonyms"]
        except KeyError:
            return synonyms

        for syn in syns:
            if syn["pred"] != "hasExactSynonym":
                continue
            synonym = syn["val"]
            synonyms.append(synonym)

        return synonyms

    def parse(self):
        d = Disorder()
        d.primaryDomainId = self.get_id()
        d.domainIds = self.get_domain_ids()

        if d.primaryDomainId not in d.domainIds:
            d.domainIds.append(d.primaryDomainId)

        d.description = self.get_description()
        d.displayName = self.get_display_name()
        d.synonyms = self.get_synonyms()
        d.icd10 = self.get_icd10_codes()

        return d


def _is_mondo_node(node) -> bool:
    prefix = "http://purl.obolibrary.org/obo/MONDO_"
    return node["id"].startswith(prefix)


def _is_deprecated(node) -> bool:
    if node.get("meta") and node["meta"].get("deprecated"):
        return True
    return False


def _parse_edges(edges):
    prefix = "http://purl.obolibrary.org/obo/MONDO_"
    for edge in edges:
        if not edge["sub"].startswith(prefix):
            continue
        if not edge["obj"].startswith(prefix):
            continue
        if not edge["pred"] == "is_a":
            continue

        # diad = disorder is a disorder
        diad = DisorderIsSubtypeOfDisorder()
        diad.sourceDomainId = edge["sub"].replace(prefix, "mondo.")
        diad.targetDomainId = edge["obj"].replace(prefix, "mondo.")

        yield diad.generate_update()


def parse_mondo_json():
    # Get the filename based on the config
    filename = get_file_location("json")
    with open(filename, "r") as f:
        mondo_data = _json.load(f)

    graph = mondo_data["graphs"].pop()

    nodes = graph["nodes"]
    nodes = filter(_is_mondo_node, nodes)
    nodes = filter(lambda i: not _is_deprecated(i), nodes)

    mondo_records = (MondoRecord(node).parse().generate_update() for node in nodes)
    for chunk in _chunked(mondo_records, 1_000):
        MongoInstance.DB[Disorder.collection_name].bulk_write(chunk)

    edges = graph["edges"]
    for chunk in _chunked(_parse_edges(edges), 1_000):
        MongoInstance.DB[DisorderIsSubtypeOfDisorder.collection_name].bulk_write(chunk)
