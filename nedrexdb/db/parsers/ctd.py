from collections import defaultdict as _defaultdict
from csv import DictReader as _DictReader
from typing import Optional as _Optional
from itertools import product as _product, chain as _chain
import gzip as _gzip

from more_itertools import chunked as _chunked

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.disorder import Disorder
from nedrexdb.db.models.nodes.drug import Drug
from nedrexdb.db.models.edges.drug_has_indication import DrugHasIndication

get_file_location = _get_file_location_factory("ctd")


class CTDDrugChemicalRow:
    def __init__(self, row):
        self._row = row

    def drug_ids(self, casn_map: dict[str, list[str]]) -> _Optional[list[str]]:
        drug_id = self._row["CasRN"]
        if drug_id:
            return casn_map.get(drug_id, [])
        else:
            return []

    def disorder_ids(self, mn_map: dict[str, list[str]]) -> _Optional[list[str]]:
        disease_id = self._row["DiseaseID"]
        if disease_id:
            return mn_map.get(disease_id.replace("MESH:", "mesh."), [])
        else:
            return []

    def parse(self, casn_map, mn_map):
        indications = []

        for drug, disorder in _product(self.drug_ids(casn_map), self.disorder_ids(mn_map)):
            dhi = DrugHasIndication(sourceDomainId=drug, targetDomainId=disorder, dataSources=["ctd"])
            indications.append(dhi)

        return indications


def mesh_to_nedrex_map() -> dict[str, list[str]]:
    mn_map = _defaultdict(list)

    for doc in Disorder.find(MongoInstance.DB):
        mesh_ids = [mid for mid in doc["domainIds"] if mid.startswith("mesh.")]
        for mid in mesh_ids:
            mn_map[mid].append(doc["primaryDomainId"])

    return mn_map


def cas_rn_to_nedrex_map() -> dict[str, list[str]]:
    casn_map = _defaultdict(list)

    for doc in Drug.find(MongoInstance.DB):
        casn_map[doc["casNumber"]].append(doc["primaryDomainId"])

    return casn_map


def parse():
    fieldnames = [
        "ChemicalName",
        "ChemicalID",
        "CasRN",
        "DiseaseName",
        "DiseaseID",
        "DirectEvidence",
        "InferenceGeneSymbol",
        "InferenceScore",
        "OmimIDs",
        "PubMedIDs",
    ]
    fname = get_file_location("chemical_disease_relationships")
    casn_map = cas_rn_to_nedrex_map()
    mn_map = mesh_to_nedrex_map()

    with _gzip.open(fname, "rt") as f:
        reader = _DictReader(f, delimiter="\t", fieldnames=fieldnames)
        updates = (
            CTDDrugChemicalRow(row).parse(casn_map, mn_map) for row in reader if row["DirectEvidence"] == "therapeutic"
        )

        for chunk in _chunked(updates, 1_000):
            chunk = [i.generate_update() for i in _chain(*chunk)]
            MongoInstance.DB[DrugHasIndication.collection_name].bulk_write(chunk)
