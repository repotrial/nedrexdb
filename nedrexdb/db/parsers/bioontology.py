import json

from more_itertools import chunked

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.phenotype import Phenotype
from nedrexdb.db.models.nodes.side_effect import SideEffect
from nedrexdb.db.models.edges.side_effect_same_as_phenotype import SideEffectSameAsPhenotype

get_file_location = _get_file_location_factory("bioontology")


def parse():
    fname = get_file_location("meddra_mappings")

    with fname.open() as f:
        data = json.load(f)

    # Parse SideEffect nodes.
    meddra_items = {}

    for cui in data:
        for meddra in cui["meddra_terms"]:
            primary_id = f"meddra.{meddra['url'].rsplit('/', 1)[1]}"

            if meddra_items.get(primary_id):
                meddra_items[primary_id].domainIds.append(f"umls.{cui['cui']}")
            else:
                meddra_items[primary_id] = SideEffect(
                    primaryDomainId=primary_id,
                    domainIds=[primary_id, f"umls.{cui['cui']}"],
                    displayName=meddra["name"],
                    dataSources=["bioontology.org"],
                )

    updates = (se.generate_update() for se in meddra_items.values())
    for chunk in chunked(updates, 1_000):
        MongoInstance.DB[SideEffect.collection_name].bulk_write(chunk)

    # Parse SideEffect-(SameAs)-Phenoyype edges.
    nedrex_phenotypes = {i["primaryDomainId"] for i in Phenotype.find(MongoInstance.DB)}
    se_pheno_relations = []

    for cui in data:
        for meddra in cui["meddra_terms"]:
            meddra_id = f"meddra.{meddra['url'].rsplit('/', 1)[1]}"

            hpo_mappings = [hpo for hpo in meddra["hpo_mappings"] if hpo in nedrex_phenotypes]
            se_pheno_relations += [
                SideEffectSameAsPhenotype(sourceDomainId=meddra_id, targetDomainId=hpo, dataSources=["bioontology.org"])
                for hpo in hpo_mappings
            ]

    updates = (rel.generate_update() for rel in se_pheno_relations)
    for chunk in chunked(updates, 1_000):
        MongoInstance.DB[SideEffectSameAsPhenotype.collection_name].bulk_write(chunk)
