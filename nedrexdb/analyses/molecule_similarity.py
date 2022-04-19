from datetime import datetime
from itertools import combinations
from typing import Final

import rdkit.Chem as Chem  # type: ignore
from more_itertools import chunked
from pymongo import UpdateOne  # type: ignore
from rdkit import DataStructs, RDLogger  # type: ignore
from rdkit.Chem import AllChem, MACCSkeys  # type: ignore
from tqdm import tqdm  # type: ignore

from nedrexdb.db import MongoInstance


if MongoInstance.DB is None:
    raise TypeError("MongoInstance must be configured before importing molecule_similarity")
else:
    _DRUG_COLL: Final = MongoInstance.DB["drug"]
    _DRUG_SIMILARITY_COLL: Final = MongoInstance.DB["molecule_similarity_molecule"]

# disable logging, because we expect warnings
RDLogger.DisableLog("rdApp.*")


def set_indexes():
    _DRUG_SIMILARITY_COLL.create_index("memberOne")
    _DRUG_SIMILARITY_COLL.create_index("memberTwo")
    _DRUG_SIMILARITY_COLL.create_index([("memberOne", 1), ("memberTwo", 1)])


def get_parsable_drugs():
    # get SMILES strings
    drugs = {doc["primaryDomainId"]: doc.get("smiles") for doc in _DRUG_COLL.find()}
    # remove key-value pairs where the value is None
    drugs = {k: v for k, v in drugs.items() if v}
    # remove key-value pairs with unparsable values
    drugs = {k: Chem.MolFromSmiles(v) for k, v in drugs.items() if Chem.MolFromSmiles(v)}
    return drugs


def find_similar_compounds_morgan(parsable_drugs) -> None:
    morgan_fps = {k: AllChem.GetMorganFingerprintAsBitVect(v, 2, nBits=16_384) for k, v in parsable_drugs.items()}

    pairs_iter = combinations(sorted(parsable_drugs.keys()), 2)
    filtered_pairs = (
        (
            a,
            b,
        )
        for a, b in pairs_iter
        if DataStructs.TanimotoSimilarity(morgan_fps[a], morgan_fps[b]) >= 0.3
    )

    for chunk in tqdm(chunked(filtered_pairs, 1_000), leave=False, desc="Finding similar compounds (Morgan R2)"):
        tnow = datetime.utcnow()
        updates = [
            UpdateOne(
                {"memberOne": a, "memberTwo": b},
                {
                    "$set": {
                        "updated": tnow,
                    },
                    "$setOnInsert": {"created": tnow, "type": "MoleculeSimilarityMolecule"},
                },
                upsert=True,
            )
            for a, b in chunk
        ]

        _DRUG_SIMILARITY_COLL.bulk_write(updates)  # type: ignore


def find_similar_compounds_maccs(parsable_drugs) -> None:
    maccs_fps = {k: MACCSkeys.GenMACCSKeys(v) for k, v in parsable_drugs.items()}

    pairs_iter = combinations(sorted(parsable_drugs.keys()), 2)
    filtered_pairs = (
        (
            a,
            b,
        )
        for a, b in pairs_iter
        if DataStructs.TanimotoSimilarity(maccs_fps[a], maccs_fps[b]) >= 0.8
    )

    for chunk in tqdm(chunked(filtered_pairs, 1_000), leave=False, desc="Finding similar compounds (MACCS)"):
        tnow = datetime.utcnow()
        updates = [
            UpdateOne(
                {"memberOne": a, "memberTwo": b},
                {
                    "$set": {
                        "updated": tnow,
                    },
                    "$setOnInsert": {"created": tnow, "type": "MoleculeSimilarityMolecule"},
                },
                upsert=True,
            )
            for a, b in chunk
        ]

        _DRUG_SIMILARITY_COLL.bulk_write(updates)  # type: ignore


def calculate_similarity(parsable_drugs):
    fingerprints = {}
    fingerprints["maccs"] = {k: MACCSkeys.GenMACCSKeys(v) for k, v in parsable_drugs.items()}

    for r in range(1, 5):
        fingerprints[f"morgan_r{r}"] = {
            k: AllChem.GetMorganFingerprintAsBitVect(v, r, nBits=16_384) for k, v in parsable_drugs.items()
        }

    comparisons = (
        (
            doc["memberOne"],
            doc["memberTwo"],
        )
        for doc in _DRUG_SIMILARITY_COLL.find()
    )

    for chunk in tqdm(chunked(comparisons, 1_000), leave=False, desc="Calculating compound similarities"):
        tnow = datetime.utcnow()
        updates = []

        for a, b in chunk:
            query = {"memberOne": a, "memberTwo": b}
            update = {"$set": {"updated": tnow}}

            for r in range(1, 5):
                key = f"morgan_r{r}"
                update["$set"][key] = DataStructs.TanimotoSimilarity(fingerprints[key][a], fingerprints[key][b])

            update["$set"]["maccs"] = DataStructs.TanimotoSimilarity(fingerprints["maccs"][a], fingerprints["maccs"][b])

            updates.append(UpdateOne(query, update))

        _DRUG_SIMILARITY_COLL.bulk_write(updates)


def run():
    set_indexes()
    parsable_drugs = get_parsable_drugs()
    find_similar_compounds_morgan(parsable_drugs)
    find_similar_compounds_maccs(parsable_drugs)
    calculate_similarity(parsable_drugs)
