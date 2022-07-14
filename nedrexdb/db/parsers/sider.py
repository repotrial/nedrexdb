import csv
import gzip
from collections import defaultdict
from itertools import chain, product

from more_itertools import chunked
from tqdm import tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.drug import Drug
from nedrexdb.db.models.nodes.side_effect import SideEffect
from nedrexdb.db.models.edges.drug_has_side_effect import DrugHasSideEffect

get_file_location = _get_file_location_factory("sider")


def pubchem_to_drugbank_map():
    d = defaultdict(list)

    for drug in Drug.find(MongoInstance.DB):
        pubchem_ids = [i.split(".")[1] for i in drug["domainIds"] if i.startswith("pubchem.")]
        # NOTE: Remove IDs > than 8 chars in length.
        # SIDER uses stitch IDs, which arebased on PubChem IDs.
        # From what I can tell, IDs start CID1 or CID0,
        # followed by a zero-padded 8-char PubChem ID.
        pubchem_ids = [i for i in pubchem_ids if len(i) <= 8]
        for pcid in pubchem_ids:
            d[f"CID0{pcid.zfill(8)}"].append(drug["primaryDomainId"])
            d[f"CID1{pcid.zfill(8)}"].append(drug["primaryDomainId"])

    return d


def umls_to_meddra_map():
    d = defaultdict(list)

    for side_effect in SideEffect.find(MongoInstance.DB):
        umls_ids = [i.split(".")[1] for i in side_effect["domainIds"] if i.startswith("umls")]
        for umls_id in umls_ids:
            d[umls_id].append(side_effect["primaryDomainId"])

    return d


def parse():
    fname = get_file_location("frequency_data")

    pc_db_map = pubchem_to_drugbank_map()
    mt_md_map = umls_to_meddra_map()

    updates = []

    with gzip.open(fname, "rt") as f:
        reader = csv.reader(f, delimiter="\t")

        for row in reader:
            # NOTE: This seems to be handled differently on the SIDER
            # website (see http://sideeffects.embl.de/drugs/85/pt)
            if row[3] == "placebo":
                continue

            drugs = set(chain(*[pc_db_map.get(i, []) for i in row[:2]]))
            side_effects = set(chain(*[mt_md_map.get(i, []) for i in [row[2], row[8]]]))
            min_freq, max_freq = row[5:7]

            for drug, side_effect in product(drugs, side_effects):
                dhse = DrugHasSideEffect(
                    sourceDomainId=drug,
                    targetDomainId=side_effect,
                    maximum_frequency=float(max_freq),
                    minimum_frequency=float(min_freq),
                    dataSources=["sider"],
                )

                updates.append(dhse.generate_update())

    for chunk in tqdm(chunked(updates, 1_000)):
        MongoInstance.DB[DrugHasSideEffect.collection_name].bulk_write(chunk)
