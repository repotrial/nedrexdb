from collections import defaultdict

from tqdm import tqdm

from nedrexdb import config as _config


def profile_collections(db):
    nodes = _config["api.node_collections"]
    edges = _config["api.edge_collections"]

    collections = nodes + edges
    for coll in collections:
        print(coll)
        doc_count = 0
        attr_counts = defaultdict(int)

        for doc in tqdm(db[coll].find(), leave=False):
            doc_count += 1
            for attr in doc.keys():
                attr_counts[attr] += 1

        unique_attrs = list(attr_counts.keys())
        db["_collections"].update_one(
            {"collection": coll},
            {
                "$set": {
                    "document_count": doc_count,
                    "unique_attributes": unique_attrs,
                    "attribute_counts": dict(attr_counts),
                },
            },
            upsert=True,
        )
