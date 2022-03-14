import json

from more_itertools import chunked

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.tissue import Tissue

get_file_location = _get_file_location_factory("uberon")


def parse():
    with open(get_file_location("ext")) as f:
        uberon = json.load(f)

    nodes = uberon["graphs"][0]["nodes"]
    uberon_nodes = [node for node in nodes if node["id"].startswith("http://purl.obolibrary.org/obo/UBERON_")]

    tissues = (
        Tissue(
            primaryDomainId=f"uberon.{node['id'].replace('http://purl.obolibrary.org/obo/UBERON_', '')}",
            domainIds=[f"uberon.{node['id'].replace('http://purl.obolibrary.org/obo/UBERON_', '')}"],
            displayName=node.get("lbl", ""),
        ).generate_update()
        for node in uberon_nodes
    )

    for chunk in chunked(tissues, 1_000):
        MongoInstance.DB[Tissue.collection_name].bulk_write(chunk)
