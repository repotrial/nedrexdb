from itertools import chain

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.nodes.tissue import Tissue
from nedrexdb.db.models.edges.gene_expressed_in_tissue import GeneExpressedInTissue
from nedrexdb.db.models.edges.protein_expressed_in_tissue import ProteinExpressedInTissue


def trim_uberon():
    coll_1 = MongoInstance.DB[GeneExpressedInTissue.collection_name]
    coll_2 = MongoInstance.DB[ProteinExpressedInTissue.collection_name]
    tissue_coll = MongoInstance.DB[Tissue.collection_name]

    docs = chain(coll_1.find(), coll_2.find())
    used_uberon_ids = {doc["targetDomainId"] for doc in docs}
    all_uberon_ids = {doc["primaryDomainId"] for doc in tissue_coll.find()}
    unused_uberon_ids = all_uberon_ids - used_uberon_ids

    query = {"primaryDomainId": {"$in": list(unused_uberon_ids)}}
    tissue_coll.delete_many(query)
