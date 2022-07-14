from nedrexdb.db import MongoInstance
from nedrexdb.logger import logger


def drop_empty_collections():
    """
    This is requires to ensure that Neo4j export works correctly
    """
    for coll_name in MongoInstance.DB.list_collection_names():
        coll = MongoInstance.DB[coll_name]
        try:
            next(coll.find())
        except StopIteration:
            # indicates empty collection
            logger.warning(f"collection {coll_name!r} is empty, dropping")
            coll.drop()
