import datetime as _datetime
from typing import Optional as _Optional

from pydantic import (
    BaseModel as _BaseModel,
    Field as _Field,
    StrictStr as _StrictStr,
)
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class GeneBase(models.MongoMixin):
    node_type: str = "Gene"
    collection_name: str = "gene"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)
        db[cls.collection_name].create_index("domainId")
        db[cls.collection_name].create_index("approvedSymbol")
        db[cls.collection_name].create_index("symbols")


class Gene(_BaseModel, GeneBase):
    primaryDomainId: _StrictStr = ""
    domainIds: list[str] = _Field(default_factory=list)

    displayName: _StrictStr = ""
    synonyms: list[str] = _Field(default_factory=list)
    approvedSymbol: _Optional[_StrictStr] = None
    symbols: list[str] = _Field(default_factory=list)
    description: _StrictStr = ""

    chromosome: _StrictStr = ""
    mapLocation: _StrictStr = ""
    geneType: _StrictStr = ""

    dataSources: list[str] = _Field(default_factory=list)

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"primaryDomainId": self.primaryDomainId}
        update = {
            "$addToSet": {
                "domainIds": {"$each": self.domainIds},
                "synonyms": {"$each": self.synonyms},
                "symbols": {"$each": self.symbols},
                "dataSources": {"$each": self.dataSources},
            },
            "$set": {
                "displayName": self.displayName,
                "approvedSymbol": self.approvedSymbol,
                "description": self.description,
                "chromosome": self.chromosome,
                "mapLocation": self.mapLocation,
                "geneType": self.geneType,
                "type": self.node_type,
                "updated": tnow,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
