import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class ProteinEncodedByGeneBase(models.MongoMixin):
    edge_type: str = "ProteinEncodedByGene"
    collection_name: str = "protein_encoded_by_gene"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)])


class ProteinEncodedByGene(_BaseModel, ProteinEncodedByGeneBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {
            "sourceDomainId": self.sourceDomainId,
            "targetDomainId": self.targetDomainId,
        }
        update = {"$setOnInsert": {"created": tnow}, "$set": {"updated": tnow, "type": self.edge_type}}

        return _UpdateOne(query, update, upsert=True)
