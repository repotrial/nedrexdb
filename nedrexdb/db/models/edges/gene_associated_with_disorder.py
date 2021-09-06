import datetime as _datetime
from typing import Optional as _Optional

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class GeneAssociatedWithDisorderBase(models.MongoMixin):
    edge_type: str = "GeneAssociatedWithDisorder"
    collection_name: str = "gene_associated_with_disorder"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)], unique=True)


class GeneAssociatedWithDisorder(_BaseModel, GeneAssociatedWithDisorderBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""
    assertedBy: list[str] = []
    score: _Optional[float] = None

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {
            "sourceDomainId": self.sourceDomainId,
            "targetDomainId": self.targetDomainId,
        }
        update = {
            "$setOnInsert": {"created": tnow},
            "$set": {"updated": tnow, "type": self.edge_type},
            "$addToSet": {"assertedBy": {"$each": self.assertedBy}},
        }

        if self.score:
            update["$set"]["score"] = self.score

        return _UpdateOne(query, update, upsert=True)
