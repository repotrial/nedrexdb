import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class VariantAssociatedWithDisorderBase(models.MongoMixin):
    edge_type = "VariantAssociatedWithDisorder"
    collection_name = "variant_associated_with_disorder"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)], unique=True)


class VariantAssociatedWithDisorder(_BaseModel, VariantAssociatedWithDisorderBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""

    reviewStatus: _StrictStr = ""
    effects: list[dict[str, str]] = []

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"sourceDomainId": self.sourceDomainId, "targetDomainId": self.targetDomainId}

        update = {
            "$set": {
                "updated": tnow,
                "type": self.edge_type,
            },
            "$addToSet": {
                "effects": {"$each": self.effects},
            },
            "$setOnInsert": {
                "created": tnow,
            },
        }

        return _UpdateOne(query, update, upsert=True)
