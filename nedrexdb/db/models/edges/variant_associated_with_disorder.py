import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class VariantAssociatedWithDisorderBase(models.MongoMixin):
    edge_type = "VariantAssociatedWithDisorder"
    collection_name = "variant_associated_with_disorder"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("accession", unique=True)
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index("sourceDomainId")


class VariantAssociatedWithDisorder(_BaseModel, VariantAssociatedWithDisorderBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""
    accession: _StrictStr = ""

    reviewStatus: _StrictStr = ""
    effects: list[str] = []

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"accession": self.accession}

        update = {
            "$set": {
                "updated": tnow,
                "type": self.edge_type,
                "sourceDomainId": self.sourceDomainId,
                "targetDomainId": self.targetDomainId,
                "reviewStatus": self.reviewStatus,
            },
            "$addToSet": {
                "effects": {"$each": self.effects},
            },
            "$setOnInsert": {
                "created": tnow,
            },
        }

        return _UpdateOne(query, update, upsert=True)
