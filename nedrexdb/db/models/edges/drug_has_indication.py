import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr, Field as _Field
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class DrugHasIndicationBase(models.MongoMixin):
    edge_type: str = "DrugHasIndication"
    collection_name: str = "drug_has_indication"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)], unique=True)


class DrugHasIndication(_BaseModel, DrugHasIndicationBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""
    assertedBy: list[str] = _Field(default_factory=list)

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {
            "sourceDomainId": self.sourceDomainId,
            "targetDomainId": self.targetDomainId,
        }

        update = {
            "$set": {
                "updated": tnow,
            },
            "$setOnInsert": {
                "created": tnow,
            },
            "$addToSet": {"assertedBy": {"$each": self.assertedBy}},
        }

        return _UpdateOne(query, update, upsert=True)
