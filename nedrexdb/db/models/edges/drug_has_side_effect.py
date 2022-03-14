import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr, Field as _Field
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class DrugHasSideEffectBase(models.MongoMixin):
    edge_type: str = "DrugHasSideEffect"
    collection_name: str = "drug_has_side_effect"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)], unique=True)


class DrugHasSideEffect(_BaseModel, DrugHasSideEffectBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""
    maximum_frequency: float = -1
    minimum_frequency: float = 100
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
                "type": self.edge_type,
            },
            "$setOnInsert": {
                "created": tnow,
            },
            "$max": {"maximum_frequency": self.maximum_frequency},
            "$min": {"minimum_frequency": self.minimum_frequency},
            "$addToSet": {
                "assertedBy": {"$each": self.assertedBy},
            },
        }

        return _UpdateOne(query, update, upsert=True)
