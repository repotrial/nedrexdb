import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class DrugHastargetBase(models.MongoMixin):
    edge_type: str = "DrugHasTarget"
    collection_name: str = "drug_has_target"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)], unique=True)


class DrugHasTarget(_BaseModel, DrugHastargetBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""
    actions: list[str] = []
    dataSources: list[str] = []
    # TODO: Decide whether to keep this field in NeDRexDB.
    tags: list[str] = []

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {
            "sourceDomainId": self.sourceDomainId,
            "targetDomainId": self.targetDomainId,
        }
        update = {
            "$setOnInsert": {"created": tnow},
            "$addToSet": {
                "actions": {"$each": self.actions},
                "dataSources": {"$each": self.dataSources},
                "tags": {"$each": self.tags},
            },
            "$set": {"updated": tnow, "type": self.edge_type},
        }

        return _UpdateOne(query, update, upsert=True)
