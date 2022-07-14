import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr, Field as _Field
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class GOIsSubtypeOfGOBase(models.MongoMixin):
    edge_type: str = "GOIsSubtypeOfGO"
    collection_name: str = "go_is_subtype_of_go"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)], unique=True)


class GOIsSubtypeOfGO(_BaseModel, GOIsSubtypeOfGOBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""
    dataSources: list[str] = _Field(default_factory=list)

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"sourceDomainId": self.sourceDomainId, "targetDomainId": self.targetDomainId}
        update = {
            "$setOnInsert": {"created": tnow},
            "$set": {"updated": tnow, "type": self.edge_type},
            "$addToSet": {"dataSources": {"$each": self.dataSources}},
        }

        return _UpdateOne(query, update, upsert=True)
