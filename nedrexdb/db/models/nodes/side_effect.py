import datetime as _datetime

from pydantic import BaseModel as _BaseModel, Field as _Field, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class SideEffectBase(models.MongoMixin):
    node_type: str = "SideEffect"
    collection_name: str = "side_effect"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)
        db[cls.collection_name].create_index("domainIds")


class SideEffect(_BaseModel, SideEffectBase):
    class Config:
        validate_assignment = True

    primaryDomainId: _StrictStr = ""
    domainIds: list[str] = _Field(default_factory=list)
    displayName: _StrictStr = ""

    dataSources: list[str] = _Field(default_factory=list)

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"primaryDomainId": self.primaryDomainId}
        update = {
            "$addToSet": {
                "domainIds": {"$each": self.domainIds},
                "dataSources": {"$each": self.dataSources},
            },
            "$set": {"displayName": self.displayName, "updated": tnow},
            "$setOnInsert": {"created": tnow, "type": self.node_type},
        }
        return _UpdateOne(query, update, upsert=True)
