import datetime as _datetime

from pydantic import BaseModel as _BaseModel, Field as _Field, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class TissueBase(models.MongoMixin):
    node_type: str = "Tissue"
    collection_name: str = "tissue"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)


class Tissue(_BaseModel, TissueBase):
    primaryDomainId: _StrictStr = ""
    domainIds: list[str] = _Field(default_factory=list)
    displayName: _StrictStr = ""
    organ: _StrictStr = ""
    dataSources: list[str] = _Field(default_factory=list)

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"primaryDomainId": self.primaryDomainId}
        update = {
            "$addToSet": {
                "domainIds": {"$each": self.domainIds},
                "dataSources": {"$each": self.dataSources},
            },
            "$set": {
                "type": self.node_type,
                "updated": tnow,
                "displayName": self.displayName,
                "organ": self.organ,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
