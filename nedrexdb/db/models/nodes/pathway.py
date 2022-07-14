import datetime as _datetime

from pydantic import BaseModel as _BaseModel, Field as _Field, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class PathwayBase(models.MongoMixin):
    node_type: str = "Pathway"
    collection_name: str = "pathway"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)


class Pathway(_BaseModel, PathwayBase):
    primaryDomainId: _StrictStr = ""
    domainIds: list[str] = _Field(default_factory=list)
    displayName: _StrictStr = ""
    species: _StrictStr = ""
    taxid: int = -1
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
                "taxid": self.taxid,
                "species": self.species,
                "displayName": self.displayName,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
