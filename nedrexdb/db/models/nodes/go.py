import datetime as _datetime

from pydantic import BaseModel as _BaseModel, Field as _Field, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class GOBase(models.MongoMixin):
    node_type: str = "GO"
    collection_name: str = "go"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)
        db[cls.collection_name].create_index("domainIds")


class GO(_BaseModel, GOBase):
    primaryDomainId: _StrictStr = ""
    domainIds: list[str] = _Field(default_factory=list)

    displayName: _StrictStr = ""
    synonyms: list[str] = _Field(default_factory=list)
    description: _StrictStr = ""

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"primaryDomainId": self.primaryDomainId}
        update = {
            "$addToSet": {"domainIds": {"$each": self.domainIds}, "synonyms": {"$each": self.synonyms}},
            "$set": {
                "displayName": self.displayName,
                "description": self.description,
                "type": self.node_type,
                "updated": tnow,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
