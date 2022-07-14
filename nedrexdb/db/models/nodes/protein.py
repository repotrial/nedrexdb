import datetime as _datetime

from pydantic import (
    BaseModel as _BaseModel,
    Field as _Field,
    StrictStr as _StrictStr,
)
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class ProteinBase(models.MongoMixin):
    node_type: str = "Protein"
    collection_name: str = "protein"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)
        db[cls.collection_name].create_index("domainIds")
        db[cls.collection_name].create_index("taxid")


class Protein(_BaseModel, ProteinBase):
    class Config:
        validate_assignment = True

    primaryDomainId: _StrictStr = ""
    domainIds: list[str] = _Field(default_factory=list)

    displayName: _StrictStr = ""
    synonyms: list[str] = _Field(default_factory=list)
    comments: _StrictStr = ""
    geneName: _StrictStr = ""

    taxid: int = -1
    sequence: _StrictStr = ""

    dataSources: list[str] = _Field(default_factory=list)

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"primaryDomainId": self.primaryDomainId}
        update = {
            "$addToSet": {
                "domainIds": {"$each": self.domainIds},
                "synonyms": {"$each": self.synonyms},
                "dataSources": {"$each": self.dataSources},
            },
            "$set": {
                "displayName": self.displayName,
                "comments": self.comments,
                "geneName": self.geneName,
                "taxid": self.taxid,
                "sequence": self.sequence,
                "type": self.node_type,
                "updated": tnow,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
