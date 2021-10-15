import datetime as _datetime
from typing import List as _List

from pydantic import BaseModel as _BaseModel, Field as _Field, StrictStr as _StrictStr

from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class PhenotypeBase(models.MongoMixin):
    node_type: str = "Phenotype"
    collection_name: str = "phenotype"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)


class Phenotype(_BaseModel, PhenotypeBase):
    class Config:
        validate_assignment = True

    primaryDomainId: _StrictStr = ""
    domainIds: _List[str] = _Field(default_factory=list)

    displayName: _StrictStr = ""
    synonyms: _List[str] = _Field(default_factory=list)

    description: _StrictStr = ""

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"primaryDomainId": self.primaryDomainId}
        update = {
            "$addToSet": {
                "domainIds": {"$each": self.domainIds},
                "synonyms": {"$each": self.synonyms},
            },
            "$set": {
                "updated": tnow,
                "type": self.node_type,
                "description": self.description,
                "displayName": self.displayName,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
