import datetime as _datetime
from typing import List as _List

from pydantic import BaseModel as _BaseModel, Field as _Field, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne


class DisorderBase:
    node_type: str = "Disorder"
    collection_name: str = "disorder"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)
        db[cls.collection_name].create_index("domainIds")

    @classmethod
    def find(cls, db, query=None):
        if query is None:
            query = {}
        return db[cls.collection_name].find(query)

    @classmethod
    def find_one(cls, db, query=None):
        if query is None:
            query = {}
        return db[cls.collection_name].find_one(query)


class Disorder(_BaseModel, DisorderBase):
    class Config:
        validate_assignment = True

    primaryDomainId: _StrictStr = ""
    domainIds: _List[str] = _Field(default_factory=list)

    displayName: _StrictStr = ""
    synonyms: _List[str] = _Field(default_factory=list)
    icd10: _List[str] = _Field(default_factory=list)

    description: _StrictStr = ""

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"primaryDomainId": self.primaryDomainId}
        update = {
            "$addToSet": {
                "domainIds": {"$each": self.domainIds},
                "synonyms": {"$each": self.synonyms},
                "icd10": {"$each": self.icd10},
            },
            "$set": {
                "displayName": self.displayName,
                "description": self.description,
                "type": self.node_type,
                "updated": tnow,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
