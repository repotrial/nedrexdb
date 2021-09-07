import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class ProteinInteractsWithProteinBase(models.MongoMixin):
    edge_type: str = "ProteinInteractsWithProtein"
    collection_name: str = "protein_interacts_with_protein"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("memberOne")
        db[cls.collection_name].create_index("memberTwo")
        db[cls.collection_name].create_index([("memberOne", 1), ("memberTwo", 1)], unique=True)


class ProteinInteractsWithProtein(_BaseModel, ProteinInteractsWithProteinBase):
    class Config:
        validate_assignment = True

    memberOne: _StrictStr = ""
    memberTwo: _StrictStr = ""
    evidenceTypes: list[str] = []
    methods: list[str] = []
    databases: list[str] = []

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        m1, m2 = sorted([self.memberOne, self.memberTwo])

        query = {"memberOne": m1, "memberTwo": m2}

        update = {
            "$setOnInsert": {
                "created": tnow,
            },
            "$set": {
                "updated": tnow,
                "type": self.edge_type,
            },
            "$addToSet": {
                "methods": {"$each": self.methods},
                "databases": {"$each": self.databases},
                "evidenceTypes": {"$each": self.evidenceTypes},
            },
        }

        return _UpdateOne(query, update, upsert=True)
