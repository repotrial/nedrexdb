import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr, Field as _Field
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class DrugHasContraindicationBase(models.MongoMixin):
    edge_type: str = "DrugHasContraindication"
    collection_name: str = "drug_has_contraindication"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)], unique=True)


class DrugHasContraindication(_BaseModel, DrugHasContraindicationBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""
    dataSources: list[str] = _Field(default_factory=list)

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {
            "sourceDomainId": self.sourceDomainId,
            "targetDomainId": self.targetDomainId,
        }

        update = {
            "$set": {
                "updated": tnow,
                "type": self.edge_type,
            },
            "$setOnInsert": {
                "created": tnow,
            },
            "$addToSet": {"dataSources": {"$each": self.dataSources}},
        }

        return _UpdateOne(query, update, upsert=True)
