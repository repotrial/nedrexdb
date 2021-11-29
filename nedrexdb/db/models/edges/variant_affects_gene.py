import datetime as _datetime

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class VariantAffectsGeneBase(models.MongoMixin):
    edge_type = "VariantAffectsGene"
    collection_name = "variant_affects_gene"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index("sourceDomainId")


class VariantAffectsGene(_BaseModel, VariantAffectsGeneBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()
        query = {"sourceDomainId": self.sourceDomainId, "targetDomainId": self.targetDomainId}

        update = {
            "$set": {
                "updated": tnow,
                "type": self.edge_type,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
