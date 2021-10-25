import datetime as _datetime
from typing import List as _List

from pydantic import BaseModel as _BaseModel, Field as _Field, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class GenomicVariantBase(models.MongoMixin):
    node_type: str = "GenomicVariant"
    collection_name: str = "genomic_variant"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("primaryDomainId", unique=True)


class GenomicVariant(_BaseModel, GenomicVariantBase):
    class Config:
        validate_assignment = True

    primaryDomainId: _StrictStr = ""
    domainIds: _List[str] = _Field(default_factory=list)

    chromosome: str = ""
    position: int = -1

    clinicalSignificance: _List[str] = []
    referenceSequence: str = ""
    alternativeSequence: str = ""

    def generate_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {"primaryDomainId": self.primaryDomainId}
        update = {
            "$addToSet": {"domainIds": {"$each": self.domainIds}},
            "$set": {
                "updated": tnow,
                "chromosome": self.chromosome,
                "position": self.position,
                "clinicalSignificance": self.clinicalSignificance,
                "referenceSequence": self.referenceSequence,
                "alternativeSequence": self.alternativeSequence,
            },
            "$setOnInsert": {
                "created": tnow,
                "type": self.node_type,
            },
        }

        return _UpdateOne(query, update, upsert=True)
