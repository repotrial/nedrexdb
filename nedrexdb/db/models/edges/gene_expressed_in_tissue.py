import datetime as _datetime
from typing import Optional as _Optional

from pydantic import BaseModel as _BaseModel, StrictStr as _StrictStr
from pymongo import UpdateOne as _UpdateOne

from nedrexdb.db import models


class GeneExpressedInTissueBase(models.MongoMixin):
    edge_type: str = "GeneExpressedInTissue"
    collection_name: str = "gene_expressed_in_tissue"

    @classmethod
    def set_indexes(cls, db):
        db[cls.collection_name].create_index("sourceDomainId")
        db[cls.collection_name].create_index("targetDomainId")
        db[cls.collection_name].create_index([("sourceDomainId", 1), ("targetDomainId", 1)], unique=True)


class GeneExpressedInTissue(_BaseModel, GeneExpressedInTissueBase):
    class Config:
        validate_assignment = True

    sourceDomainId: _StrictStr = ""
    targetDomainId: _StrictStr = ""
    TPM: _Optional[float] = None
    nTPM: _Optional[float] = None
    pTPM: _Optional[float] = None

    def query_update(self):
        tnow = _datetime.datetime.utcnow()

        query = {
            "sourceDomainId": self.sourceDomainId,
            "targetDomainId": self.targetDomainId,
        }

        update = {
            "$set": {
                "updated": tnow,
                "type": self.edge_type,
                "TPM": self.TPM,
                "nTPM": self.nTPM,
                "pTPM": self.pTPM,
            },
            "$setOnInsert": {"created": tnow},
        }

        return _UpdateOne(query, update, upsert=True)
