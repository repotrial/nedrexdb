from mongoengine import Document as _Document
from mongoengine import IntField as _IntField
from mongoengine import ListField as _ListField
from mongoengine import StringField as _StringField


class Protein(_Document):
    meta = {"indexes": ["primaryDomainId", "domainIds", "taxid"]}

    primaryDomainId = _StringField(unique=True)
    domainIds = _ListField(_StringField(), default=[])

    # TODO: Review which fields should have a default empty value / be nullable
    displayName = _StringField(default="")
    synonyms = _ListField(_StringField(), default=[])
    comments = _StringField(default="")
    geneName = _StringField(default="")

    taxid = _IntField(default=-1)
    sequence = _StringField(default="")

    type = _StringField(default="Protein")
