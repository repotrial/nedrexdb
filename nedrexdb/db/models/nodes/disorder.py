from mongoengine import Document as _Document
from mongoengine import ListField as _ListField
from mongoengine import StringField as _StringField


class Disorder(_Document):
    meta = {"indexes": ["primaryDomainId", "domainIds"]}
    primaryDomainId = _StringField(required=True)
    domainIds = _ListField(_StringField(), default=[])

    displayName = _StringField()
    synonyms = _ListField(_StringField(), default=[])
    icd10 = _ListField(_StringField(), default=[])

    description = _StringField(default="")
    type = _StringField(default="Disorder")
