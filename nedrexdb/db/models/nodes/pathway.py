from mongoengine import Document as _Document
from mongoengine import StringField as _StringField
from mongoengine import ListField as _ListField


class Pathway(_Document):
    primaryDomainId = _StringField(required=True)
    domainIds = _ListField(_StringField(), default=[])
    displayName = _StringField()
    species = _StringField()

    type = _StringField(default="Pathway")
