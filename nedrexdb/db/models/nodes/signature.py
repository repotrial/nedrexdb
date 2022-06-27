from mongoengine import Document as _Document
from mongoengine import StringField as _StringField, ListField as _ListField


class Signature(_Document):
    meta = {"indexes": ["primaryDomainId"]}

    # TODO: Decide whether to add a domainIds field (similar to other nodes)
    primaryDomainId = _StringField(unique=True)
    domainIds = _ListField(_StringField())
    description = _StringField()
    type = _StringField(default="Signature")
