from mongoengine import Document as _Document
from mongoengine import StringField as _StringField


class Signature(_Document):
    meta = {"indexes": ["primaryDomainId"]}

    primaryDomainId = _StringField(unique=True)
    description = _StringField()
    type = _StringField(default="Signature")
