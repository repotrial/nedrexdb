from mongoengine import Document as _Document
from mongoengine import ListField as _ListField
from mongoengine import StringField as _StringField


class Gene(_Document):
    meta = {"indexes": ["primaryDomainId", "domainIds", "approvedSymbol", "symbols"]}

    primaryDomainId = _StringField(unique=True)
    domainIds = _ListField(_StringField(), default=[])

    displayName = _StringField()
    synonyms = _ListField(_StringField(), default=[])
    approvedSymbol = _StringField()
    symbols = _ListField(_StringField(), default=[])
    description = _StringField()

    chromosome = _StringField()
    mapLocation = _StringField()
    geneType = _StringField(default=None)

    type = _StringField(default="Gene")
