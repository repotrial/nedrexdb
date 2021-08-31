from mongoengine import Document as _Document
from mongoengine import ListField as _ListField
from mongoengine import StringField as _StringField


class Drug(_Document):
    meta = {"indexes": ["primaryDomainId", "domainIds"], "allow_inheritance": True}

    primaryDomainId = _StringField(unique=True)
    domainIds = _ListField(_StringField(), default=[])

    # TODO: Decide whether to remove 'primaryDataset' attribute.
    primaryDataset = _StringField()
    # TODO: Decide standard attribute for recording datasets.
    allDatasets = _ListField(_StringField(), default=[])

    displayName = _StringField()
    synonyms = _ListField(_StringField(), default=[])
    description = _StringField(default="")

    drugCategories = _ListField(_StringField(), default=[])
    drugGroups = _ListField(_StringField(), default=[])
    casNumber = _StringField(default="")
    indication = _StringField(default="")


class BiotechDrug(Drug):
    sequences = _ListField(_StringField(), default=[])
    type = _StringField(default="BiotechDrug")


class SmallMoleculeDrug(Drug):
    iupacName = _StringField()
    smiles = _StringField()
    inchi = _StringField()
    molecularFormula = _StringField()
    type = _StringField(default="SmallMoleculeDrug")
