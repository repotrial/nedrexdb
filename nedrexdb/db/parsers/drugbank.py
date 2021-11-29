import xml.etree.cElementTree as _et
from collections import OrderedDict as _OrderedDict
from csv import DictReader as _DictReader
from itertools import chain
from multiprocessing import Pool as _Pool
from typing import Optional as _Optional
from uuid import uuid4 as _uuid4
from zipfile import ZipFile as _ZipFile

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm
from xmljson import badgerfish as _bf

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.drug import Drug, BiotechDrug, SmallMoleculeDrug
from nedrexdb.db.models.nodes.protein import Protein
from nedrexdb.db.models.edges.drug_has_target import DrugHasTarget
from nedrexdb.exceptions import AssumptionError as _AssumptionError

get_file_location = _get_file_location_factory("drugbank")


def _recursive_yield(elem):
    if isinstance(elem, _OrderedDict):
        yield elem
    else:
        for i in elem:
            yield from _recursive_yield(i)


class DrugBankDrugTarget:
    def __init__(self, entry):
        self._entry = entry

    def iter_targets(self):
        targets = self._entry.get(ns("targets"))
        if not targets:
            return []

        targets = targets.get(ns("target"))
        if not targets:
            return []

        for target in _recursive_yield(targets):
            actions = target.get(ns("actions"))
            if actions:
                actions = [i["$"] for i in _recursive_yield(actions[ns("action")])]
            else:
                actions = []

            polypeptide = target.get(ns("polypeptide"))
            if not polypeptide:
                continue

            for item in _recursive_yield(polypeptide):
                if not item["@source"] in {"TrEMBL", "Swiss-Prot"}:
                    continue

                yield (f"uniprot.{item['@id']}", actions)

    def get_drug(self):
        return DrugBankEntry(self._entry).get_primary_domain_id()

    def parse(self) -> list[DrugHasTarget]:
        drug = self.get_drug()

        updates = [
            DrugHasTarget(
                sourceDomainId=drug,
                targetDomainId=protein,
                actions=actions,
                databases=["DrugBank"],
            )
            for protein, actions in self.iter_targets()
        ]
        return updates


class DrugBankEntry:
    def __init__(self, entry):
        self._entry = entry
        self.__calculated_properties = None

    # NOTE: Obtaining calculated-properties is used multiple times for small
    #       molcule properties. The _calculated_properties method uses
    #       __calculated_properties to effectively cache the result the first
    #       time around.
    @property
    def _calculated_properties(self):
        if self.__calculated_properties is not None:
            return self.__calculated_properties

        properties = self._entry[ns("calculated-properties")]
        if ns("property") not in properties.keys():
            self.__calculated_properties = {}
            return self.__calculated_properties

        properties = properties[ns("property")]
        if isinstance(properties, _OrderedDict):
            properties = [properties]
        self.__calculated_properties = {prop[ns("kind")]["$"]: prop[ns("value")]["$"] for prop in properties}
        return self.__calculated_properties

    def get_drug_type(self) -> str:
        drug_type = self._entry["@type"]

        if drug_type == "biotech":
            return "BiotechDrug"
        elif drug_type == "small molecule":
            return "SmallMoleculeDrug"
        else:
            raise _AssumptionError("encountered unexpected DrugBank drug type")

    def get_primary_domain_id(self) -> str:
        drug_ids = self._entry[ns("drugbank-id")]
        if isinstance(drug_ids, _OrderedDict):
            drug_ids = [drug_ids]

        primary_ids = [drug_id for drug_id in drug_ids if "@primary" in drug_id]
        if len(primary_ids) != 1:
            raise _AssumptionError("expected only one primary ID for DrugBank drug")

        return f"drugbank.{primary_ids.pop()['$']}"

    def get_domain_ids(self) -> list[str]:
        drug_ids = self._entry[ns("drugbank-id")]
        if isinstance(drug_ids, _OrderedDict):
            drug_ids = [drug_ids]

        drug_ids = [f"drugbank.{drug_id['$']}" for drug_id in drug_ids]
        return drug_ids

    def get_display_name(self) -> str:
        return self._entry[ns("name")]["$"]

    def get_indications(self) -> str:
        indications = self._entry[ns("indication")]
        if "$" in indications.keys():
            return indications["$"]
        else:
            return ""

    def get_cas_number(self) -> str:
        cas_number = self._entry[ns("cas-number")]
        if "$" in cas_number:
            return cas_number["$"]
        else:
            return ""

    def get_description(self) -> str:
        description = self._entry[ns("description")]
        if "$" in description:
            return description["$"]
        else:
            return ""

    def get_synonyms(self) -> list[str]:
        synonyms: list[str] = []

        syns = self._entry[ns("synonyms")]
        if not ns("synonym") in syns.keys():
            return synonyms

        syns = syns[ns("synonym")]
        if isinstance(syns, _OrderedDict):
            syns = [syns]
        for synonym in syns:
            synonyms.append(synonym["$"])

        return synonyms

    def get_drug_categories(self) -> list[str]:
        categories: list[str] = []

        cats = self._entry[ns("categories")]
        if not ns("category") in cats:
            return categories

        cats = cats[ns("category")]
        if isinstance(cats, _OrderedDict):
            cats = [cats]
        for cat in cats:
            categories.append(cat[ns("category")]["$"])

        return categories

    def get_drug_groups(self) -> list[str]:
        groups: list[str] = []

        if ns("groups") not in self._entry.keys():
            return groups

        grps = self._entry[ns("groups")][ns("group")]
        if isinstance(grps, _OrderedDict):
            grps = [grps]
        for grp in grps:
            groups.append(grp["$"])

        return groups

    def get_smiles(self) -> _Optional[str]:
        return self._calculated_properties.get("SMILES")

    def get_inchi(self) -> _Optional[str]:
        return self._calculated_properties.get("InChI")

    def get_iupac(self) -> _Optional[str]:
        return self._calculated_properties.get("IUPAC Name")

    def get_molecular_formula(self) -> _Optional[str]:
        return self._calculated_properties.get("Molecular Formula")

    def get_sequences(self) -> list[str]:
        sequences: list[str] = []

        seqs = self._entry[ns("sequences")]
        if not ns("sequence") in seqs.keys():
            return sequences

        seqs = seqs[ns("sequence")]
        if isinstance(seqs, _OrderedDict):
            seqs = [seqs]

        seqs = [seq for seq in seqs if seq["@format"] == "FASTA"]
        seqs = [seq["$"].split("\n", 1) for seq in seqs]
        seqs = [(name[1:], seq.replace("\n", "")) for name, seq in seqs]
        sequences = [">{} {}\n{}".format(_uuid4(), name, seq) for name, seq in seqs]
        return sequences

    def parse(self):
        drug_type = self.get_drug_type()
        if drug_type == "BiotechDrug":
            d = BiotechDrug()
            d.sequence = self.get_sequences()
        elif drug_type == "SmallMoleculeDrug":
            d = SmallMoleculeDrug()
            d.inchi = self.get_inchi()
            d.smiles = self.get_smiles()
            d.iupacName = self.get_iupac()
            d.molecularFormula = self.get_molecular_formula()

        d.primaryDomainId = self.get_primary_domain_id()
        d.domainIds = self.get_domain_ids()

        d.primaryDataset = "DrugBank"
        d.allDatasets.append("DrugBank")
        d.description = self.get_description()
        d.indication = self.get_indications()

        d.displayName = self.get_display_name()
        d.synonyms = self.get_synonyms()
        d.drugCategories = self.get_drug_categories()
        d.drugGroups = self.get_drug_groups()
        d.casNumber = self.get_cas_number()

        return d


# ns = namespace
def ns(string) -> str:
    return "{http://www.drugbank.ca}" + string


def _entry_to_update(entry):
    # NOTE: This function parses both drugs and drug targets, which means that
    #       there is no need to iterate over the file a second time.
    entry_as_json = _bf.data(entry)[ns("drug")]
    db = DrugBankEntry(entry_as_json).parse().generate_update()
    dht = [i.generate_update() for i in DrugBankDrugTarget(entry_as_json).parse()]
    return db, dht


def parse_drugbank_open():
    filename = get_file_location("open")
    zf = _ZipFile(filename)
    with zf.open("drugbank vocabulary.csv") as f:
        f = (i.decode("utf-8") for i in f)
        reader = _DictReader(f)
        for row in reader:
            drug = Drug()
            drug.primaryDomainId = f"drugbank.{row['DrugBank ID']}"
            drug.domainIds = [f"drugbank.{row['DrugBank ID']}"]
            drug.displayName = row["Common name"]
            drug.casNumber = row["CAS"]
            if drug.primaryDomainId == "drugbank.DB15661":
                print(drug)
            yield drug


def parse_drugbank():
    updates = (drug.generate_update() for drug in parse_drugbank_open())
    for chunk in _chunked(updates, 1_000):
        MongoInstance.DB[Drug.collection_name].bulk_write(chunk)


def _parse_drugbank():
    filename = get_file_location("all")

    def db_iter():
        with open(filename, "r") as handle:
            depth = 0

            # Iterate through and yield drugs.
            for event, elem in _et.iterparse(handle, events=["start", "end"]):
                if elem.tag != ns("drug"):
                    continue

                if event == "start":
                    depth += 1
                elif event == "end":
                    depth -= 1

                if event == "end" and depth == 0:
                    yield elem

    proteins = {protein["primaryDomainId"] for protein in Protein.find(MongoInstance.DB)}

    with _Pool(2) as pool:
        updates = pool.imap_unordered(_entry_to_update, db_iter(), chunksize=10)
        for chunk in _tqdm(_chunked(updates, 100), leave=False, desc="Parsing DrugBank"):
            chunk = [item for item in chunk if item]
            drugs, drug_targets = zip(*chunk)

            drugs = list(drugs)
            MongoInstance.DB[Drug.collection_name].bulk_write(drugs)

            drug_targets = list(chain(*drug_targets))
            drug_targets = [update for update in drug_targets if update._filter["targetDomainId"] in proteins]

            # NOTE: In testing, it was possible to get an empty list for drug
            #       targets, which causes the bulk_write method to fail.
            if drug_targets:
                MongoInstance.DB[DrugHasTarget.collection_name].bulk_write(drug_targets)
