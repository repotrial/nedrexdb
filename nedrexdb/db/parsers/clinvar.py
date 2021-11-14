import gzip as _gzip
import xml.etree.cElementTree as _et
from collections import defaultdict as _defaultdict
from csv import DictReader as _DictReader
from functools import lru_cache as _lru_cache
from itertools import chain as _chain

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.edges.variant_associated_with_disorder import VariantAssociatedWithDisorder
from nedrexdb.db.models.nodes.disorder import Disorder
from nedrexdb.db.models.nodes.genomic_variant import GenomicVariant
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("clinvar")


def xml_disorder_mapper(id, db):
    if db == "MONDO":
        return f"mondo.{id.replace('MONDO:', '')}"
    elif db == "OMIM":
        return f"omim.{id}"
    elif db == "Orphanet":
        return f"orhanet.{id}"
    elif db == "MeSH":
        return f"mesh.{id}"
    elif db in {"Human Phenotype Ontology", "EFO", "Gene", "MedGen"}:
        return None
    else:
        print(db)


def disorder_domain_id_to_primary_id_map():
    d = _defaultdict(list)
    for doc in Disorder.find(MongoInstance.DB):
        for domain_id in doc["domainIds"]:
            d[domain_id].append(doc["primaryDomainId"])
    return d


def get_variant_list():
    variants = {doc["primaryDomainId"] for doc in GenomicVariant.find(MongoInstance.DB)}
    return variants


@_lru_cache(maxsize=None)
def get_disorder_by_domain_id(domain_id: str):
    query = {"domainIds": domain_id}
    return [doc["primaryDomainId"] for doc in Disorder.find(MongoInstance.DB, query)]


@_lru_cache(maxsize=None)
def get_variant_by_primary_domain_id(pdid: str):
    query = {"primaryDomainId": pdid}
    return GenomicVariant.find_one(MongoInstance.DB, query)


class ClinVarXMLParser:
    def __init__(self, fname):
        self.fname = fname

    def iter_parse(self):
        variant_ids = get_variant_list()
        disorder_domain_id_map = disorder_domain_id_to_primary_id_map()

        assert None not in variant_ids

        with _gzip.open(self.fname, "rt") as f:
            for _, elem in _et.iterparse(f, events=("end",)):
                if elem.tag == "ReferenceClinVarAssertion":
                    pass

                elif elem.tag == "ClinVarSet":
                    ms = elem.find("ReferenceClinVarAssertion").find("MeasureSet")
                    if ms:
                        variant_pdid = f"clinvar.{ms.attrib['ID']}"
                    else:
                        variant_pdid = None

                    if ms and variant_pdid in variant_ids:
                        traits = elem.find("ReferenceClinVarAssertion").find("TraitSet").findall("Trait")
                        traits = _chain(
                            *[
                                [xref.attrib for xref in trait.findall("XRef")]
                                for trait in traits
                                if trait.attrib["Type"] == "Disease"
                            ]
                        )
                        traits = {xml_disorder_mapper(item["ID"], item["DB"]) for item in traits}
                        traits = set(
                            _chain(*[disorder_domain_id_map.get(domain_id, []) for domain_id in traits if domain_id])
                        )
                        traits.discard(None)

                        effects = [
                            effect.strip()
                            for effect in elem.find("ReferenceClinVarAssertion")
                            .find("ClinicalSignificance")
                            .find("Description")
                            .text.split(",")
                        ]
                        review_status = (
                            elem.find("ReferenceClinVarAssertion")
                            .find("ClinicalSignificance")
                            .find("ReviewStatus")
                            .text
                        )
                        acc = elem.find("ReferenceClinVarAssertion").find("ClinVarAccession").attrib["Acc"]

                        for trait in traits:
                            vawd = VariantAssociatedWithDisorder(
                                sourceDomainId=variant_pdid,
                                targetDomainId=trait,
                                accession=acc,
                                effects=effects,
                                reviewStatus=review_status,
                            )

                            yield vawd

                    elem.clear()

                elif elem.tag == "MeasureSet":
                    pass
                elif elem.tag == "ClinVarAccession":
                    pass
                elif elem.tag in {"ClinicalSignificance", "Description", "ReviewStatus"}:
                    pass
                elif elem.tag in {"TraitSet", "Trait", "XRef"}:
                    pass
                else:
                    elem.clear()


class ClinVarVCFParser:
    fieldnames = (
        "CHROM",
        "POS",
        "ID",
        "REF",
        "ALT",
        "QUAL",
        "FILTER",
        "INFO",
    )

    def __init__(self, fname):
        self.fname = fname

    def iter_rows(self):
        with _gzip.open(self.fname, "rt") as f:
            f = (line for line in f if not line.startswith("#"))
            reader = _DictReader(f, fieldnames=self.fieldnames, delimiter="\t")
            for row in reader:
                row["INFO"] = {k: v for k, v in [i.split("=", 1) for i in row["INFO"].split(";")]}
                yield row


class ClinVarRow:
    def __init__(self, row):
        self._row = row

    @property
    def identifier(self):
        return f"clinvar.{self._row['ID']}"

    def get_rs(self):
        if self._row["INFO"].get("RS"):
            return [f"dbsnp.{self._row['INFO']['RS']}"]
        else:
            return []

    @property
    def chromosome(self):
        return self._row["CHROM"]

    @property
    def position(self):
        return int(self._row["POS"])

    @property
    def reference(self):
        return self._row["REF"]

    @property
    def alternative(self):
        return self._row["ALT"]

    @property
    def associated_genes(self):
        return [f"entrez.{entrez_id}" for _, entrez_id in [i.split(":") for i in self._row["INFO"].get("GENEINFO", [])]]

    def parse(self):
        return GenomicVariant(
            primaryDomainId=self.identifier,
            domainIds=[self.identifier] + self.get_rs(),
            chromosome=self.chromosome,
            position=self.position,
            referenceSequence=self.reference,
            alternativeSequence=self.alternative,
        )


def parse():
    fname = get_file_location("human_data")
    parser = ClinVarVCFParser(fname)

    # updates = (ClinVarRow(i).parse().generate_update() for i in parser.iter_rows())
    # for chunk in _tqdm(_chunked(updates, 1_000), desc="Parsing ClinVar genomic variants", leave=False):
    #     MongoInstance.DB[GenomicVariant.collection_name].bulk_write(chunk)

    fname = get_file_location("human_data_xml")
    parser = ClinVarXMLParser(fname)
    updates = (i.generate_update() for i in parser.iter_parse())
    for chunk in _tqdm(
        _chunked(updates, 1_000), desc="Parsing ClinVar genomic variant-disorder relationships", leave=False
    ):
        MongoInstance.DB[VariantAssociatedWithDisorder.collection_name].bulk_write(chunk)
