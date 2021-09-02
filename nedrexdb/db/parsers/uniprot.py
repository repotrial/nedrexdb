import gzip as _gzip
import re as _re
import itertools as _itertools

from Bio import SeqIO as _SeqIO, SeqRecord as _SeqRecord
from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.protein import Protein

get_file_location = _get_file_location_factory("uniprot")


class UniProtRecord:
    _CURLY_REGEX = _re.compile(r"{|}")
    _DESCRIPTION_CUTOFF_STRINGS = ["Contains:", "Includes:", "Flags:"]
    _CATEGORY_FIELDS = ["RecName:", "AltName:", "SubName:", ""]
    _SUBCATEGORY_FIELDS = [
        "Full=",
        "Short=",
        "EC=",
        "Allergen=",
        "Biotech=",
        "CD_antigen=",
        "INN=",
    ]
    _COMBINED_FIELDS = sorted(
        (" ".join(i) for i in _itertools.product(_CATEGORY_FIELDS, _SUBCATEGORY_FIELDS)),
        key=lambda i: len(i),
        reverse=True,
    )
    _COMBINATION_REGEX = _re.compile(r"|".join(_COMBINED_FIELDS))

    def __init__(self, record: _SeqRecord.SeqRecord):
        self._record = record

    def get_primary_id(self) -> str:
        return f"uniprot.{self._record.id}"

    def get_sequence(self) -> str:
        return str(self._record.seq)

    def get_display_name(self) -> str:
        return self._record.name

    def get_taxid(self) -> int:
        taxid = self._record.annotations["ncbi_taxid"][0]
        return int(taxid)

    def get_synonyms(self) -> list[str]:
        synonyms = self._record.description.split()
        cutoff = next(
            (val for val, item in enumerate(synonyms) if item in self._DESCRIPTION_CUTOFF_STRINGS),
            999_999,
        )
        synonyms = " ".join(synonyms[:cutoff])
        synonyms = self._COMBINATION_REGEX.split(synonyms)
        synonyms = [i.strip() for i in synonyms]
        synonyms = [i[:-1] if i.endswith(";") else i for i in synonyms]
        synonyms = [i for i in synonyms if i]
        return synonyms

    def get_gene_name(self) -> str:
        gene_name = self._record.annotations.get("gene_name", "")
        if not gene_name:
            pass
        else:
            if gene_name.startswith("Name="):
                gene_name = gene_name.replace("Name=", "").split(";", 1)[0]
                gene_name = self._CURLY_REGEX.split(gene_name)[0].strip()

        return gene_name

    def get_comments(self) -> str:
        return self._record.annotations.get("comment", "")

    def parse(self):
        p = Protein()

        p.primaryDomainId = self.get_primary_id()
        p.domainIds.append(p.primaryDomainId)

        p.displayName = self.get_display_name()
        p.synonyms = self.get_synonyms()
        p.comments = self.get_comments()
        p.geneName = self.get_gene_name()

        p.taxid = self.get_taxid()
        p.sequence = self.get_sequence()

        return p


def _iter_gzipped_swiss(fname):
    with _gzip.open(fname, "rt") as f:
        for record in _SeqIO.parse(f, "swiss"):
            yield record


def parse_proteins():
    filenames = [get_file_location("trembl"), get_file_location("swissprot")]
    uniprot_records = _itertools.chain(*[_iter_gzipped_swiss(filename) for filename in filenames])
    updates = (UniProtRecord(record).parse().generate_update() for record in uniprot_records)

    for chunk in _tqdm(_chunked(updates, 1_000), desc="Parsing Swiss-Prot and TrEMBL", leave=False):
        MongoInstance.DB[Protein.collection_name].bulk_write(chunk)
