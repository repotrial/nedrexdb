import gzip as _gzip
from csv import DictReader as _DictReader
from pathlib import Path as _Path

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.edges.protein_interacts_with_protein import ProteinInteractsWithProtein as _PPI
from nedrexdb.db.models.nodes.protein import Protein as _Protein
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("iid")

_DEVELOPMENT_STAGES = {
    "1 cell embryo",
    "2 cell embryo",
    "4 cell embryo",
    "8 cell embryo",
    "morula",
    "blastocyst",
}

_TISSUES = {
    "adipose tissue",
    "adrenal gland",
    "amygdala",
    "bone",
    "bone marrow",
    "brain",
    "dorsal root ganglia",
    "heart",
    "hypothalamus",
    "kidney",
    "liver",
    "lung",
    "lymph nodes",
    "mammary gland",
    "ovary",
    "pancreas",
    "pituitary gland",
    "placenta",
    "prostate",
    "salivary gland",
    "skeletal muscle",
    "small intestine",
    "spleen",
    "stomach",
    "testes",
    "uterus",
}

_JOINT_TISSUES = [
    "synovial macrophages",
    "chondrocytes",
    "growth plate cartilage",
    "synovial membrane",
    "articular cartilage",
]

_BRAIN_TISSUES = [
    "putamen, left",
    "superior frontal gyrus, left, medial bank of gyrus",
    "middle temporal gyrus, left, inferior bank of gyrus",
    "superior frontal gyrus, left, lateral bank of gyrus",
    "middle temporal gyrus, left, superior bank of gyrus",
    "cingulate gyrus, frontal part, left, inferior bank of gyrus",
    "pontine nuclei, left",
    "spinal trigeminal nucleus, left",
    "claustrum, left",
    "inferior temporal gyrus, left, bank of the its",
    "middle frontal gyrus, left, superior bank of gyrus",
    "cingulate gyrus, frontal part, left, superior bank of gyrus",
    "inferior temporal gyrus, left, bank of mts",
    "middle frontal gyrus, left, inferior bank of gyrus",
    "lingual gyrus, left, peristriate",
    "superior temporal gyrus, left, inferior bank of gyrus",
    "superior temporal gyrus, left, lateral bank of gyrus",
    "parahippocampal gyrus, left, lateral bank of gyrus",
    "body of caudate nucleus, left",
    "lingual gyrus, left, striate",
    "subiculum, left",
    "CA1 field, left",
    "inferior temporal gyrus, left, lateral bank of gyrus",
    "superior parietal lobule, left, inferior bank of gyrus",
    "head of caudate nucleus, left",
    "precuneus, left, inferior lateral bank of gyrus",
    "CA3 field, left",
    "dentate gyrus, left",
    "inferior olivary complex, left",
    "medial orbital gyrus, left",
    "postcentral gyrus, left, superior lateral aspect of gyrus",
    "substantia nigra, pars compacta, left",
    "CA4 field, left",
    "fusiform gyrus, left, bank of the its",
    "gyrus rectus, left",
    "lateral group of nuclei, left, ventral division",
    "parahippocampal gyrus, left, bank of the cos",
    "precuneus, left, superior lateral bank of gyrus",
    "striatum",
    "telencephalon",
    "putamen",
    "cerebral nuclei",
    "basal ganglia",
    "cerebral cortex",
    "superior frontal gyrus",
    "frontal lobe",
    "superior frontal gyrus, left",
    "middle temporal gyrus, left",
    "middle temporal gyrus",
    "temporal lobe",
    "cingulate gyrus, frontal part, left",
    "cingulate gyrus, frontal part",
    "cingulate gyrus",
    "limbic lobe",
    "basal part of pons",
    "metencephalon",
    "pontine nuclei",
    "pons",
    "spinal trigeminal nucleus",
    "myelencephalon",
    "claustrum",
    "inferior temporal gyrus, left",
    "inferior temporal gyrus",
    "middle frontal gyrus, left",
    "middle frontal gyrus",
    "occipital lobe",
    "lingual gyrus, left",
    "lingual gyrus",
    "superior temporal gyrus",
    "superior temporal gyrus, left",
    "parahippocampal gyrus, left",
    "parahippocampal gyrus",
    "body of the caudate nucleus",
    "caudate nucleus",
    "hippocampal formation",
    "subiculum",
    "CA1 field",
    "parietal lobe",
    "superior parietal lobule, left",
    "superior parietal lobule",
    "head of the caudate nucleus",
    "precuneus",
    "precuneus, left",
    "CA3 field",
    "dentate gyrus",
    "inferior olivary complex",
    "medial orbital gyrus",
    "postcentral gyrus",
    "postcentral gyrus, left",
    "midbrain tegmentum",
    "substantia nigra",
    "substantia nigra, left",
    "mesencephalon",
    "CA4 field",
    "fusiform gyrus, left",
    "fusiform gyrus",
    "gyrus rectus",
    "dorsal thalamus",
    "thalamus",
    "diencephalon",
    "lateral group of nuclei, ventral division",
    "lateral group of nuclei",
]

_SUBCELLULAR_LOCATIONS = [
    "Golgi apparatus",
    "cytoplasm",
    "cytoskeleton",
    "endoplasmic reticulum",
    "extracellular space",
    "mitochondrion",
    "nuclear matrix",
    "nucleolus",
    "nucleoplasm",
    "nucleus",
    "peroxisome",
    "plasma membrane",
    "vacuole",
]


class IIDRow:
    def __init__(self, row):
        self._row = row

    def get_member_one(self) -> str:
        return f"uniprot.{self._row['uniprot1']}"

    def get_member_two(self) -> str:
        return f"uniprot.{self._row['uniprot2']}"

    def get_methods(self) -> list[str]:
        if self._row["methods"] == "-":
            return []
        return [i.strip() for i in self._row["methods"].split("|")]

    def get_databases(self) -> list[str]:
        return ["iid"]

    # def get_databases(self) -> list[str]:
    #     if self._row["dbs"] == "-":
    #         return []
    #     return [i.strip() for i in self._row["dbs"].split(";")]

    def get_development_stages(self) -> list[str]:
        return [stage.capitalize() for stage in _DEVELOPMENT_STAGES if self._row.get(stage) == "2"]

    def get_tissues(self) -> list[str]:
        return [tissue.capitalize() for tissue in _TISSUES if self._row.get(tissue) == "2"]

    def get_joint_tissues(self) -> list[str]:
        return [joint_tissue.capitalize() for joint_tissue in _JOINT_TISSUES if self._row.get(joint_tissue) == "2"]

    def get_brain_tissues(self) -> list[str]:
        return [brain_tissue.capitalize() for brain_tissue in _BRAIN_TISSUES if self._row.get(brain_tissue) == "2"]

    def get_subcellular_locations(self) -> list[str]:
        return [location.capitalize() for location in _SUBCELLULAR_LOCATIONS if self._row.get(location) == "2"]

    def get_evidence_types(self) -> list[str]:
        return [i.strip() for i in self._row["evidence_type"].split("|")]

    def parse(self) -> _PPI:
        ppi = _PPI(
            memberOne=self.get_member_one(),
            memberTwo=self.get_member_two(),
            methods=self.get_methods(),
            dataSources=self.get_databases(),
            evidenceTypes=self.get_evidence_types(),
            developmentStages=self.get_development_stages(),
            tissues=self.get_tissues(),
            jointTissues=self.get_joint_tissues(),
            brainTissues=self.get_brain_tissues(),
            subcellularLocations=self.get_subcellular_locations(),
        )
        return ppi


class IIDParser:
    def __init__(self, f):
        self.f: _Path = f

        if self.f.name.endswith(".gz") or self.f.name.endswith(".gzip"):
            self.gzipped = True
        else:
            self.gzipped = False

    def parse(self):
        if self.gzipped:
            f = _gzip.open(self.f, "rt")
        else:
            f = self.f.open()

        proteins = {i["primaryDomainId"] for i in _Protein.find(MongoInstance.DB)}

        fieldnames = next(f).strip().split("\t")
        reader = _DictReader(f, delimiter="\t", fieldnames=fieldnames)
        updates = (IIDRow(row).parse() for row in reader)
        updates = (ppi for ppi in updates if ppi.memberOne in proteins and ppi.memberTwo in proteins)
        updates = (ppi.generate_update() for ppi in updates)

        for chunk in _tqdm(_chunked(updates, 1_000), leave=False, desc="Parsing IID"):
            MongoInstance.DB[_PPI.collection_name].bulk_write(chunk)

        f.close()


def parse_ppis():
    filename = get_file_location("human")
    IIDParser(filename).parse()
