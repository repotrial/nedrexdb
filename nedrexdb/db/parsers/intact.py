from csv import DictReader as _DictReader
from itertools import product as _product
from zipfile import ZipFile as _ZipFile

from more_itertools import chunked as _chunked
from tqdm import tqdm as _tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.models.nodes.protein import Protein
from nedrexdb.db.models.edges.protein_interacts_with_protein import ProteinInteractsWithProtein
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("intact")


def get_interactors(row, interactor):
    if interactor in {"A", "B"}:
        main_id_key = f"ID(s) interactor {interactor}"
        alt_id_key = f"Alt. ID(s) interactor {interactor}"
    else:
        raise Exception(f"Invalid interactor {interactor=!r}")

    interactors = set()
    interactors.add(row[main_id_key])
    interactors.update(row[alt_id_key].split("|"))
    interactors.discard("-")

    interactors = [pro.replace("uniprotkb:", "uniprot.") for pro in interactors if pro.startswith("uniprotkb:")]

    return interactors


class IntActRow:
    def __init__(self, row):
        self._row = row

    def parse(self):
        a_interactors = get_interactors(self._row, "A")
        b_interactors = get_interactors(self._row, "B")

        for a, b in _product(a_interactors, b_interactors):
            a, b = sorted([a, b])

            yield ProteinInteractsWithProtein(memberOne=a, memberTwo=b, dataSources=["intact"])


def parse_ppis():
    zf = _ZipFile(get_file_location("psimitab"))
    with zf.open("intact.txt", "r") as f:
        f = (line.decode("utf-8") for line in f)
        fieldnames = next(f)[1:-1].split("\t")
        reader = _DictReader(f, fieldnames=fieldnames, delimiter="\t")
        reader = (row for row in reader if row["Taxid interactor A"] == "taxid:9606(human)|taxid:9606(Homo sapiens)")
        reader = (row for row in reader if row["Taxid interactor B"] == "taxid:9606(human)|taxid:9606(Homo sapiens)")
        for row in reader:
            yield from IntActRow(row).parse()


def parse():
    proteins = {i["primaryDomainId"] for i in Protein.find(MongoInstance.DB)}
    updates = (ppi.generate_update() for ppi in parse_ppis() if ppi.memberOne in proteins and ppi.memberTwo in proteins)

    for chunk in _tqdm(_chunked(updates, 1_000), leave=False, desc="Parsing PPIs from IntAct"):
        MongoInstance.DB[ProteinInteractsWithProtein.collection_name].bulk_write(chunk)
