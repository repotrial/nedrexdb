import csv
import gzip

from pymongo import UpdateMany
from tqdm import tqdm

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory

get_file_location = _get_file_location_factory("unichem")


def parse():
    fname = get_file_location("pubchem_drugbank_map")
    updates = []

    with gzip.open(fname, "rt") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)  # Skip the header row

        for db, pc in tqdm(reader, leave=False):
            update = UpdateMany(
                {"domainIds": f"drugbank.{db}"}, {"$addToSet": {"domainIds": f"pubchem.{pc}"}}, upsert=False
            )

            updates.append(update)

    coll = MongoInstance.DB["drug"]
    coll.bulk_write(updates)
