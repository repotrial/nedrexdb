import csv

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.disorder import Disorder

get_file_location = _get_file_location_factory("repotrial")


def parse():
    fname = get_file_location("mappings")

    dcoll = MongoInstance.DB[Disorder.collection_name]

    with open(fname) as f:
        reader = csv.reader(f, delimiter="\t")
        for omim, icd10 in reader:
            dcoll.update(
                {"domainIds": omim},
                {
                    "$addToSet": {
                        "icd10": {"$each": icd10.split("|")},
                        "dataSources": "repotrial",
                    }
                },
            )
