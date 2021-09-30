from dataclasses import dataclass as _dataclass

from pymongo import MongoClient as _MongoClient

from nedrexdb import config as _config
from nedrexdb.db.models.nodes import (
    disorder as _disorder,
    drug as _drug,
    gene as _gene,
    pathway as _pathway,
    protein as _protein,
    go as _go,
)
from nedrexdb.db.models.edges import (
    disorder_is_subtype_of_disorder as _disorder_is_subtype_of_disorder,
    drug_has_contraindication as _drug_has_contraindication,
    drug_has_indication as _drug_has_indication,
    drug_has_target as _drug_has_target,
    gene_associated_with_disorder as _gene_associated_with_disorder,
    protein_encoded_by_gene as _protein_encoded_by_gene,
    protein_in_pathway as _protein_in_pathway,
    protein_interacts_with_protein as _protein_interacts_with_protein,
    go_is_subtype_of_go as _go_is_subtype_of_go,
    protein_has_go_annotation as _protein_has_go_annotation,
)


@_dataclass
class MongoInstance:
    CLIENT = None
    DB = None

    @classmethod
    def connect(cls, version):
        if version not in ("live", "dev"):
            raise ValueError(f"version given ({version!r}) should be 'live' or 'dev")

        port = _config[f"db.{version}.mongo_port"]
        host = "localhost"
        dbname = _config["db.mongo_db"]

        cls.CLIENT = _MongoClient(host=host, port=port)
        cls.DB = cls.CLIENT[dbname]

    @classmethod
    def set_indexes(cls):
        if not cls.DB:
            raise ValueError("run nedrexdb.db.connect() first to connect to MongoDB")
        # Nodes
        _disorder.Disorder.set_indexes(cls.DB)
        _drug.Drug.set_indexes(cls.DB)
        _gene.Gene.set_indexes(cls.DB)
        _pathway.Pathway.set_indexes(cls.DB)
        _protein.Protein.set_indexes(cls.DB)
        _go.GO.set_indexes(cls.DB)
        # Edges
        _disorder_is_subtype_of_disorder.DisorderIsSubtypeOfDisorder.set_indexes(cls.DB)
        _drug_has_contraindication.DrugHasContraindication.set_indexes(cls.DB)
        _drug_has_indication.DrugHasIndication.set_indexes(cls.DB)
        _drug_has_target.DrugHasTarget.set_indexes(cls.DB)
        _gene_associated_with_disorder.GeneAssociatedWithDisorder.set_indexes(cls.DB)
        _protein_encoded_by_gene.ProteinEncodedByGene.set_indexes(cls.DB)
        _protein_in_pathway.ProteinInPathway.set_indexes(cls.DB)
        _protein_interacts_with_protein.ProteinInteractsWithProtein.set_indexes(cls.DB),
        _go_is_subtype_of_go.GOIsSubtypeOfGOBase.set_indexes(cls.DB)
        _protein_has_go_annotation.ProteinHasGOAnnotation.set_indexes(cls.DB)
