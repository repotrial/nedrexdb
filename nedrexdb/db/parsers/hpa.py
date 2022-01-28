import gzip
import xml.etree.cElementTree as et
from itertools import chain

from tqdm import tqdm
from more_itertools import chunked

from nedrexdb.db import MongoInstance
from nedrexdb.db.parsers import _get_file_location_factory
from nedrexdb.db.models.nodes.tissue import Tissue
from nedrexdb.db.models.nodes.gene import Gene
from nedrexdb.db.models.nodes.protein import Protein
from nedrexdb.db.models.edges.gene_expressed_in_tissue import GeneExpressedInTissue
from nedrexdb.db.models.edges.protein_expressed_in_tissue import ProteinExpressedInTissue

get_file_location = _get_file_location_factory("hpa")


class HPAEntry:
    def __init__(self, entry):
        self.__entry = entry
        self.__proteins = None
        self.__genes = None
        self.__rna_expression = None
        self.__protein_expression = None

    @property
    def proteins(self):
        if self.__proteins is None:
            self.__proteins = [
                f"uniprot.{i.get('id')}"
                for i in self.__entry.find("identifier").findall("xref")
                if i.get("db") == "Uniprot/SWISSPROT"
            ]
        return self.__proteins

    @property
    def genes(self):
        if self.__genes is None:
            self.__genes = [
                f"entrez.{i.get('id')}"
                for i in self.__entry.find("identifier").findall("xref")
                if i.get("db") == "NCBI GeneID"
            ]
        return self.__genes

    @property
    def rna_expression(self):
        if self.__rna_expression is not None:
            return self.__rna_expression

        rna_expression_elem = self.__entry.find("rnaExpression")
        expression = []

        for item in rna_expression_elem.findall("data"):
            tissue = item.find("tissue")
            tissue_obj = get_tissue(tissue)
            if tissue_obj is None:
                continue

            data = {"tissue": tissue_obj}

            for key, xpath in [
                ("nTPM", "./level[@type='normalizedRNAExpression']"),
                ("pTPM", "./level[@type='proteinCodingRNAExpression']"),
                ("TPM", "./level[@type='RNAExpression']"),
            ]:
                expr = item.find(xpath)
                if expr is not None:
                    data[key] = float(expr.get("expRNA"))

            expression.append(data)

        self.__rna_expression = expression
        return self.__rna_expression

    @property
    def protein_expression(self):
        if self.__protein_expression is not None:
            return self.__protein_expression
        tissue_expression_elem = self.__entry.find("tissueExpression")

        if tissue_expression_elem is None:
            self.__protein_expression = []
            return self.__protein_expression

        expression = []

        for item in tissue_expression_elem.findall("data"):
            tissue = item.find("tissue")
            tissue_obj = get_tissue(tissue)
            if tissue_obj is None:
                continue

            level = item.find("level").text
            expression.append({"tissue": tissue_obj, "level": level})

        self.__protein_expression = expression
        return self.__protein_expression


def get_tissue(tissue):
    tissue_ont = tissue.get("ontologyTerms")

    if not tissue_ont:
        return None
    uberon_ids = [
        f"uberon.{identifier.split(':')[1]}" for identifier in tissue_ont.split(",") if identifier.startswith("UBERON")
    ]

    return uberon_ids


def iter_entries():
    fname = get_file_location("all")
    with gzip.open(fname, "rt") as f:
        xml_parser = et.iterparse(f, events=("end",))

        for _, elem in xml_parser:
            if elem.tag != "entry":
                continue

            entry = HPAEntry(elem)
            gene_expression = []
            protein_expression = []

            for gene in entry.genes:
                for rna_expr in entry.rna_expression:
                    gene_expression += [
                        GeneExpressedInTissue(
                            sourceDomainId=gene,
                            targetDomainId=tissue,
                            TPM=rna_expr.get("TPM"),
                            nTPM=rna_expr.get("nTPM"),
                            pTPM=rna_expr.get("pTPM"),
                        )
                        for tissue in rna_expr["tissue"]
                    ]

            for protein in entry.proteins:
                for pro_expr in entry.protein_expression:
                    protein_expression += [
                        ProteinExpressedInTissue(sourceDomainId=protein, targetDomainId=tissue, level=pro_expr["level"])
                        for tissue in pro_expr["tissue"]
                    ]

            yield gene_expression, protein_expression

            elem.clear()


def parse_hpa():
    tissues = {i["primaryDomainId"] for i in Tissue.find(MongoInstance.DB)}
    genes = {i["primaryDomainId"] for i in Gene.find(MongoInstance.DB)}
    proteins = {i["primaryDomainId"] for i in Protein.find(MongoInstance.DB)}

    for chunk in tqdm(chunked(iter_entries(), 10), leave=False):
        gene_expression, protein_expression = zip(*chunk)

        gene_expression = [
            item.generate_update()
            for item in chain(*gene_expression)
            if item.sourceDomainId in genes and item.targetDomainId in tissues
        ]

        MongoInstance.DB[GeneExpressedInTissue.collection_name].bulk_write(gene_expression)

        protein_expression = [
            item.generate_update()
            for item in chain(*protein_expression)
            if item.sourceDomainId in proteins and item.targetDomainId in tissues
        ]
        MongoInstance.DB[ProteinExpressedInTissue.collection_name].bulk_write(protein_expression)
