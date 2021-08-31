from nedrexdb import __version__


def test_version() -> None:
    assert __version__ == "0.1.0"


def test_node_imports() -> None:
    from nedrexdb.db.models.nodes.disorder import Disorder
    from nedrexdb.db.models.nodes.drug import Drug
    from nedrexdb.db.models.nodes.gene import Gene
    from nedrexdb.db.models.nodes.pathway import Pathway
    from nedrexdb.db.models.nodes.protein import Protein
    from nedrexdb.db.models.nodes.signature import Signature
