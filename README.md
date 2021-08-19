# NeDRexDB
This repository will contain the code related to building and running NeDRexDB, originally based on the [repotrial/repodb_v2](https://github.com/repotrial/repodb_v2) repository.

Unlike the `repotrial/repodb_v2` repository, this code contains *only* the code related to building and running the NeDRex database. 

## Changes in v2.0.0
Note that the first release from this repository starts at v2.0.0, reflecting the fact that a first version of NeDRexDB exists (from the `repodb_v2` repository). 

### General
- [ ] All code has been refactored and commented to reduce coupling, reduce redundancy, and improve cohesion. This should improve maintainability and readability.
- [ ] Python code has been updated to use Python 3.9 features.

### Changes in node and edge types
- [ ] A new node type, `Phenotype`, has been added to NeDRexDB.
- [ ] A new node type, `GenomicVariant`, has been added to NeDRexDB.
- [ ] A new edge type, `DisorderHasPhenotype`, has been added to NeDRexDB.
- [ ] A new edge type, `VariantAssociatedWithDisorder`, has been added to NeDRexDB.
- [ ] A new edge type, `VariantAffectsGene`, has been added to NeDRexDB.

### Changes in databases
- [ ] Data from BioGRID is now parsed and integrated into NeDRexDB, adding `ProteinInteractsWithProtein` edges.
- [ ] Data from IntAct is now parsed and integrated into NeDRexDB, adding `ProteinInteractsWithProtein` edges.
- [ ] Data from the Comparative Toxicogenomics Database (CTD) is now parsed and integrated into NeDRexDB, adding `DrugHasIndication` edges.
- [ ] Data from the Human Phenotype Ontology (HPO) is now parsed and integrated into NeDRexDB, adding `Phenotype` nodes and `DisorderHasPhenotype` edges.
- [ ] Data from ClinVar is now parsed and integrated into NeDRexDB, adding `GenomicVariant` nodes, `VariantAssociatedWithDisorder` edges, and `VariantAffectsGene` edges.
- [ ] Data from dbSNP is now parsed and integrated into NeDRex, adding prevalence data to some `GenomicVariant` nodes.

### Changes in existing parsers and integrations
- [ ] MONDO integration now includes a check for obsolete disorders and no longer include these nodes.
- [ ] Drug Central integration now works on the SQL dump download (rather than requiring a seperate export to CSV outside of the NeDRexDB code).
- [ ] All integrations for edges now include a check to ensure that both nodes involved (source/target or memberOne/memberTwo, as appropriate) exist in NeDRexDB.


## Database orchestration
Database orchestration changes have been added to facilitate automatic updates. This is implemented by setting up a second instance of MongoDB in which NeDRexDB is rebuilt; the database volume of this container then replaces the database volume of the main MongoDB instance<sup>$</sup>, effecting the update.

- [ ] Code to orchestrate docker containers and volumes is now included in this repository.
- [ ] Code to download the latest version of source databases is now included in this repository.

<sup>$</sup> The scheduling of the NeDRexDB update is not included in this repository, because this depends on external factors (e.g., whether any tasks are running in the API). 
