:- style_check(-discontiguous).
:- dynamic(user:file_search_path/2).
:- multifile(user:file_search_path/2).
user:file_search_path(metta_out,'/mnt/hdd_2/abdu/prolog_out_v1').
user:file_search_path(gencode, metta_out('gencode')).
user:file_search_path(uniprot, metta_out('uniprot')).
user:file_search_path(ontology, metta_out('ontology')).
user:file_search_path(gaf, metta_out('gaf')).
user:file_search_path(cellxgene, metta_out('cellxgene')).
user:file_search_path(eqtl, metta_out('gtex/eqtl')).
user:file_search_path(tadmap, metta_out('tadmap')).
user:file_search_path(refseq, metta_out('refseq')).

:- time(consult(eqtl(edges))).
:- time(consult([gencode(nodes), gencode(edges)])).
:- time(consult([uniprot(nodes), uniprot(edges)])).
:- time(consult([ontology(nodes), ontology(edges)])).
:- time(consult(gaf(edges))).
:- time(consult(cellxgene(edges))).
:- time(consult(tadmap(edges))).
:- time(consult(refseq(edges))).