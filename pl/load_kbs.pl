:- style_check(-discontiguous).
:- dynamic(user:file_search_path/2).
:- multifile(user:file_search_path/2).

load_with_time(Files, FileName) :-
    format("Loading ~w...~n", [FileName]),
    time(consult(Files)),
    format("Loaded ~w!~n", [FileName]).

user:file_search_path(prolog_out,'/mnt/hdd_2/abdu/prolog_out_v2').
user:file_search_path(gene, prolog_out('gencode/gene')).
user:file_search_path(exon, prolog_out('gencode/exon')).
user:file_search_path(transcript, prolog_out('gencode/transcript')).
user:file_search_path(uniprot, prolog_out('uniprot')).
user:file_search_path(gene_ontology, prolog_out('gene_ontology')).
user:file_search_path(gaf, prolog_out('gaf')).
% user:file_search_path(cellxgene, prolog_out('cellxgene')).
user:file_search_path(eqtl, prolog_out('gtex/eqtl')).
user:file_search_path(tadmap, prolog_out('tadmap')).
user:file_search_path(refseq, prolog_out('refseq')).
% Add additional paths (abc, cell_line_ontology, uberon, cadd, dbsnp, dbsuper, enhancer_atlas, roadmap, uberon, 
% fabian, epd, peregrine)
user:file_search_path(abc, prolog_out('abc')).
user:file_search_path(cell_line_ontology, prolog_out('cell_line_ontology')).
user:file_search_path(uberon, prolog_out('uberon')).
user:file_search_path(cadd, prolog_out('cadd')).
user:file_search_path(dbsnp, prolog_out('dbsnp')).
user:file_search_path(dbsuper, prolog_out('dbsuper')).
user:file_search_path(enhancer_atlas, prolog_out('enhancer_atlas')).
user:file_search_path(roadmap_chromatin_state, prolog_out('roadmap/chromatin_state')).
user:file_search_path(roadmap_dhs, prolog_out('roadmap/dhs')).
user:file_search_path(roadmap_h3_mark, prolog_out('roadmap/h3_mark')).
user:file_search_path(epd, prolog_out('epd')).
user:file_search_path(fabian, prolog_out('fabian')).
user:file_search_path(peregrine, prolog_out('peregrine')).
user:file_search_path(top_ld_eur, prolog_out('top_ld/EUR')).

:- load_with_time([transcript(nodes), transcript(edges)], "gencode transcripts").
:- load_with_time([gene(nodes)], "gencode genes").
:- load_with_time([exon(nodes)], "gencode exons").
:- load_with_time([uniprot(nodes), uniprot(edges)], "uniprot").
:- load_with_time([eqtl(edges)], "gtex eqtl").
:- load_with_time([gene_ontology(nodes), gene_ontology(edges)], "gene ontology").
:- load_with_time([gaf(edges)], "go gene product").
% % :- load_with_time([cellxgene(edges)], ).
:- load_with_time([tadmap(nodes), tadmap(edges)], "tadmap").
:- load_with_time([refseq(edges)], "refseq").
% % Consult additional files (abc, cell_line_ontology, cadd, dbsnp, dbsuper, enhancer_atlas, roadmap, uberon, epd, peregrine)
:- load_with_time([abc(edges)], "abc").
:- load_with_time([cell_line_ontology(nodes), cell_line_ontology(edges)], "cell_line ontology").
:- load_with_time([uberon(nodes), uberon(edges)], "uberon").
:- load_with_time([cadd(nodes)], "cadd").
:- load_with_time([dbsnp(nodes)], "dbsnp").
:- load_with_time([dbsuper(nodes), dbsuper(edges)], "dbsuper").
:- load_with_time([enhancer_atlas(nodes), enhancer_atlas(edges)], "enhancer atlas").
:- load_with_time([roadmap_chromatin_state(edges)], "roadmap chromatin state").
:- load_with_time([roadmap_dhs(edges)], "roadmap dhs").
:- load_with_time([roadmap_h3_mark(edges)], "roadmap h3 mark").
:- load_with_time([epd(nodes), epd(edges)], "epd").
:- load_with_time([peregrine(nodes), peregrine(edges)], "peregrine").
:- load_with_time([fabian(edges)], "fabian").
:- load_with_time([top_ld_eur(edges)], "top_ld").
