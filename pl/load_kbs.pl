:- style_check(-discontiguous).
:- dynamic(user:file_search_path/2).
% :- dynamic(bgc/1).
% :- multifile(bgc/1).
:- multifile(user:file_search_path/2).
% Node predicates
:- multifile gene/1.
:- multifile protein/1.
:- multifile transcript/1.
:- multifile exon/1.
:- multifile snp/1.
:- multifile structural_variant/1.
:- multifile sequence_variant/1.
:- multifile enhancer/1.
:- multifile promoter/1.
:- multifile super_enhancer/1.
:- multifile non_coding_rna/1.
:- multifile pathway/1.
:- multifile regulatory_region/1.
:- multifile transcription_binding_site/1.
:- multifile go/1.
:- multifile uberon/1.
:- multifile clo/1.
:- multifile cl/1.
:- multifile efo/1.
:- multifile bto/1.
:- multifile motif/1.
:- multifile tad/1.

% Edge predicates
:- multifile transcribed_to/2.
:- multifile transcribed_from/2.
:- multifile translates_to/2.
:- multifile translation_of/2.
:- multifile coexpressed_with/2.
:- multifile interacts_with/2.
:- multifile expressed_in/2.
:- multifile has_part/2.
:- multifile part_of/2.
:- multifile subclass_of/2.
:- multifile capable_of/2.
:- multifile genes_pathways/2.
:- multifile parent_pathway_of/2.
:- multifile child_pathway_of/2.
:- multifile go_gene_product/2.
:- multifile belongs_to/2.
:- multifile associated_with/2.
:- multifile regulates/2.
:- multifile eqtl_association/2.
:- multifile closest_gene/2.
:- multifile upstream_gene/2.
:- multifile downstream_gene/2.
:- multifile in_gene/2.
:- multifile in_ld_with/2.
:- multifile lower_resolution/2.
:- multifile located_on_chain/2.
:- multifile tfbs_snp/2.
:- multifile binds_to/2.
:- multifile in_tad_region/2.
:- multifile activity_by_contact/2.
:- multifile chromatin_state/2.
:- multifile in_dnase_I_hotspot/2.
:- multifile histone_modification/2.
:- multifile regulates/2.
:- multifile distance/2.

% Properties
:- multifile chr/2.
:- multifile start/2.
:- multifile end/2.
:- multifile alt/2.
:- multifile ref/2.
:- multifile gene_name/2.
:- multifile transcript_name/2.
:- multifile transcript_id/2.
:- multifile transcript_type/2.
:- multifile score/2.
:- multifile biological_context/2.
:- multifile caf_ref/2.
:- multifile caf_alt/2.
:- multifile term_name/2.
:- multifile term_name/3.
:- multifile term_name/4.
:- multifile term_name/5.
:- multifile term_name/6.
:- multifile term_name/7.
:- multifile term_name/8.
:- multifile term_name/9.
:- multifile raw_cadd_score/2.
:- multifile phred_score/2.
:- multifile detection_method/2.
:- multifile evidence_type/2.
:- multifile slope/2.
:- multifile maf/2.
:- multifile p_value/2.
:- multifile accession_d/2.

% Additional binary predicates
:- multifile synonyms/2.
:- multifile rel_type/2.


load_with_time(Files, FileName) :-
    format("Loading ~w...~n", [FileName]),
    time(consult(Files)),
    format("Loaded ~w!~n", [FileName]).


user:file_search_path(prolog_out_v2,'/mnt/hdd_1/abdu/prolog_out_v2').
user:file_search_path(prolog_out,'/mnt/hdd_1/abdu/prolog_out_v3').
% user:file_search_path(prolog_out,'/mnt/hdd_2/abdu/prolog_out_v4').
% user:file_search_path(prolog_out,'/mnt/hdd_2/abdu/prolog_out').
user:file_search_path(gene, prolog_out('gencode/gene')).
user:file_search_path(exon, prolog_out('gencode/exon')).
user:file_search_path(transcript, prolog_out('gencode/transcript')).
user:file_search_path(uniprot, prolog_out('uniprot')).
user:file_search_path(gene_ontology, prolog_out('gene_ontology')).
user:file_search_path(gaf, prolog_out('gaf')).
% user:file_search_path(cellxgene, prolog_out('cellxgene')).
user:file_search_path(tadmap, prolog_out('tadmap')).
user:file_search_path(tflink, prolog_out('tflink')).
%user:file_search_path(roadmap_chromatin_state, prolog_out('roadmap/chromatin_state')).
%user:file_search_path(roadmap_dhs, prolog_out('roadmap/dhs')).
%user:file_search_path(roadmap_h3_mark, prolog_out('roadmap/h3_mark')).

user:file_search_path(refseq, prolog_out('refseq')).
user:file_search_path(eqtl, prolog_out('gtex/eqtl')).
user:file_search_path(abc, prolog_out('abc')).
% user:file_search_path(cell_line_ontology, prolog_out('cell_line_ontology')).
% user:file_search_path(uberon, prolog_out('uberon')).
% user:file_search_path(efo, prolog_out('experimental_factor_ontology')).
% user:file_search_path(bto, prolog_out('brenda_tissue_ontology')).
% user:file_search_path(cadd, prolog_out('cadd')).
user:file_search_path(dbsnp, prolog_out('dbsnp')).
user:file_search_path(dbsuper, prolog_out('dbsuper')).
user:file_search_path(enhancer_atlas, prolog_out('enhancer_atlas')).
% user:file_search_path(epd, prolog_out('epd')).
%user:file_search_path(fabian, prolog_out('fabian')).
user:file_search_path(peregrine, prolog_out('peregrine')).
% user:file_search_path(top_ld_eur, prolog_out('top_ld/EUR')).
user:file_search_path(tfbs, prolog_out('tfbs')).
user:file_search_path(tf_snp, prolog_out('tf_snp')).
user:file_search_path(enhancer_ccre, prolog_out('ccre/enhancer_ccre')).
user:file_search_path(promoter_ccre, prolog_out('ccre/promoter_ccre')).


load_atomspace :- 
    %load_with_time([transcript(nodes), transcript(edges)], "gencode transcripts"),
    load_with_time([gene(nodes)], "gencode genes"),
    % load_with_time([exon(nodes)], "gencode exons"),
    %load_with_time([uniprot(nodes), uniprot(edges)], "uniprot"),
    load_with_time([eqtl(edges)], "gtex eqtl"),
    %load_with_time([gene_ontology(nodes), gene_ontology(edges)], "gene ontology"),
    %load_with_time([gaf(edges)], "go gene product"),
    % % load_with_time([cellxgene(edges)], ),
    load_with_time([tadmap(nodes), tadmap(edges)], "tadmap"),
    load_with_time([refseq(edges)], "refseq"),
    load_with_time([abc(edges)], "abc"),
    %load_with_time([cell_line_ontology(nodes), cell_line_ontology(edges)], "cell_line ontology"),
    %load_with_time([uberon(nodes), uberon(edges)], "uberon"),
    %load_with_time([efo(nodes), efo(edges)], "experimental factor ontology"),
    %load_with_time([bto(nodes), bto(edges)], "brenda tissue ontology"),
    % load_with_time([cadd(nodes)], "cadd"),
    load_with_time([dbsnp(nodes)], "dbsnp"),
    load_with_time([dbsuper(nodes), dbsuper(edges)], "dbsuper"),
    % load_with_time([enhancer_ccre(nodes), enhancer_ccre(edges)], "enhancer ccre"),
    % load_with_time([promoter_ccre(nodes), promoter_ccre(edges)], "promoter ccre"),
    load_with_time([enhancer_atlas(nodes), enhancer_atlas(edges)], "enhancer atlas"),
    %load_with_time([roadmap_chromatin_state(edges)], "roadmap chromatin state"),
    %load_with_time([roadmap_dhs(edges)], "roadmap dhs"),
    %load_with_time([roadmap_h3_mark(edges)], "roadmap h3 mark"),
    % load_with_time([epd(nodes), epd(edges)], "epd"),
    load_with_time([tflink(edges)], "tflink"),
    load_with_time([peregrine(nodes), peregrine(edges)], "peregrine"),
    %load_with_time([fabian(edges)], "fabian"),
    %load_with_time([top_ld_eur(edges)], "top_ld"),
    load_with_time([tfbs(nodes), tfbs(edges)], "tfbs"),
    load_with_time([tf_snp(edges)], "tf_snp").

% bgc declarations for parameter learning with cplint
% Node predicates
bgc(gene(X)) :- gene(X), chr(gene(X), chr16).
bgc(gene_name(X, Y)) :- gene_name(X, Y), chr(X, chr16).
% bgc(protein(X)) :- protein(X).
% bgc(transcript(X)) :- transcript(X).
% bgc(exon(X)) :- exon(X).
bgc(snp(X)) :- chr(snp(X), chr16).
% bgc(structural_variant(X)) :- structural_variant(X).
% bgc(sequence_variant(X)) :- sequence_variant(X).
% bgc(enhancer(X)) :- enhancer(X).
% bgc(promoter(X)) :- promoter(X).
bgc(super_enhancer(X)) :- super_enhancer(X).
% bgc(non_coding_rna(X)) :- non_coding_rna(X).
% bgc(pathway(X)) :- pathway(X).
bgc(regulatory_region(X)) :- regulatory_region(X).
% bgc(transcription_binding_site(X)) :- transcription_binding_site(X).
% bgc(go(X)) :- go(X).
% bgc(uberon(X)) :- uberon(X).
% bgc(clo(X)) :- clo(X).
% bgc(cl(X)) :- cl(X).
% bgc(efo(X)) :- efo(X).
% bgc(bto(X)) :- bto(X).
% bgc(motif(X)) :- motif(X).
bgc(tad(X)) :- tad(X), chr(tad(X), chr16).

% Edge predicates
% bgc(transcribed_to(X, Y)) :- transcribed_to(X, Y).
% bgc(transcribed_from(X, Y)) :- transcribed_from(X, Y).
% bgc(translates_to(X, Y)) :- translates_to(X, Y).
% bgc(translation_of(X, Y)) :- translation_of(X, Y).
% bgc(coexpressed_with(X, Y)) :- coexpressed_with(X, Y).
% bgc(interacts_with(X, Y)) :- interacts_with(X, Y).
% bgc(expressed_in(X, Y)) :- expressed_in(X, Y).
% bgc(has_part(X, Y)) :- has_part(X, Y).
% bgc(part_of(X, Y)) :- part_of(X, Y).
% bgc(subclass_of(X, Y)) :- subclass_of(X, Y).
% bgc(capable_of(X, Y)) :- capable_of(X, Y).
% bgc(genes_pathways(X, Y)) :- genes_pathways(X, Y).
% bgc(parent_pathway_of(X, Y)) :- parent_pathway_of(X, Y).
% bgc(child_pathway_of(X, Y)) :- child_pathway_of(X, Y).
% bgc(go_gene_product(X, Y)) :- go_gene_product(X, Y).
% bgc(belongs_to(X, Y)) :- belongs_to(X, Y).
bgc(associated_with(X, Y)) :- associated_with(X, Y).
bgc(regulates(X, Y)) :- regulates(X, Y).
% bgc(eqtl_association(X, Y)) :- eqtl_association(X, Y).
bgc(closest_gene(X, Y)) :- closest_gene(X, Y), chr(Y, chr16).
% bgc(upstream_gene(X, Y)) :- upstream_gene(X, Y).
% bgc(downstream_gene(X, Y)) :- downstream_gene(X, Y).
% bgc(in_gene(X, Y)) :- in_gene(X, Y).
% bgc(in_ld_with(X, Y)) :- in_ld_with(X, Y).
% bgc(lower_resolution(X, Y)) :- lower_resolution(X, Y).
% bgc(located_on_chain(X, Y)) :- located_on_chain(X, Y).
bgc(tfbs(X)) :- tfbs(X), chr(X, chr16).
bgc(tf_snp(X, Y)) :- tf_snp(X, Y).
bgc(binds_to(X, Y)) :- binds_to(X, Y).
bgc(in_tad_region(X, Y)) :- in_tad_region(X, Y), chr(Y, chr16).
% bgc(activity_by_contact(X, Y)) :- activity_by_contact(X, Y).
% bgc(chromatin_state(X, Y)) :- chromatin_state(X, Y).
% bgc(in_dnase_I_hotspot(X, Y)) :- in_dnase_I_hotspot(X, Y).
% bgc(histone_modification(X, Y)) :- histone_modification(X, Y).

% Properties
bgc(chr(X, Y)) :- chr(X, chr16).
bgc(start(X, Y)) :- chr(X, chr16), start(X, Y).
bgc(end(X, Y)) :- chr(X, chr16), end(X, Y).
bgc(alt(X, Y)) :- chr(X, chr16), alt(X, Y).
bgc(ref(X, Y)) :- chr(X, chr16), ref(X, Y).
bgc(gene_name(X, Y)) :- gene_name(X, Y).
%bgc(transcript_name(X, Y)) :- transcript_name(X, Y).
%bgc(transcript_id(X, Y)) :- transcript_id(X, Y).
%bgc(transcript_type(X, Y)) :- transcript_type(X, Y).
bgc(score(X, Y)) :- chr(X, chr16), score(X, Y).
%bgc(biological_context(X, Y)) :- biological_context(X, Y).
%bgc(term_name(X, Y)) :- term_name(X, Y).
%bgc(term_name(X, Y, Z)) :- term_name(X, Y, Z).
%bgc(term_name(X, Y, Z, W)) :- term_name(X, Y, Z, W).
%bgc(term_name(X, Y, Z, W, V)) :- term_name(X, Y, Z, W, V).
%bgc(term_name(X, Y, Z, W, V, U)) :- term_name(X, Y, Z, W, V, U).
%bgc(term_name(X, Y, Z, W, V, U, T)) :- term_name(X, Y, Z, W, V, U, T).
%bgc(term_name(X, Y, Z, W, V, U, T, S)) :- term_name(X, Y, Z, W, V, U, T, S).
%bgc(term_name(X, Y, Z, W, V, U, T, S, R)) :- term_name(X, Y, Z, W, V, U, T, S, R).
%bgc(raw_cadd_score(X, Y)) :- raw_cadd_score(X, Y).
%bgc(phred_score(X, Y)) :- phred_score(X, Y).
%bgc(detection_method(X, Y)) :- detection_method(X, Y).
%bgc(evidence_type(X, Y)) :- evidence_type(X, Y).
bgc(effect(X, Y)) :- effect(X, Y).
bgc(score(X)) :- score(X, Y).

% Additional binary predicates
%bgc(synonyms(X, Y)) :- synonyms(X, Y).
%bgc(rel_type(X, Y)) :- rel_type(X, Y).

