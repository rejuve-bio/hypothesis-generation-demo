:- use_module(library(yaml)).
:- style_check(-discontiguous).
:- set_prolog_flag(style_check, false).
:- dynamic(user:file_search_path/2).
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
:- multifile ccre_module/2.


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
:- multifile tf_snp/2.
:- multifile binds_to/2.
:- multifile in_tad_region/2.
:- multifile activity_by_contact/2.
:- multifile chromatin_state/2.
:- multifile in_dnase_I_hotspot/2.
:- multifile histone_modification/2.
:- multifile distance/2.
:- multifile pgboost/2.
:- multifile development_stage/2.
:- multifile accessible_in/2.

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
:- multifile accession_e/2.
:- multifile taxon_id/2.

% Additional binary predicates
:- multifile synonyms/2.
:- multifile rel_type/2.


load_with_time(Files, Name) :-
    format("Loading ~w...~n", [Name]),
    time(consult(Files)),
    format("Loaded ~w!~n", [Name]).

kb_yaml_path(Path) :-
    source_file(load_atomspace, File),
    file_directory_name(File, Dir),
    atomic_list_concat([Dir, '/../config/kb.yaml'], Path).

load_kb_entry(Key, Entry) :-
    (   get_dict(nodes, Entry, true)
    ->  NodeTerm =.. [Key, nodes], NodesFiles = [NodeTerm]
    ;   NodesFiles = []
    ),
    (   get_dict(edges, Entry, true)
    ->  EdgeTerm =.. [Key, edges], EdgesFiles = [EdgeTerm]
    ;   EdgesFiles = []
    ),
    append(NodesFiles, EdgesFiles, Files),
    (   Files \= []
    ->  load_with_time(Files, Key)
    ;   true
    ).

load_atomspace :-
    kb_yaml_path(YamlPath),
    yaml_read(YamlPath, Config),
    get_dict(settings, Config, Settings),
    get_dict(prolog_base, Settings, BasePath),
    atom_string(BasePathAtom, BasePath),
    assertz(user:file_search_path(prolog_out, BasePathAtom)),
    get_dict(kbs, Config, KBs),
    dict_pairs(KBs, _, Pairs),
    forall(
        member(Key-Entry, Pairs),
        (   get_dict(path, Entry, KBPath),
            atom_string(KBPathAtom, KBPath),
            assertz(user:file_search_path(Key, prolog_out(KBPathAtom))),
            (   get_dict(loaded, Entry, true)
            ->  load_kb_entry(Key, Entry)
            ;   true
            )
        )
    ).
