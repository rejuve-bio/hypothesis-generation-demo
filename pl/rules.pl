% :- use_module(queries).
:- use_module(library(mcintyre)).
:- mc.
:- style_check(-discontiguous).
:- use_module(library(clpfd)).
% :- use_module(library(liftcover)).
% :- use_module(library(plstat)).
% :- lift.

:- dynamic hideme/1.
:- multifile hideme/1.

hideme([]).
hideme([Goal|Goals]) :-
  call(Goal),
  hideme(Goals).
hideme(Goal) :-
    call(Goal).

% regulatory_effect_1(S, G) :- 
%     in_regulatory_region(S, Enh),
%     activity_by_contact(Enh, G, _Score).

upcase_gene(gene(Id), gene(UId)) :-
    upcase_atom(Id, UId).

% Gets the top 10 (ranked by PIP) snps in credible set Locus
causal(Locus, Snp) :-
    in_credible_set(Locus, Snp, _PIP).

distance_bin(snp(S), gene(G), very_close) :-   % < 5 kb
    gene_body_distance(snp(S), gene(G), D), D < 5000.
distance_bin(snp(S), gene(G), close) :-         % 5-50 kb
    gene_body_distance(snp(S), gene(G), D), D >= 5000, D < 50000.
distance_bin(snp(S), gene(G), moderate) :-      % 50-200 kb
    gene_body_distance(snp(S), gene(G), D), D >= 50000, D < 200000.

regulatory_effect_4(S, G, Enh, Tf) :-
    in_regulatory_region(S, Enh),
    activity_by_contact(Enh, G, _Score),
    % variant_in_tfbs(S, Tf),
    tfbs_effect(S, Tf, _TfScore, _Effect),
    regulates(Tf, G).

regulatory_effect_3(S, G, Enh, Tf) :-
    in_regulatory_region(S, Enh),
    activity_by_contact(Enh, G, _Score),
    % variant_in_tfbs(S, Tf).
    tfbs_effect(S, Tf, _TfScore, _Effect).

regulatory_effect_2(S, G, Enh) :-
    in_regulatory_region(S, Enh),
    activity_by_contact(Enh, G, _Score).

regulatory_effect_1(S, G, enhancer(EnhId)) :-
    in_regulatory_region(S, enhancer(EnhId)),
    atomic_list_concat([EnhId, '_grch38'], EnhGrch38),
    associated_with(enhancer(EnhGrch38), RawGene),
    upcase_gene(RawGene, G).

eqtl_association(Variant, Gene) :-
    hideme(qtl_association(eqtl, Variant, Gene)).

pqtl_association(Variant, Gene) :-
    hideme(qtl_association(pqtl, Variant, Gene)).

sqtl_association(Variant, Gene) :-
    hideme(qtl_association(sqtl, Variant, Gene)).


pqtl_coloc(Locus, Gene) :-
    hideme((qtl_coloc_gene(pqtl, Locus, Gene, H4, _Tissue), H4 > 0.8)).

eqtl_coloc(Locus, Gene) :-
    hideme((qtl_coloc_gene(eqtl, Locus, Gene, H4, _Tissue), H4 > 0.8)).

sqtl_coloc(Locus, Gene) :-
    hideme((qtl_coloc_gene(sqtl, Locus, Gene, H4, _Tissue), H4 > 0.8)).

coding_effect(S, G, Effect) :- 
  hideme(has_coding_effect(S, G, Effect)).
