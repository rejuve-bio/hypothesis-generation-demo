:- use_module(library(mcintyre)).
:- mc.
:- use_module(library(janus)).
:- use_module(library(plstat), []).
:- use_module(library(solution_sequences)).
:- begin_lpad.

relevant_gene(Gene, Locus): 1e-4 :-
    in_credible_set(Locus, Snp, _PIP),
    once(coding_effect(Snp, Gene, stop_gained)).

relevant_gene(Gene, Locus): 1e-4 :-
    in_credible_set(Locus, Snp, _PIP),
    once(coding_effect(Snp, Gene, frameshift_variant)).

relevant_gene(Gene, Locus): 1e-4 :-
    in_credible_set(Locus, Snp, _PIP),
    once(coding_effect(Snp, Gene, missense_probably_damaging)).

relevant_gene(Gene, Locus): 1e-4 :-
    in_credible_set(Locus, Snp, _PIP),
    once(coding_effect(Snp, Gene, missense_benign)).

relevant_gene(Gene, Locus): 0.0004 :-
    once(pqtl_coloc(Locus, Gene)).

relevant_gene(Gene, Locus): 1e-4 :-
    once(sqtl_coloc(Locus, Gene)).

relevant_gene(Gene, Locus): 0.0003 :-
    once(eqtl_coloc(Locus, Gene)).

relevant_gene(Gene, Locus): 0.2687 :-
    in_credible_set(Locus, Snp, _PIP),
    once(pqtl_association(Snp, Gene)).

relevant_gene(Gene, Locus): 1e-4 :-
    in_credible_set(Locus, Snp, _PIP),
    once(eqtl_association(Snp, Gene)).

relevant_gene(Gene, Locus): 1e-4 :-
    in_credible_set(Locus, Snp, _PIP),
    once(sqtl_association(Snp, Gene)).

relevant_gene(Gene, Locus): 0.0209 :-
    in_credible_set(Locus, Snp, _PIP),
    regulatory_effect_4(Snp, Gene, Enh, Tf).

relevant_gene(Gene, Locus): 0.0014 :-
    in_credible_set(Locus, Snp, _PIP),
    regulatory_effect_3(Snp, Gene, Enh, Tf).

relevant_gene(Gene, Locus): 0.0103 :-
    in_credible_set(Locus, Snp, _PIP),
    regulatory_effect_2(Snp, Gene, Enh).

relevant_gene(Gene, Locus): 1e-4 :-
    in_credible_set(Locus, Snp, _PIP),
    regulatory_effect_1(Snp, Gene, Enh).

relevant_gene(Gene, Locus): 0.1057 :-
    in_credible_set(Locus, Snp, _PIP),
    distance_bin(Snp, Gene, very_close).

relevant_gene(Gene, Locus): 0.1625 :-
    in_credible_set(Locus, Snp, _PIP),
    distance_bin(Snp, Gene, close).

relevant_gene(Gene, Locus): 0.0045 :-
    in_credible_set(Locus, Snp, _PIP),
    distance_bin(Snp, Gene, moderate).

relevant_gene(Gene, Locus): 1e-4 :-
    in_credible_set(Locus, Snp, _PIP),
    once(in_tad_with(Snp, Gene)).

:- end_lpad.