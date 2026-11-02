:- use_module(library(liftcover)).
:- discontiguous relevant_gene/3.
:- discontiguous neg/1.
:- multifile relevant_gene/3.
:- multifile neg/1.
:- dynamic relevant_gene/3.
:- dynamic neg/1.
:- dynamic fold/2.
:- lift.

:- set_lift(verbosity, 3).
:- set_lift(iter, -1).
:- set_lift(random_restarts_number , 5).
:- set_lift(neg_ex, given).
:- set_lift(eps, 0.001).
:- set_lift(threads, 1).


:- begin_in.

relevant_gene(Gene, Locus): 0.90 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        coding_effect(Snp, Gene, stop_gained)
    )).

relevant_gene(Gene, Locus): 0.90 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        coding_effect(Snp, Gene, frameshift_variant)
    )).

relevant_gene(Gene, Locus): 0.75 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        coding_effect(Snp, Gene, missense_probably_damaging)
    )).

relevant_gene(Gene, Locus): 0.35 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        coding_effect(Snp, Gene, missense_benign)
    )).


relevant_gene(Gene, Locus): 0.80 :-
    once(pqtl_coloc(Locus, Gene)).

relevant_gene(Gene, Locus): 0.60 :-
    once(sqtl_coloc(Locus, Gene)).

relevant_gene(Gene, Locus): 0.55 :-
    once(eqtl_coloc(Locus, Gene)).

relevant_gene(Gene, Locus): 0.08 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        pqtl_association(Snp, Gene)
    )).

relevant_gene(Gene, Locus): 0.05 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        eqtl_association(Snp, Gene)
    )).

relevant_gene(Gene, Locus): 0.05 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        sqtl_association(Snp, Gene)
    )).


relevant_gene(Gene, Locus): 0.55 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        regulatory_effect_4(Snp, Gene, _Enh, _Tf)
    )).

relevant_gene(Gene, Locus): 0.40 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        regulatory_effect_3(Snp, Gene, _Enh, _Tf)
    )).

relevant_gene(Gene, Locus): 0.30 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        regulatory_effect_2(Snp, Gene, _Enh)
    )).

relevant_gene(Gene, Locus): 0.08 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        regulatory_effect_1(Snp, Gene, _Enh)
    )).

relevant_gene(Gene, Locus): 0.18 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        distance_bin(Snp, Gene, very_close)
    )).

relevant_gene(Gene, Locus): 0.12 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        distance_bin(Snp, Gene, close)
    )).

relevant_gene(Gene, Locus): 0.05 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        distance_bin(Snp, Gene, moderate)
    )).

relevant_gene(Gene, Locus): 0.06 :-
    once((
        in_credible_set(Locus, Snp, _PIP),
        in_tad_with(Snp, Gene)
    )).

:- end_in.