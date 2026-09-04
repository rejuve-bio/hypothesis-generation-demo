:- module(hypgen, [
    server_start/1,
    server_stop/1,
    candidate_genes/2,
    within_k_distance/3,
    relevant_gene/2,
    hideme/1,
    load_atomspace/0, 
    init/0, 
    json_proof_tree/3, 
    proof_tree/4,
    set_mi_depth/1]).

:- use_module(library(janus)).
:- use_module(library(http/http_client)).
:- use_module(library(http/json)).
:- use_module(library(thread)).

:- style_check(-discontiguous).
:- style_check(-singleton).

:- include('load_kbs.pl').
:- include('queries.pl').
:- include('utils.pl').
:- include('rules.pl').
:- table causal/2.
:- table in_credible_set/3.
:- table coding_effect/3.
:- table pqtl_coloc/2.
:- table eqtl_coloc/2.
:- table sqtl_coloc/2.
:- table pqtl_association/2.
:- table eqtl_association/2.
:- table sqtl_association/2.
:- table regulatory_effect_4/4.
:- table regulatory_effect_3/4.
:- table regulatory_effect_2/3.
:- table regulatory_effect_1/3.
:- table distance_bin/3.
:- table in_tad_with/2.
:- include('inference_rules.pl').
% :- include('train_rules.pl')
% :- include('param_learn.pl').
:- include('server.pl').
:- include('hypgen_server.pl').
:- include('meta_interpreter.pl').
:- include('json_util.pl').


init :- 
    format("Loading atomspace...~n", []),
    load_atomspace,
    format("Done!~n", []).
