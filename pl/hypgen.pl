% :- module(hypgen, [
%     server_start/1,
%     server_stop/1,
%     candidate_genes/2,
%     within_k_distance/3,
%     find_and_rank_tfs/3,
%     bgc/1,
%     %relevant_gene/2,
%     %hideme/1,
%     load_atomspace/0]).
%     %in_tad_with/2,
%     %regulatary_effect/2,
%     %in_regulatory_region/2,
%     %alters_tfbs/3,
%     %overlaps_with_tf_enh/2,
%     %read_partition_file/2,
%     %read_samples/2,
%     %read_train_set/1,
%     %read_test_set/1,
%     %fold/2
%     %]).
% :- use_module(library(main)).

:- style_check(-discontiguous).
:- style_check(-singleton).

:- include('util.pl').
:- include('load_kbs.pl').
:- include('queries.pl').
% :- include('rules.pl').
:- include('pl_rules.pl').
% :- include('param_learn.pl').
:- include('server.pl').

init :- 
    format("Loading atomspace...~n", []),
    load_atomspace,
    set_prolog_flag(stack_limit, 103_079_215_104), 
    % format("Asserting background rules & knowledge...~n", []),
    % findall(F, bgc(F), Facts),
    % findall(R, rules(R), Rs),
    % append(Rs, Rules),
    % append(Facts, Rules, Bg),
    % assertz(bg(Bg)),
    % length(Bg, L),
    % format("Num of background facts/rules: ~d~n", [L]),
    format("Done!~n", []).

% :- initialization(init, main).