% :- use_module(library(mcintyre)).
% :- mc.
:- use_module(library(janus)).
:- use_module(library(liftcover)).
:- use_module(library(plstat)).
:- lift.

% :- begin_lpad.
:- begin_in.

relevant_gene(Gene, Snp):0.348 :-
  regulatory_effect(Snp, Gene).

relevant_gene(Gene, Snp):0.0186 :-
  eqtl_association(Snp, Gene).

relevant_gene(Gene, Snp):0.0035 :-
  activity_by_contact(Snp, Gene).

relevant_gene(Gene, Snp):0.1616 :-
  pgboost(Snp, Gene).
  
:- end_in.

regulatory_effect(S, G) :- 
    in_regulatory_region(S, Enh),
    associated_with(Enh, G).
    % tf_snp(Tf, S),
    % regulates(Tf, G),
    % binds_to(Tf, Tfbs),
    % overlaps_with(Tfbs, Enh), !.


coding_effect(S, G) :- 
  hideme([
    chr(S, Chr),
    start(S, Pos),
    ref(S, Ref),
    alt(S, Alt),
    has_coding_effect(G, Chr, Pos, Ref, Alt)
  ]).

in_regulatory_region(S, Enh) :-
    hideme([S = snp(_),
    (Enh = super_enhancer(E)
    ;Enh = enhancer(E)),
    within_k_distance(Enh, S, 50000, 1)]). %50,000kb obtained from dbsup

alters_tfbs(S, Tf, G) :-
    find_and_rank_tfs(S, Tf, G).
    % format('Alters TFBS: S: ~w, Tf: ~w, G: ~w~n', [S, Tf, G]).


load_enh_tfbs(Enh) :-
  chr(Enh, Chr),
  start(Enh, Start),
  end(Enh, End),
  load_tfbs_data(Chr, Start, End).

codes_for(G, P) :-
    transcribed_to(G, T),
    translates_to(T, P).

in_tad_with(S, G1) :- 
    (closest_gene(S, G1)
    ;
    (closest_gene(S, G2),
    in_tad_region(G2, T),
    in_tad_region(G1, T))).

% :- end_lpad.

score_examples(Dir, Fold, LabelScores) :-
  load_test_fold(Dir, Fold, TestIndices),
  format(atom(ModelPath), '~w/fold_~w/models.pl', [Dir, Fold]),
  read_model_file(ModelPath, Models),
  assert_all(Models, ModelsRef),
  in(Program),
  findall(Label-Score, 
  (
    member(Index, TestIndices),
    eval_pair(Index, gene(G), snp(S), Label),
    prob_lift(relevant_gene(Index, gene(G), snp(S)), Program, Score)
  ), LabelScores).

auc_eval_fold(Dir, Fold, AUC) :-
  score_examples(Dir, Fold, Pairs),
  pairs_keys_values(Pairs, Labels, Scores),
  py_call(importlib:import_module('sklearn.metrics'), SklearnMetrics),
  py_call(SklearnMetrics:roc_auc_score(Labels, Scores), AUC),
  format("Fold ~w AUC: ~f~n", [Fold, AUC]),
  retract_all(ModelsRef).

auc_eval_all(Dir, NumFolds, M_AUC, S_AUC) :-
  MaxFold is NumFolds - 1,
  findall(AUC, (
    between(0, MaxFold, Fold),
    auc_eval_fold(Dir, Fold, AUC)
  ), AUCs), 
  format("AUCs across ~w Folds: ~w", [NumFolds, AUCs]),
  mean(AUCs, M_AUC),
  std_dev(AUCs, S_AUC).

