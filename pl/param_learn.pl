:- style_check(-discontiguous).
:- use_module(library(janus)).
:- use_module(library(clpfd)).
:- use_module(library(auc)).
:- use_module(library(liftcover)).
:- use_module(library(plstat)).
:- use_module(library(http/json)).
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
:- set_lift(random_restarts_number , 3).
:- set_lift(neg_ex, given).
:- set_lift(eps, 0.001).
:- set_lift(threads, 20).
% :- set_lift(parameter_learning, gd).
% :- set_lift(gamma, 0.01).
% :- set_lift(regularization, l2).
% :- set_lift(max_initial_weight, 0.01).
% :- set_lift(eta, 0.001).
% :- set_lift(processor, cpu).  

passes(relevant_gene(G, S)) :- relevant_gene(G, S).

:- begin_in.

% relevant_gene(G, S): 0.25 :- in_tad_with(S, G).

relevant_gene(G, S): 0.25 :- regulatory_effect(S, G).

relevant_gene(G, S): 0.25 :- eqtl_association(S, G).
relevant_gene(G, S): 0.25 :- activity_by_contact(S, G).

:- end_in.


read_partition_file(Filename, Numbers) :- 
 setup_call_cleanup(open(Filename, read, Stream),
        read_samples(Stream, Numbers),
        close(Stream)).

read_model_file(Filename, Models) :- 
 setup_call_cleanup(open(Filename, read, Stream),
        read_models(Stream, Models),
        close(Stream)).

read_samples(Stream, []) :- at_end_of_stream(Stream).
read_samples(Stream, [Number|Numbers]) :- 
  \+ at_end_of_stream(Stream),
  read_line_to_string(Stream, Line),
  string_to_atom(Line, Atom),
  atom_number(Atom, Number),
  read_samples(Stream, Numbers).

read_models(Stream, []) :- 
  peek_char(Stream, end_of_file), !.
read_models(Stream, [Model|Models]) :- 
  read_term(Stream, Model, []),
    (   Model == end_of_file
    ->  Models = []
    ;   read_models(Stream, Models)
    ).

load_train_fold(Dir, Fold, Train) :-
  format(atom(DirectoryPath), '~w/fold_~w/train_models.txt', [Dir, Fold]),
  read_partition_file(DirectoryPath, Train).

load_test_fold(Dir, Fold, Test) :-
  format(atom(DirectoryPath), '~w/fold_~w/test_models.txt', [Dir, Fold]),
  read_partition_file(DirectoryPath, Test).


assert_all([],[]).

assert_all([H|T],[HRef|TRef]):-
  assertz(H,HRef),
  assert_all(T,TRef).

retract_all([]):-!.

retract_all([H|T]):-
  erase(H),
  retract_all(T).

run_param(_, _, [], [], [], [], [], []).
run_param(Dir, Program, [Fold|RestFold], [LPH|LPT],
         [AROCH|AROCT], [APRH|ARPT], [ROCH|ROCT], [PRH|PRT]) :- 
  load_train_fold(Dir, Fold, TrainFold),
  load_test_fold(Dir, Fold, TestFold),
  append(TrainFold, TestFold, AllF),
  assert(fold(train, TrainFold), TrainFoldRef),
  assert(fold(test, TestFold), TestFoldRef),
  assert(fold(all, AllF), AllFRef),
  format(atom(ModelPath), '~w/fold_~w/models.pl', [Dir, Fold]),
  format('Reading models from ~w~n', [ModelPath]),
  read_model_file(ModelPath, Models),
  assert_all(Models, ModelsRef),
  format('Read models~n'),
  % count postive and negative examples
  findall(relevant_gene(I, G, S), (fold(train, Train), relevant_gene(I, G, S), member(I, Train)), TrPos),
  findall(relevant_gene(I, G, S), (fold(test, Test), relevant_gene(I, G, S), member(I, Test)), TePos),
  findall(relevant_gene(I, G, S), (fold(train, Train), neg(relevant_gene(I, G, S)), member(I, Train)), TrNeg),
  findall(relevant_gene(I, G, S), (fold(test, Test), neg(relevant_gene(I, G, S)), member(I, Test)), TeNeg),
  % length(TrainFold, NTrain),
  % length(TestFold, NTest),
  % length(TrPos, TrNPos),
  % length(TePos, TeNPos),
  % length(TrNeg, TrNNeg),
  % length(TeNeg, TeNNeg),
  % format('Fold ~w: Train size: ~w, Test size: ~w~n', [Fold, NTrain, NTest]),
  % format('Fold ~w: Train Pos examples: ~w, Neg examples: ~w~n', [Fold, TrNPos, TrNNeg]),
  % format('Fold ~w: Test Pos examples: ~w, Neg examples: ~w~n', [Fold, TeNPos, TeNNeg]),
  assertz(in(Program), ProgRef),
  induce_par_lift([train], LPH),
  test_lift(LPH, [test], LL, AROCH, _, APRH, _), 
  compute_area_points(LPH, [test], ROCH, PRH),
  retract_all(ModelsRef),
  retract_all([TrainFoldRef]),
  retract_all([TestFoldRef]),
  retract_all([AllFRef]),
  run_param(Dir, Program, RestFold, LPT, AROCT, ARPT, ROCT, PRT).

run_param_learning(Dir, NumFolds, AUCROC, AUCPR, M_AUCROC, S_AUCROC, M_AUCPR, S_AUCPR) :-
  numlist(0, NumFolds, Folds),
  format('Loading Program~n'),
  in(Program),
  format('Running parameter learning~n'),
  run_param(Dir, Program, Folds, LP, AUCROC, AUCPR, ROC, PR), 
  mean(AUCROC, M_AUCROC),
  std_dev(AUCROC, S_AUCROC),
  mean(AUCPR, M_AUCPR),
  std_dev(AUCPR, S_AUCPR), 
  format(atom(RocPath), '~w/charts/roc_plot.png', [Dir]),
  format(atom(PrPath), '~w/charts/pr_plot.png', [Dir]),
  init_py,
  py_version,
  py_call(inference_util:plot_curves(ROC, PR, RocPath, PrPath), _RetVal).

convert_minus_pair_to_list(Key-Value, [Key, Value]).

compute_area_points(P, TestFolds, ROC, PR) :-
  test_prob_lift(P, TestFolds, _NPos, _NNeg, _, LG),
  findall(E,member(_- \+(E),LG),Neg),
  length(LG,NEx),
  length(Neg,NNeg),
  NPos is NEx-NNeg,
  keysort(LG,LG1),
  reverse(LG1,LG2),
  catch(compute_pointsroc(LG2,+1e20,0,0,NPos,NNeg,[],ROCPairs), 
    error(evaluation_error(zero_divisor),_), 
    ROC = []
  ), 
  compute_aucpr(LG2,NPos,NNeg,_,PRPair),
  maplist(convert_minus_pair_to_list, ROCPairs, ROC), 
  maplist(convert_minus_pair_to_list, PRPair, PR).

output(relevant_gene/2).

input(in_tad_region/2).
input(in_tad_with/2).
input(closest_gene/2).
input(within_k_distance/3).
input(find_and_rank_tfs/3).
input(load_tfbs_data/4).
input(overlaps_with/2).
input(binds_to/2).
input(regulatory_effect/2).
input(in_regulatory_region/2).
input(regulates/2).
input(pairs_values/2).
input(activity_by_contact/2).
input(eqtl_association/2).

input(gene/1).
input(snp/1).
input(chr/2).
input(start/2).
input(end/2).
input(tfbs_snp/2).
input(score/2).
input(alt/2).
input(ref/2).

input(gene_name/2).
