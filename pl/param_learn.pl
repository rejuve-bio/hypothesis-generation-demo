:- style_check(-discontiguous).
:- use_module(library(janus)).
:- use_module(library(clpfd)).
:- use_module(library(auc)).
:- use_module(library(plstat)).
:- use_module(library(http/json)).

:- include('train_rules.pl').

run_param(_, _, [], [], [], [], [], [], [], []).
run_param(Dir, Program, [Fold|RestFold], [LPH|LPT],
         [AROCH|AROCT], [APRH|ARPT], [ROCH|ROCT], [PRH|PRT], [Res|RRest], [Confs|ConfsRest]) :-
  get_time(FoldStart),
  format('Starting fold ~w at ~6f~n', [Fold, FoldStart]),
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
  length(TrainFold, NTrain),
  length(TestFold, NTest),
  length(TrPos, TrNPos),
  length(TePos, TeNPos),
  length(TrNeg, TrNNeg),
  length(TeNeg, TeNNeg),
  format('Fold ~w: Train size: ~w, Test size: ~w~n', [Fold, NTrain, NTest]),
  TrPosPerc is (TrNPos / (TrNPos + TrNNeg))*100,
  format('Fold ~w: Train Pos examples: ~w (~w %), Neg examples: ~w~n', [Fold, TrNPos, TrPosPerc, TrNNeg]),
  TestPosPerc is (TeNPos / (TeNPos + TeNNeg))*100,
  format('Fold ~w: Test Pos examples: ~w (~w %), Neg examples: ~w~n', [Fold, TeNPos, TestPosPerc, TeNNeg]),
  assertz(in(Program), ProgRef),
  induce_par_lift([train], LPH, Eta),
  % maplist([[E0, E1], Conf]>>(W is E0+E1, Conf is W/(W+1)), Eta, Confs),
  maplist([[_, E1], Conf]>>(Conf is E1/(E1+1)), Eta, Confs),
  forall(member([_, E1], Eta), format("Eta ~w~n", [E1])),
  format('Rule confidences:~w~n', [Confs]),
  % test_lift(LPH, [test], LL, AROCH, _, APRH, _),
  compute_area_points(LPH, [test], ROCH, PRH),
  format('About to run test_prob_lift~n'),
  test_prob_lift(LPH, [test], NPos, NNeg, _, Res),
  format('Ran test_prob_lift~n'),
  % Compute AUC-ROC using sklearn (trapezoidal) instead of convex hull
  maplist(res_to_label_score, Res, LabelScores),
  py_call(inference_util:sklearn_roc_auc(LabelScores), AROCH),
  py_call(inference_util:sklearn_pr_auc(LabelScores), APRH),
  format('Fold ~w metrics: AROC=~6f AUPRC=~6f~n', [Fold, AROCH, APRH]),
  retract_all(ModelsRef),
  retract_all([TrainFoldRef]),
  retract_all([TestFoldRef]),
  retract_all([AllFRef]),
  retract_all([ProgRef]),
  get_time(FoldEnd),
  FoldDuration is FoldEnd - FoldStart,
  format('Finished fold ~w in ~2f seconds~n', [Fold, FoldDuration]),
  run_param(Dir, Program, RestFold, LPT, AROCT, ARPT,
   ROCT, PRT, RRest, ConfsRest).

run_param_learning(Dir, NumFolds, AUCROC, AUCPR, M_AUCROC, S_AUCROC, M_AUCPR, S_AUCPR, Threshold, Results) :-
  get_time(TotalStart),
  format('Starting run_param_learning at ~6f~n', [TotalStart]),
  init_py,
  py_version,
  NFolds is NumFolds - 1,
  numlist(0, NFolds, Folds),
  format('Loading Program~n'),
  once(in(Program)),
  format('Running parameter learning~n'),
  run_param(Dir, Program, Folds, LPs, AUCROC, AUCPR, ROC, PR, Results, AllConfs),
  format('Done training~n'),
  mean(AUCROC, M_AUCROC),
  std_dev(AUCROC, S_AUCROC),
  mean(AUCPR, M_AUCPR),
  std_dev(AUCPR, S_AUCPR),
  format('~n=== Results ===~n'),
  format('AUCROC: ~4f +/- ~4f~n', [M_AUCROC, S_AUCROC]),
  format('AUCPR:  ~4f +/- ~4f~n', [M_AUCPR, S_AUCPR]),
  format('~n=== Per-Fold AUCROC ===~n'),
  maplist([Fold, Auc]>>format('  Fold ~w: ~6f~n', [Fold, Auc]), Folds, AUCROC),
  format('~n=== Per-Fold AUCPR ===~n'),
  maplist([Fold, Auc]>>format('  Fold ~w: ~6f~n', [Fold, Auc]), Folds, AUCPR),
  format('~n=== Per-Fold Learned Parameters ===~n'),
  print_per_fold_params(LPs, AllConfs, Folds),
  format('~n=== Average Learned Weights ===~n'),
  print_avg_weights(LPs),
  format('~n=== Average Confidences (Eta1-based) ===~n'),
  print_avg_confidences(LPs, AllConfs),
  retract(in(Program)),
  confusion_table(Results, Threshold, Dir, 0),
  format(atom(RocPath), '~w/charts/roc_plot.png', [Dir]),
  format(atom(PrPath), '~w/charts/pr_plot.png', [Dir]),
  py_call(inference_util:plot_curves(ROC, PR, RocPath, PrPath), _RetVal),
  get_time(TotalEnd),
  TotalDuration is TotalEnd - TotalStart,
  format('Total run_param_learning time: ~2f seconds~n', [TotalDuration]).

% Convert test_prob_lift result pair to [Label, Score] for sklearn
res_to_label_score(Prob - \+(_), [0, Prob]) :- !.
res_to_label_score(Prob - _, [1, Prob]).

is_pos(Lit) :- \+ Lit = neg(_).
predicted_label(P, Threshold, pos) :- P >= Threshold, !.
predicted_label(_, _, neg).

confusion_table([], _, _, _).
confusion_table([Fold|Rest], Threshold, Dir, FoldC) :-
  format('Confusion Fold: ~w~n', FoldC),
  format(atom(FilePath), '~w/fold_~w/confusion_tbl.txt', [Dir, FoldC]),
  setup_call_cleanup(
     open(FilePath, write, Stream),
     write_fold_confusion(Fold, Threshold, Stream),
     close(Stream)
  ),
  NextFold is FoldC + 1,
  confusion_table(Rest, Threshold, Dir, NextFold).

write_fold_confusion(Results, Threshold, Stream) :-
  confusion_counts(Results, Threshold, confusion(TP,FP,TN,FN)),
  findall(P-Lit, (member(P-Lit, Results), is_pos(Lit), P >= Threshold), PosPred),
  findall(P-H,   (member(P-(\+H), Results), P >= Threshold), NegPred),
  findall(P-Lit, (member(P-Lit, Results), is_pos(Lit), P <  Threshold), PosMiss),
  findall(P-H,   (member(P-(\+H), Results), P <  Threshold), NegMiss),
  format(Stream, 'Prediction Threshold=~w~n', [Threshold]),
  format(Stream, 'TP=~w, FP=~w, TN=~w, FN=~w~n', [TP, FP, TN, FN]),
  format(Stream, '============ TP ============~n', []),
  forall(member(P-Lit, PosPred),
         format(Stream, '~w ~w~n', [P, Lit])),
  format(Stream, '============ FP ============~n', []),
  forall(member(P-H, NegPred),
         format(Stream, '~w ~w~n', [P, H])),
  format(Stream, '============ TN ============~n', []),
  forall(member(P-H, NegMiss),
         format(Stream, '~w ~w~n', [P, H])),
  format(Stream, '============ FN ============~n', []),
  forall(member(P-Lit, PosMiss),
         format(Stream, '~w ~w~n', [P, Lit])),

  flush_output(Stream).

confusion_counts(Results, Threshold, confusion(TP,FP,TN,FN)) :-
  foldl(confusion_step(Threshold), Results, confusion(0, 0, 0, 0), 
      confusion(TP,FP,TN,FN)).

confusion_step(T, P-(\+H), confusion(TP0,FP0,TN0,FN0), confusion(TP,FP,TN,FN)) :-
    predicted_label(P, T, Pred),
    (   Pred == pos -> FP is FP0+1, TP=TP0,   TN=TN0,   FN=FN0
    ;   TN is TN0+1, TP=TP0, FP=FP0, FN=FN0).

confusion_step(T, P-H, confusion(TP0,FP0,TN0,FN0), confusion(TP,FP,TN,FN)) :-
    is_pos(H),
    predicted_label(P, T, Pred),
    (   Pred == pos -> TP is TP0+1, FP=FP0,   TN=TN0,   FN=FN0
    ;   FN is FN0+1, TP=TP0, FP=FP0, TN=TN0).

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

print_per_fold_params(LPs, AllConfs, Folds) :-
  maplist(print_single_fold_params(LPs, AllConfs), Folds, LPs, AllConfs).

print_single_fold_params(_AllLPs, _AllConfs, Fold, LP, Confs) :-
  format('--- Fold ~w ---~n', [Fold]),
  length(LP, NRules),
  N is NRules - 1,
  numlist(0, N, Indices),
  maplist(print_fold_rule(LP, Confs), Indices).

print_fold_rule(LP, Confs, Idx) :-
  nth0(Idx, LP, ((_:W ; _) :- Body)),
  nth0(Idx, Confs, C),
  format('  ~w: weight=~6f conf=~6f~n', [Body, W, C]).

print_avg_weights(LPs) :-
  LPs = [First|_],
  length(First, NRules),
  N is NRules - 1,
  numlist(0, N, Indices),
  maplist(print_avg_weight_aux(LPs), Indices).

print_avg_weight_aux(LPs, Idx) :-
  findall(W, (member(LP, LPs), nth0(Idx, LP, ((_:W ; _) :- _))), Weights),
  LPs = [First|_],
  nth0(Idx, First, ((_ ; _) :- Body)),
  mean(Weights, Mean),
  std_dev(Weights, Std),
  format('  ~w: ~4f +/- ~4f~n', [Body, Mean, Std]).

print_avg_confidences(LPs, AllConfs) :-
  LPs = [First|_],
  length(First, NRules),
  N is NRules - 1,
  numlist(0, N, Indices),
  maplist(print_avg_conf_aux(LPs, AllConfs), Indices).

print_avg_conf_aux(LPs, AllConfs, Idx) :-
  findall(C, (member(Confs, AllConfs), nth0(Idx, Confs, C)), ConfList),
  LPs = [First|_],
  nth0(Idx, First, ((_ ; _) :- Body)),
  mean(ConfList, Mean),
  std_dev(ConfList, Std),
  format('  ~w: ~4f +/- ~4f~n', [Body, Mean, Std]).

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
