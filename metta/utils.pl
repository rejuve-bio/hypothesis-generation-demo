:- dynamic relevant_gene_model/3.
:- dynamic neg/1.

ground_fact(Goal, A) :- 
     format('Goal ~w~n', Goal),
     (call(Goal)) -> A=True;A=False.

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

eval_pair(Index, gene(G), snp(S), 1) :- relevant_gene_model(Index, gene(G), snp(S)).
eval_pair(Index, gene(G), snp(S), 0) :- neg(relevant_gene_model(Index, gene(G), snp(S))).

cleanup :-
    retractall(relevant_gene_model(_, _, _)),
    retractall(neg(_)).

count_models(Dummy, Count) :-
  findall(I, eval_pair(I, Gene, Snp, 1), PosIdxs),
  findall(I, eval_pair(I, Gene, Snp, 0), NegIdxs),
  length(PosIdxs, PosCount),
  length(NegIdxs, NegCount),
  Count is PosCount + NegCount.


string_interpolate(String, Vars, Output) :-
    format(atom(Output), String, Vars).

assert_all([],[]).

assert_all([H|T],[HRef|TRef]):-
  assertz(H,HRef),
  assert_all(T,TRef).

retract_all([]):-!.

retract_all([H|T]):-
  erase(H),
  retract_all(T).