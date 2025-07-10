:- use_module(library(janus)).
:- use_module(library(liftcover)).
% :- use_module(library(slipcover)).

% :- sc.
% :- set_sc(verbosity, 3).
% :- set_sc(iter, 30).
% :- set_sc(random_restarts, 5).
% :- set_sc(epsilon_em, 0.001).
% :- set_sc(neg_ex, given).

:- lift.

:- set_lift(verbosity, 3).
:- set_lift(neg_ex, given).

% :- begin_bg.

person(ann).
person(bob).
person(carl).
person(dana).
person(ed).

stressed(ann).
stressed(carl).

influences(ann, bob).   % Ann influences Bob
influences(carl, dana). % Carl influences Dana
influences(bob, dana).   % Bob also influences Dana

init_py :-
    py_add_lib_dir('/home/abdu/bio_ai/code/hypothesis-generation-demo').

builtin(py_call).

pass(X) :-
    format(user_error, 'X ~w~n', [X]),
    % init_py,
    format(user_error, 'Python initialized~n', []),
    py_call(inference_util:mock_pred(X), Y),
    format(user_error, 'X ~w, Y ~w~n', [X, Y]),
    X = Y.

bgc(person(X)) :- person(X).
bgc(stressed(X)) :- stressed(X).
bgc(influences(X, Y)) :- influences(X, Y).

% :- end_bg.

:- begin_in.
% Rule 1: Stress leads to smoking with probability p1
smokes(X) : 0.6 :- stressed(X).

% Rule 2: Being influenced leads to smoking with probability p2
% smokes(X) : 0.3 :- influences(Y, X).
smokes(X) : 0.3 :- pass(X).

:- end_in.

fold(train, [1, 2, 3, 4]).
fold(test, []).

fold(all, F) :-
    fold(train, FTr),
    fold(test, FTe),
    append(FTr, FTe, F).

output(smokes/1).

input(person/1).
input(stress/1).
input(influences/2).
input(pass/1).
% input(init_py/0).
% input(py_add_lib_dir/1).
input(py_call/2).

%consult('pl/smokes.pl').

begin(model(1)).
smokes(ann).
end(model(1)).
begin(model(2)).
smokes(bob).
end(model(2)).
begin(model(3)).
smokes(dana).
end(model(3)).
begin(model(4)).
neg(smokes(carl)).
end(model(4)).
begin(model(5)).
neg(smokes(ed)).
end(model(5)).
