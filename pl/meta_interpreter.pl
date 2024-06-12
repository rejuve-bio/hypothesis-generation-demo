:- style_check(-discontiguous).
:- op(500, xfy, =>).
:- use_module(library(clpfd)).

:- use_module(library(apply)).
:- use_module(library(gv)).
:- use_module(library(yall)).
:- use_module(library(option)).
:- use_module(library(interpolate)).
:- use_module(library(term_ext)).
:- use_module(library(process)).
:- use_module(library(readutil)).


t(_,_,_).

not_var(X) :- \+ var(X).

view_proof(Proof) :-
  gv_view({Proof}/[Out0]>>export_proof(Out0, Proof), options{directed: true}).

write_proof(File, Proof, Options) :-
    option(format(Format), Options, json),
    gv_export(File, {Proof}/[Out0]>>export_proof(Out0, Proof), [format(Format), directed(true)]).

json_proof(A, PT) :-
    prooftree(A, Proof),
    tmp_file_stream(text, File, Out),
    gv_export(File, {Proof}/[Out0]>>export_proof(Out0, Proof), [format(json), directed(true)]),
    close(Out),
    read_file_to_string(File, PT, []).

prooftree(A, PT):-
  mi(A, PT),
  numbervars(PT).

export_proof(Out, t(true, true, _)) :- !.
export_proof(Out, t(hideme, hideme, [])) :- !.
export_proof(Out, t(built_in, G, [])) :- !,
    dot_node(Out, G),
    dot_node(Out, built_in),
    dot_arc(Out, G, built_in).

export_proof(Out, t(and, C, SubProofs)) :- !,
  dot_node(Out, C, [label(and)]),
  maplist(export_subproof(Out, C), SubProofs).

export_proof(Out, t(or, C, SubProofs)) :- !,
  include(not_var, SubProofs, GProofs),
  ((length(GProofs, L), L > 1)
  -> (dot_node(Out, C, [label(or)]), 
      maplist(export_subproof(Out, C), GProofs))
    ; (GProofs = [P], 
      export_proof(Out, P))).

% export_proof(Out, t(or, C, SubProofs)) :- !,
%   ((length(SubProofs, L), L > 1)
%   -> (dot_node(Out, C, [label(or)]), 
%      maplist(export_subproof(Out, C), SubProofs))
%   ; (
%     SubProofs = [P],
%     export_proof(Out, P)
%   )).

export_proof(Out, Proof) :-
  Proof = t(Rule,Concl,SubProofs),
  dot_node(Out, Concl),
  dot_node(Out, Proof, [label(Rule)]),
  dot_arc(Out, Concl, Proof),
  maplist(export_subproof(Out, Proof), SubProofs).

export_subproof(Out, Proof, t(hideme, hideme, [])) :- !.
export_subproof(Out, Proof, []) :- !.
export_subproof(Out, and, t(and, C, SubProof)) :- !,
  dot_node(Out, C, [label(and)]),
  dot_arc(Out, Proof, C),
  export_proof(Out, SubProof).

export_subproof(Out, Proof, SubProof) :-
  SubProof = t(_,Concl,_),
  dot_node(Out, Concl),
  dot_arc(Out, Proof, Concl),
  export_proof(Out, SubProof).

mi(true, t(true, true, [])) :- !.

mi((hideme(A), B), PB) :- 
  call(A), mi(B, PB).

mi((A, hideme(B)), PA) :- 
  mi(A, PA), call(B).

mi((A, B), t(and, C, [PA, PB])) :- !, %conjuction
    copy_term(and(A, B), C),
    mi(A, PA), mi(B, PB).

%TODO Fix Me!
mi((A;B), t(or, C, [PA, PB])) :- !, %disjunction
    copy_term(or(A, B), C),
    (mi(A, PA)
    ; mi(B, PB)).

% mi((_;B), t(or, C, [PB])) :- !,
%     copy_term(or_right(A, B), C),
%     mi(B, PB).


mi(findall(X, G, Ls), t(R, C, SP)) :- !,
    findall(t(G, X, [P]), mi(G, P), Xs),
    C = "find all $G",
    Xs = [Proof|_], 
    Proof = t(_, _, SP),
    % format("Proof - ~w", [SP]),
    maplist(arg(2), Xs, Ls),
    Ls = [A1, A2, A3|_], 
    R = "$A1, $A2, $A3,...".

mi(subset(X, Xs), t(R, C, [])) :- !,
  subset(X, Xs),
  Xs = [Xs1, Xs2, Xs3|_],
  X = [X1, X2|_],
  C = "subset_of({$X1, $X2,...}, {$Xs1, $Xs2, $Xs3...})",
  R = "{$X1, $X2,...}".

mi(hideme(X), t(hideme, hideme, [])) :- !,
   call(X).


mi(G, t(built_in, G, [])) :- % Check if the goal is a built-in predicate.
    G \= true,
    G \= findall(_, _, _),
    G \= hideme(_),
    % G \= relevant_gene_coexpression(_, _),
    (predicate_property(G, built_in) ; %or
    \+ predicate_property(G,number_of_clauses(_))), !,
    % functor(G, C, _),
    % format("Built in Gs: ~w \n", [G]),
    call(G). % Directly call the built-in predicate.


mi(G, t(R, G, [P])) :-
    G \= true,
    G \=  (_,_),
    G \= (_;_),
    G \= hideme(_),
    clause(G, Body, Ref), 
    clause(HeadC, BodyC, Ref),
    without_hidden(BodyC, BodyF),
    copy_term(HeadC :- BodyF, R), 
    mi(Body, P).

without_hidden((hideme(_), hideme(_)), true) :- !.
without_hidden((X, hideme(_)), WX) :-
  without_hidden(X, WX).
without_hidden((hideme(_), X), WX) :- 
  without_hidden(X, WX).
without_hidden((X, Y), (WX, WY)) :- 
    X \= hideme(_),
    Y \= hideme(_),
    without_hidden(X, WX), 
    without_hidden(Y, WY).

without_hidden(X, X) :-
  X \= (_,_),
  X \= hideme(_).

% explanation_of(true, true) :- !.
%  %S is a fact
% % explanation_of(S, S) :- !,
% %   S \= true,
% %   S \= (_,_),
% %   clause(S, Body),
% %   Body = true.

% explanation_of(explanation(E), E) :- !.

% explanation_of(S, E) :- !,
%   S \= true,
%   S \= (_,_),
%   clause(S, Body),
%   Body \= true,
%   Body \= (_,_),
%   Body = explanation(E).

% explanation_of(S, E) :- !,
%   S \= true,
%   S \= (_,_),
%   clause(S, Body),
%   Body = (X, Y),
%   explanation_of_((X,Y), E).

% explanation_of_(S, E) :- !,
%   S \= true,
%   S \= (_,_),
%   S = explanation(E).

% explanation_of_((S1, S2), E) :-
%   (S1 = explanation_of_(E)
%   -> S1 = explanation(E)
%   ;explanation_of_(S2, E)).
