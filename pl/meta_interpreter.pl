:- style_check(-discontiguous).
:- op(500, xfy, =>).
:- use_module(library(clpfd)).

:- use_module(library(apply)).
:- use_module(library(gv)).
:- use_module(library(yall)).
:- use_module(library(term_ext)).
:- use_module(library(process)).
:- use_module(library(readutil)).

:- dynamic t/3.


not_var(X) :- \+ var(X).

view_proof(Proof) :-
  gv_view({Proof}/[Out0]>>export_proof(Out0, Proof), options{directed: true}).

write_proof(File, Proof, Options) :-
    option(format(Format), Options, json),
    gv_export(File, {Proof}/[Out0]>>export_proof(Out0, Proof), [format(Format), directed(true)]).


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

mi(hideme([Goal|Goals]), t(hideme, hideme, [])) :- !,
  hideme([Goal|Goals]).

mi(hideme(A), t(hideme, hideme, [])) :- !,
  call(A).

mi((A,B), t(and, C, [PA, PB])) :- !, %conjuction
    without_hidden(A, WA),
    without_hidden(B, WB),
    copy_term(and(WA, WB), C),
    mi(A, PA), mi(B, PB).

mi((A;B), Proof) :- !,
    findall(ProofA, mi(A, ProofA), ProofAs),
    findall(ProofB, mi(B, ProofB), ProofBs),
    (ProofAs = [ProofA], ProofBs = [ProofB] -> Proof = t(or, _, [ProofA, ProofB]) ;
     ProofAs = [ProofA] -> Proof = ProofA ;
     ProofBs = [ProofB] -> Proof = ProofB).

traced_predicate(Goal) :-
  nonvar(Goal),
  Goal \= (_,_),
  Goal \= (_;_),
  functor(Goal, Name, Arity),
  Name \== sampled,
  predicate_property(hypgen:Goal, implementation_module(hypgen)).
  
mi(G, t(hideme, hideme, [])) :-
  G = sample_head(_,_,_,_,_), !,
  call(hypgen:G).

mi(G, t(built_in, G, [])) :- % Check if the goal is a built-in predicate.
    G \= true,
    G \= findall(_, _, _),
    G \= hideme(_),
    \+ traced_predicate(G), !,
    call(hypgen:G). % Directly call the built-in predicate.


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

hideme([]) :- !.
hideme([Goal|Goals]) :- !,
  call(Goal),
  hideme(Goals).

proof_tree(A, PT):-
  mi(A, PT),
  numbervars(PT).

proof_tree(Goal, NumSamples, Graph) :-
  mcintyre:mc_sample_arg_first(hypgen:mi(Goal, Proof), NumSamples, Proof, Values), 
  json_proof_tree(Values, NumSamples, Graph).

% Base case: if there's no body (empty clause), succeed with empty body
without_hidden(true, true) :- !.

% Handle hideme by removing it completely
without_hidden(hideme(_), true) :- !.

% Handle hideme with list
without_hidden(hideme([_|_]), true) :- !.

without_hidden(G, true) :- 
  functor(G, sample_head, 5),!.


without_hidden((A,B), FilteredBody) :- !,
    without_hidden(A, FA),
    without_hidden(B, FB),
    simplify_body(FA, FB, FilteredBody).

without_hidden((A;B), FilteredBody) :- !,
    without_hidden(A, FA),
    without_hidden(B, FB),
    simplify_body_or(FA, FB, FilteredBody).

without_hidden(G, G) :- 
    G \= hideme(_),
    \+ functor(G, sample_head, 5),
    G \= (_,_),
    G \= (_;_).

% Helper to simplify conjunctive bodies
simplify_body(true, B, B) :- !.
simplify_body(A, true, A) :- !.
simplify_body(A, B, (A,B)).

% Helper to simplify disjunctive bodies
simplify_body_or(true, B, B) :- !.
simplify_body_or(A, true, A) :- !.
simplify_body_or(A, B, (A;B)).

% Predicate to convert an 'and' compound clause to a list of terms
and_to_list(and(Term1, Term2), List) :-
    and_to_list(Term1, List1),
    and_to_list(Term2, List2),
    append(List1, List2, List).
and_to_list(Term, [Term]).

rule_body(R, RB) :-
  clause(relevant_gene(G, S), Body, Ref),
  clause(HeadC, BodyC, Ref),
  copy_term(HeadC :- BodyC, Term),
  numbervars(Term),
  RB = "$Term".
