:- module(meta_intepreter, [
    proof_tree/2,
    proof_tree/2,
    json_proof_tree/2
  ]).

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
:- use_module(library(http/json)).

t(_,_,_).

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

mi((hideme(A), B), PB) :- !,
mi((hideme(A), B), PB) :- !,
  call(A), mi(B, PB).

mi(hideme((A, B)), t(hideme, hideme, [])) :- !,
  A, B.

mi((A, hideme(B)), PA) :- !,
mi(hideme((A, B)), t(hideme, hideme, [])) :- !,
  A, B.

mi((A, hideme(B)), PA) :- !,
  mi(A, PA), call(B).

mi(hideme(X), t(hideme, hideme, [])) :- !,
   call(X).

mi((A, B), t(and, C, [PA, PB])) :- !, %conjuction
    copy_term(and(A, B), C),
    mi(A, PA), mi(B, PB).

%TODO Fix Me!
% mi((A;B), t(or, C, [PA, PB])) :- !, %disjunction
%     copy_term(or(A, B), C),
%     (mi(A, PA)
%     ; mi(B, PB)).

% mi((A;_), t(or_left, C, [PA])) :- !,
%   mi(A, PA).

% mi((_;B), t(or_right, C, [PB])) :- !,
%     mi(B, PB).


mi((A;B), t(or, _, Proof)) :- !,
    findall(ProofA, mi(A, ProofA), ProofAs),
    findall(ProofB, mi(B, ProofB), ProofBs),
    (ProofAs = [ProofA], ProofBs = [ProofB] -> Proof = [ProofA, ProofB] ;
     ProofAs = [ProofA] -> Proof = [ProofA] ;
     ProofBs = [ProofB] -> Proof = [ProofB]).
    (ProofAs = [ProofA], ProofBs = [ProofB] -> Proof = [ProofA, ProofB] ;
     ProofAs = [ProofA] -> Proof = [ProofA] ;
     ProofBs = [ProofB] -> Proof = [ProofB]).

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

mi(G, t(built_in, G, [])) :- % Check if the goal is a built-in predicate.
    G \= true,
    G \= findall(_, _, _),
    G \= hideme(_),
    (predicate_property(G, built_in) ; %or
    \+ predicate_property(G,number_of_clauses(_))), !,
    call(G). % Directly call the built-in predicate.


mi(G, t(R, G, [P])) :-
    G \= true,
    G \=  (_,_),
    G \= (_;_),
    G \= hideme(_),
    clause(G, Body, Ref), 
    clause(HeadC, BodyC, Ref),
    % without_hidden(BodyC, BodyF),
    % without_hidden(BodyC, BodyF),
    copy_term(HeadC :- BodyF, R), 
    mi(Body, P).

rule_body(R, RB) :-
  clause(relevant_gene(G, S), Body, Ref), 
  clause(HeadC, BodyC, Ref), 
  copy_term(HeadC :- BodyC, Term), 
  numbervars(Term),
  RB = "$Term".

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

% Predicate to convert an 'and' compound clause to a list of terms
%and_to_list(and(Term1, Term2), List) :-
%    and_to_list(Term1, List1),
%    and_to_list(Term2, List2),
%    append(List1, List2, List).
%and_to_list(Term, [Term]).


% Predicate to extract terms that do not contain variables or 'hideme(_)'
extract_ground_terms(Term, List) :-
    extract_terms(Term, List).
    %extract_terms(Term, AllTerms),
    %exclude(contains_var_or_hideme, AllTerms, FilteredTerms),
    %exclude(is_true_term, FilteredTerms, List).

% Recursive helper predicate to extract all terms
extract_terms(true, []) :- !.
%extract_terms(hideme(_), []) :- !.
extract_terms(t(true, true, []), []) :- !.
extract_terms(t(hideme, hideme, []), []) :- !.
extract_terms(t(hideme, hideme, []), []) :- !.
extract_terms(t(built_in, G, []), [G]) :- !.
extract_terms(t(and, _, SubProof), Terms) :- !,
    maplist(extract_terms, SubProof, SubTerms),
    flatten(SubTerms, Terms).

extract_terms(t(or, _, SubProof), Terms) :- !,
    maplist(extract_terms, SubProof, SubTerms),
    flatten(SubTerms, Terms).

%extract_terms(t(or_right, _, SubProof), Terms) :- !,
%    maplist(extract_terms, SubProof, SubTerms),
%    flatten(SubTerms, Terms).
    
extract_terms(t(_, C, SubProof), Terms) :- !,
    extract_terms(C, Term),
    maplist(extract_terms, SubProof, SubTerms),
    flatten([Term|SubTerms], Terms),
    %append(Term, FlatSubTerms, Terms).
    
extract_terms(Term, [Term]) :- !.

% Helper predicate to check if a term contains variables or 'hideme(_)'
%contains_var_or_hideme(Term) :-
%    term_variables(Term, Vars),
%    Vars \= [],
%    !.
%contains_var_or_hideme(Term) :-
%    compound(Term),
%    Term =.. [hideme|_],
%    !.
%contains_var_or_hideme(Term) :-
%    compound(Term),
%    Term =.. [_|Args],
%    maplist(contains_var_or_hideme, Args).

% Helper predicate to check if a term is 't(true, true, [])'
%is_true_term(t(true, true, [])).

% Flatten a list of lists
% Predicate to flatten a list of lists
%flatten([], []).
%flatten([L|Ls], Flat) :-
%    flatten(L, NewL),
%    flatten(Ls, NewLs),
%    append(NewL, NewLs, Flat).
%flatten(L, [L]).


% Predicate to extract nodes and edges
extract_nodes_edges([], [], []).
extract_nodes_edges([Term|Terms], Nodes, Edges) :-
    (Term =.. [Relation, Subject, Object]
    ; Term =.. [Relation, Subject, Object, _]), %Todo handle edge properties
    extract_nodes_edges(Terms, NodesTail, EdgesTail),
    sort([Subject, Object | NodesTail], Nodes), % Remove duplicates
    Edges = [[label(Relation), source(Subject), target(Object)] | EdgesTail].

node_to_json(Node, json([id=NodeId, type=Type])) :-
  Node =.. [Type, NodeId].

edge_to_json([label(Relation), source(Subject), target(Object)], json([source=Source, target=Target, label=Relation])) :-
edge_to_json([label(Relation), source(Subject), target(Object)], json([source=Source, target=Target, label=Relation])) :-
  Subject =.. [_, Source],
  Object =.. [_, Target].

% Predicate to create JSON graph
create_json_graph([_|Terms], JSONGraph) :-
    extract_nodes_edges(Terms, NodesList, EdgesList),
    maplist(node_to_json, NodesList, NodesJSON),
    maplist(edge_to_json, EdgesList, EdgesJSON),
    JSONGraph = json([nodes=NodesJSON, edges=EdgesJSON]).

proof_tree(A, PT):-
proof_tree(A, PT):-
  mi(A, PT),
  numbervars(PT).

% json_proof_tree(A, PT) :-
%   prooftree(A, Proof),
%   tmp_file_stream(text, File, Out),
%   gv_export(File, {Proof}/[Out0]>>export_proof(Out0, Proof), [format(json), directed(true)]),
%   close(Out),
%   read_file_to_string(File, PT, []).

json_proof_tree(A, Graph) :-
  proof_tree(A, Proof),
  proof_tree(A, Proof),
  extract_ground_terms(Proof, Terms),
  create_json_graph(Terms, JsonGraph),
  atom_json_term(Graph, JsonGraph, []).

% :- use_module(pengine_sandbox:library(meta_intepreter)).
:- use_module(library(sandbox)).

:- multifile sandbox:safe_primitive/1.

sandbox:safe_primitive(meta_intepreter:proof_tree(_,_)).
sandbox:safe_primitive(meta_intepreter:proof_tree(_,_)).
sandbox:safe_primitive(meta_intepreter:json_proof_tree(_,_)).
sandbox:safe_primitive(meta_intepreter:rule_body(_,_)).