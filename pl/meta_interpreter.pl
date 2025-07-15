% :- module(meta_interpreter, [
%     prooftree/2,
%     json_proof_tree/2,
%     rule_body/2,
%     mi/2
%   ]).

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

mi(hideme([Goal|Goals]), t(hideme, hideme, [])) :- !,
  hideme([Goal|Goals]).

mi(hideme(A), t(hideme, hideme, [])) :- !,
  call(A).

mi((A, B), t(and, C, [PA, PB])) :- !, %conjuction
    copy_term(and(A, B), C),
    mi(A, PA), mi(B, PB).

mi((A;B), Proof) :- !,
    findall(ProofA, mi(A, ProofA), ProofAs),
    findall(ProofB, mi(B, ProofB), ProofBs),
    (ProofAs = [ProofA], ProofBs = [ProofB] -> Proof = t(or, _, [ProofA, ProofB]) ;
     ProofAs = [ProofA] -> Proof = ProofA ;
     ProofBs = [ProofB] -> Proof = ProofB).

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

library_predicate(Goal, Library) :-
   current_predicate(Goal, Pred),
    predicate_property(Pred, imported_from(Module)),
    Module = Library, % don't track library predicates.

mi(G, t(lib, lib, [P])) :-
   G \= true,
   library(G, mcintyre),
   


mi(G, t(built_in, G, [])) :- % Check if the goal is a built-in predicate.
    G \= true,
    G \= findall(_, _, _),
    G \= hideme(_),
    (predicate_property(G, built_in) ; %or
     library_predicate(G, mcintyre) ; % add more libraries here
    \+ predicate_property(G,number_of_clauses(_))), !,
    call(G). % Directly call the built-in predicate.


mi(G, t(R, G, [P])) :-
    G \= true,
    G \=  (_,_),
    G \= (_;_),
    G \= hideme(_),
    \+ library_predicate(G, mcintyre),
    % format("G: ~w ~n", [G]),
    clause(G, Body, Ref), 
    clause(HeadC, BodyC, Ref),
    without_hidden(BodyC, BodyF),
    copy_term(HeadC :- BodyF, R),
    % copy_term(HeadC :- BodyC, R),
    mi(Body, P).

hideme([]) :- !.
hideme([Goal|Goals]) :- !,
  call(Goal),
  hideme(Goals).


execute_hidden_goal((A;B)) :- !,
    (execute_hidden_goal(A) ; execute_hidden_goal(B)).

execute_hidden_goal((A,B)) :- !,
  execute_hidden_goal(A),
  execute_hidden_goal(B).

execute_hidden_goal(G) :- 
  G \= (_;_),
  G \= (_,_),
  call(G).

% Base case: if there's no body (empty clause), succeed with empty body
without_hidden(true, true) :- !.

% Handle hideme by removing it completely
without_hidden(hideme(_), true) :- !.

% Handle hideme with list
without_hidden(hideme([_|_]), true) :- !.

% Handle conjunctions - process each part
without_hidden((A,B), FilteredBody) :- !,
    without_hidden(A, FA),
    without_hidden(B, FB),
    simplify_body(FA, FB, FilteredBody).

% Handle disjunctions
without_hidden((A;B), FilteredBody) :- !,
    without_hidden(A, FA),
    without_hidden(B, FB),
    simplify_body_or(FA, FB, FilteredBody).

% Keep non-hideme goals
without_hidden(G, G) :- 
    G \= hideme(_),
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


% Predicate to extract terms that do not contain variables or 'hideme(_)'
extract_ground_terms(Term, List) :-
    extract_terms(Term, AllTerms),
    exclude(contains_var_or_hideme, AllTerms, FilteredTerms),
    exclude(is_true_term, FilteredTerms, List).

% Recursive helper predicate to extract all terms
extract_terms(true, []) :- !.
extract_terms(t(hideme, _, _), []) :- !.
extract_terms(t(true, true, []), []) :- !.
extract_terms(t(built_in, G, []), [G]) :- !.
extract_terms(t(and, _, SubProof), Terms) :- !,
    maplist(extract_terms, SubProof, SubTerms),
    flatten(SubTerms, Terms).

extract_terms(t(or, _, SubProof), Terms) :- !,
    maplist(extract_terms, SubProof, SubTerms),
    flatten(SubTerms, Terms).
    
extract_terms(t(_, C, SubProof), Terms) :- !,
    extract_terms(C, Term),
    maplist(extract_terms, SubProof, SubTerms),
    flatten(SubTerms, FlatSubTerms),
    append(Term, FlatSubTerms, Terms).
    
extract_terms(Term, [Term]) :- !.

% Helper predicate to check if a term contains variables or 'hideme(_)'
contains_var_or_hideme(Term) :-
    term_variables(Term, Vars),
    Vars \= [],
    !.
contains_var_or_hideme(Term) :-
    compound(Term),
    Term =.. [hideme|_],
    !.
contains_var_or_hideme(Term) :-
    compound(Term),
    Term =.. [_|Args],
    maplist(contains_var_or_hideme, Args).

% Helper predicate to check if a term is 't(true, true, [])'
is_true_term(t(true, true, [])).

% Flatten a list of lists
% Predicate to flatten a list of lists
flatten([], []).
flatten([L|Ls], Flat) :-
    flatten(L, NewL),
    flatten(Ls, NewLs),
    append(NewL, NewLs, Flat).
flatten(L, [L]).


% Predicate to extract nodes and edges
extract_nodes_edges([], [], []).
extract_nodes_edges([Term|TermsTail], [Subject, Object|NodesTail], [Edge|Edges]) :-
    % Term =.. [Relation, Subject, Object],
    functor(Term, Relation, Arity), 
    Arity >= 2, 
    arg(1, Term, Subject),
    arg(2, Term, Object),
    Edge = [label(Relation), source(Subject), target(Object)],
    extract_nodes_edges(TermsTail, NodesTail, Edges).

node_to_json([], []).
node_to_json([Node|Nodes], [json([id=NodeId, type=Type])|NodesJSON]) :-
  functor(Node, Type, Arity), 
  Arity is 1,
  arg(1, Node, NodeId),
  node_to_json(Nodes, NodesJSON).


edge_to_json([], []).
edge_to_json([[label(Relation), source(Subject), target(Object)]|Edges], 
            [json([source=Source, target=Target, 
                                label=Relation])|EdgesJSON]) :-

      arg(1, Subject, Source),
      arg(1, Object, Target),
      edge_to_json(Edges, EdgesJSON).


% Predicate to create JSON graph
create_json_graph([_|Terms], JSONGraph) :-
    extract_nodes_edges(Terms, NodesList, EdgesList),
    node_to_json(NodesList, NodesJSON),
    edge_to_json(EdgesList, EdgesJSON),
    JSONGraph = json([nodes=NodesJSON, edges=EdgesJSON]).

prooftree(A, PT):-
  mi(A, PT),
  numbervars(PT).

json_proof_tree(A, Graph) :-
  prooftree(A, Proof),
  extract_ground_terms(Proof, Terms),
  create_json_graph(Terms, JsonGraph),
  atom_json_term(Graph, JsonGraph, []).
  % tmp_file_stream(text, File, Out), 
  % json_write(Out, JsonGraph), 
  % close(Out),
  % read_file_to_string(File, Graph, []).

rule_body(R, RB) :-
  clause(relevant_gene(G, S), Body, Ref),
  clause(HeadC, BodyC, Ref),
  copy_term(HeadC :- BodyC, Term),
  numbervars(Term),
  RB = "$Term".

% :- use_module(pengine_sandbox:library(meta_interpreter)).
:- use_module(library(sandbox)).

:- multifile sandbox:safe_primitive/1.

sandbox:safe_primitive(meta_interpreter:prooftree(_,_)).
sandbox:safe_primitive(meta_interpreter:json_proof_tree(_,_)).
sandbox:safe_primitive(meta_interpreter:rule_body(_,_)).
sandbox:safe_primitive(meta_interpreter:mi(_,_)).
