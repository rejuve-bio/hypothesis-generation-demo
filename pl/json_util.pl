:- use_module(library(http/json)).
:- use_module(library(option)).

:- multifile t/3.
:- multifile proof/1.

is_true_term(t(true, true, [])).

%Predicate to extract terms that do not contain variables or 'hideme(_)'
extract_ground_terms(Term, List) :-
    extract_terms(Term, AllTerms),
    exclude(contains_var_or_hideme, AllTerms, FilteredTerms),
    exclude(is_true_term, FilteredTerms, List).

%Recursive helper predicate to extract all terms
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

is_true_term(t(true, true, [])).

flatten([], []).
flatten([L|Ls], Flat) :-
    flatten(L, NewL),
    flatten(Ls, NewLs),
    append(NewL, NewLs, Flat).
flatten(L, [L]).



extract_nodes_edges([], [], []).
extract_nodes_edges([Term|TermsTail], [Subject, Object|NodesTail], [Edge|Edges]) :-
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


create_json_graph([_|Terms], JSONGraph) :-
    extract_nodes_edges(Terms, NodesList, EdgesList),
    sort(NodesList, SortedNodes),
    sort(EdgesList, SortedEdges),
    node_to_json(SortedNodes, NodesJSON),
    edge_to_json(SortedEdges, EdgesJSON),
    JSONGraph = json([nodes=NodesJSON, edges=EdgesJSON]).


json_proof_tree([], NumSamples, []).

json_proof_tree([failure-_|T], NumSamples, Graph) :- !,
    json_proof_tree(T, NumSamples, Graph).

json_proof_tree([Proof-Count|T], NumSamples, [Graph|TGraph]) :- !,
    json_proof_tree(Proof-Count, NumSamples, Graph),
    json_proof_tree(T, NumSamples, TGraph).

json_proof_tree(Proof-Count, NumSamples, Graph) :-
  extract_ground_terms(Proof, Terms),
  create_json_graph(Terms, JsonGraph),
  JsonGraph = json([nodes=Nodes, edges=Edges]),
  Prob is Count / NumSamples,
  UpdatedGraph = json([nodes=Nodes, edges=Edges, prob=json([value=Prob])]),
  atom_json_term(Graph, UpdatedGraph, []).