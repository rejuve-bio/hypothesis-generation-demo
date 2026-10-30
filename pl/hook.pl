:- style_check(-discontiguous).
:- dynamic(user:file_search_path/2).
:- multifile(user:file_search_path/2).

:- dynamic seen_predicate/3.

ensure_dynamic_arity(Space,Arity) :- ( current_predicate(Space/Arity)
                                       -> true ; dynamic(Space/Arity) ).



% Select the expansion mode when this file is loaded.
:- if(getenv('PETTA_ATOMSPACE', '1')).

term_expansion(Head, Out) :-
    Head \= begin_of_file,
    Head \= end_of_file,
    Head \= :-(_),
    prolog_load_context(source, Source),
    Head =.. [Name | Args],
    length(Args, N),
    (seen_predicate(Source, Name, N) ->
      Decls = []
    ; assertz(seen_predicate(Source, Name, N)),
      Decls = [:- multifile([Name/N]), :- discontiguous([Name/N])]),
    atom_string(Space, "&self"),
    Arity is N + 1,
    (seen_predicate(Source, Space, Arity) ->
      Decls2 = Decls
    ; append(Decls, [:- multifile(Space/Arity), :- dynamic(Space/Arity), :- discontiguous([Space/Arity])], Decls2),
      assertz(seen_predicate(Source, Space, Arity))
    ),
    Term =.. [Space, Name | Args], 
    append(Decls2, [Term], Out).

  :- else.

  term_expansion(Head, [:- multifile(Declaration), :- discontiguous(Declaration), Head]) :-
    Head \= begin_of_file,
    Head \= end_of_file,
    Head \= :-(_),
    prolog_load_context(source, Source),
    Head =.. [Name | Args],
    length(Args, Arity),
    (seen_predicate(Source, Name, Arity) ->
      false
    ;   assertz(seen_predicate(Source, Name, Arity)),
      Declaration = [Name/Arity]
    ).

  :- endif.
