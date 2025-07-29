:- style_check(-discontiguous).
:- dynamic(user:file_search_path/2).
:- multifile(user:file_search_path/2).

:- dynamic seen_predicate/3.

%Define term_expansion/2 to process terms as they are read
term_expansion(Head, [:- multifile(Declaration), :- discontiguous(Declaration), Head]) :-
    Head \= begin_of_file,
    Head \= end_of_file,
    Head \= :-(_),
    prolog_load_context(source, Source),
    functor(Head, Name, Arity),
    (   seen_predicate(Source, Name, Arity) ->
        false
    ;   assertz(seen_predicate(Source, Name, Arity)), 
        Declaration = [Name/Arity]
            ).
