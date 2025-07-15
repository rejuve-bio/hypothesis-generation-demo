:- style_check(-discontiguous).
:- dynamic(user:file_search_path/2).
:- multifile(user:file_search_path/2).

:- dynamic seen_predicate/3.

%:- dynamic sc_input_mod/1.

%:- thread_local sc_input_mod/1.
%:- thread_local sc_file/1.


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
%user:term_expansion(In, Out) :-
%  \+ current_prolog_flag(xref, true),
%  sc_file(Source),
%  prolog_load_context(source, Source),
%  sc_expansion(In, Out).
%
%
%sc_expansion((:- begin_bg), []) :-
%  prolog_load_context(module, M),
%  sc_input_mod(M),!,
%  assert(M:bg_on).
%
%sc_expansion(C, M:bgc(C)) :-
%  prolog_load_context(module, M),
%  C\= (:- end_bg),
%  sc_input_mod(M),
%  M:bg_on,!.
%
%sc_expansion((:- end_bg), []) :-
%  prolog_load_context(module, M),
%  sc_input_mod(M),!,
%  retractall(M:bg_on),
%  findall(C,M:bgc(C),L),
%  % retractall(M:bgc(_)),
%  (M:bg(BG0)->
%    retract(M:bg(BG0)),
%    append(BG0,L,BG),
%    assert(M:bg(BG))
%  ;
%    assert(M:bg(L))
    %  ).
