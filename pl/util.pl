/**
 * concurrent_include(:Goal, +List, -Included).
 *
 * True when Included is the list of elements X of List for which
 * call(Goal, X) succeeds. The checks are performed concurrently.
 * The order of elements in Included corresponds to the order in List.
 *
 * Behaves like include/3 but uses threads for checking the Goal.
 * Requires SWI-Prolog's `library(thread)`.
 */

concurrent_include(Goal, List, Included) :-
    concurrent_maplist(concurrent_include_worker(Goal), List, Results),
    include_results(Results, Included).

% Helper predicate called by concurrent_maplist
concurrent_include_worker(Goal, Elem, Result) :-
    (   call(Goal, Elem)
    ->  Result = element(Elem)
    ;   Result = fail
    ).

% Helper predicate to filter the results from concurrent_maplist
include_results([], []).
include_results([element(Elem)|RestResults], [Elem|RestIncluded]) :-
    !,
    include_results(RestResults, RestIncluded).
include_results([fail|RestResults], Included) :-
    include_results(RestResults, Included).

is_even(N) :-
    N mod 2 =:= 0.

