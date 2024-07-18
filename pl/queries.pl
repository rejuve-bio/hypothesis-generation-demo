:- use_module(library(http/thread_httpd)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(pengines)).
:- use_module(library(interpolate)).

candidate_genes(S, Genes) :-
    setof(Gene, in_tad_with(S, Gene), Gs),
    %TODO optimize with_k_distance
    % setof(Gene, within_k_distance(Gene, S, 500000), ClosestGenes),
    % union(InTadGenes, ClosestGenes, Gs),
    maplist(gene_name, Gs, Genes).

gene_id(Name, Id) :- gene_name(gene(Id), Name).

variant_id(S, Id) :-
    chr(S, Chr),
    start(S, Start),
    end(S, End),
    ref(S, R),
    alt(S, A),
    upcase_atom(R, Ref),
    upcase_atom(A, Alt),
    Id = "$Chr:$Start-$End-$Ref>$Alt".

within_k_distance(G, S, K) :-
    chr(G, Chr),
    chr(S, Chr),
    start(G, StartG),
    end(G, EndG),
    start(S, StartS),
    end(S, EndS),
    StartDist is abs(StartS - StartG),
    EndDist is abs(EndS - EndG), 
    (StartDist =< K
    ; EndDist =< K).

server_start(Port) :- http_server(http_dispatch, [port(Port)]).

:- use_module(library(sandbox)).
:- multifile sandbox:safe_primitive/1.

sandbox:safe_primitive(interpolate:build_text(_,_,_)).