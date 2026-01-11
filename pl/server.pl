:- use_module(library(http/thread_httpd)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).


server_start(Port) :- http_server(http_dispatch, [port(Port)]).
server_stop(Port) :- http_stop_server(Port, []).


:- http_handler('/api/hypgen', handle_hypgen, []).
:- http_handler('/api/hypgen/candidate_genes', handle_candidate_genes, []).
:- http_handler('/api/query', handle_query, []).

:- http_handler('/api/error', 
    throw(http_reply(server_error(json{message:'Internal server error'}))), 
    []).

handle_hypgen(Request) :-
    http_parameters(Request, 
              [rsid(RsId, [optional(false)]), 
               seed(Seed, [integer, optional(false)]),
               samples(Samples, [integer, optional(false)])]),

        Snp = snp(RsId),
        ((proof_tree(relevant_gene(Gene, Snp), Seed, Samples, Graph)) -> 
            reply_json(json{
                rsid: RsId,
                response: Graph
            })
        ;   reply_json(json{
                rsid: RsId,
                response: []
            })).

handle_candidate_genes(Request) :-
    http_parameters(Request, 
            [rsid(RsId, [optional(false)])]),

    Snp = snp(RsId),
    (candidate_genes(Snp, Genes) -> 
        reply_json(json{
            rsid: RsId,
            candidate_genes: Genes
        })
    ;   reply_json(json{
            rsid: RsId,
            candidate_genes: []
        })
    ).

% Used for term_name queries (phenotype lookups)
extract_term_id(Term, JsonTerm) :-
    compound(Term),
    functor(Term, Functor, 1),
    arg(1, Term, Arg),
    !,
    % Return as "functor(arg)" format
    format(atom(JsonTerm), '~w(~w)', [Functor, Arg]).
extract_term_id(Term, Term).

handle_query(Request) :-
    http_parameters(Request, 
            [query(QueryString, [optional(false)])]),
    
    catch(
        (
            term_string(Query, QueryString),
            % Handle different query patterns
            (   Query = gene_id(Name, X) ->
                findall(X, gene_id(Name, X), Results)
            ;   Query = gene_name(Gene, X) ->
                findall(X, gene_name(Gene, X), Results)
            ;   Query = variant_id(Variant, X) ->
                findall(X, variant_id(Variant, X), Results)
            ;   Query = term_name(Term, Name) ->
                findall(Term, term_name(Term, Name), RawResults),
                % Extract IDs from compound terms like efo(ID)
                maplist(extract_term_id, RawResults, Results)
            ;   % Unknown query pattern - reject to prevent arbitrary code execution
                Results = []
            ),
            reply_json(Results)
        ),
        Error,
        (
            format(string(ErrorMsg), "Query execution failed: ~w", [Error]),
            reply_json(json{error: ErrorMsg})
        )
    ).