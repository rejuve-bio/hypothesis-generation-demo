:- use_module(library(http/thread_httpd)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).


server_start(Port) :- http_server(http_dispatch, [port(Port)]).
server_stop(Port) :- http_stop_server(Port, []).


:- http_handler('/api/hypgen', handle_hypgen, []).
:- http_handler('/api/hypgen/candidate_genes', handle_candidate_genes, []).

:- http_handler('/api/error', 
    throw(http_reply(server_error(json{message:'Internal server error'}))), 
    []).

handle_hypgen(Request) :-
    http_parameters(Request, 
              [rsid(RsId, [optional(false)]), 
               samples(Samples, [integer, optional(false)])]),

        Snp = snp(RsId),
        ((proof_tree(relevant_gene(Gene, Snp), Samples, Graph)) -> 
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

      (candidate_genes(RsId, Genes) -> 
          reply_json(json{
              rsid: Id,
              candidate_genes: Genes
          })
      ;   reply_json(json{
              rsid: Id,
              candidate_genes: []
          })
      )
      ;
      reply_json(json{
          rsid: '',
          candidate_genes: []
      }).
