%:- module(hypgen, [
    %server_start/1,
    %server_stop/1
  %]).
:- use_module(library(http/thread_httpd)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).
%:- use_module(library(pengines)).


server_start(Port) :- http_server(http_dispatch, [port(Port)]).
server_stop(Port) :- http_stop_server(Port, []).


:- http_handler('/api/hypgen', handle_hypgen, []).
:- http_handler('/api/hypgen/candidate_genes', handle_candidate_genes, []).

:- http_handler('/api/error', 
    throw(http_reply(server_error(json{message:'Internal server error'}))), 
    []).

handle_hypgen(Request) :-
    http_parameters(Request, 
              [pos(Pos, [integer, optional(false)]),
               chr(Chr, [optional(false)]),
               ref(Ref, [optional(false)]), 
               alt(Alt, [optional(false)])]),

    % Find SNP by position, ref, and alt
    (findall(Snp, 
            (chr(Snp, Chr), start(Snp, Pos), ref(Snp, Ref), alt(Snp, Alt)),
            [Snp|_])  % Take the first matching SNP
    ->  % Extract rsid from Snp term
        Snp = snp(RsId),
        % Get genes and include rsid in response
        (setof(G, relevant_gene(gene(G), Snp), Genes) -> 
            reply_json(json{
                rsid: RsId,
                response: Genes
            })
        ;   reply_json(json{
                rsid: RsId,
                response: []
            })
        )
    ;   % No matching SNP found
        reply_json(json{
            rsid: '',
            response: []
            })
    ).

handle_candidate_genes(Request) :-
    % Get candidate genes for a given SNP
    % Get genes and include rsid in response
    http_parameters(Request, 
              [pos(Pos, [integer, optional(false)]),
               chr(Chr, [optional(false)]),
               ref(Ref, [optional(false)]), 
               alt(Alt, [optional(false)])]),

    % Find SNP by position, ref, and alt
    (findall(Snp, 
            (chr(Snp, Chr), start(Snp, Pos), ref(Snp, Ref), alt(Snp, Alt)),
              [Snp|_]) ->
      Snp = snp(Id),
      (candidate_genes(snp(Id), Genes) -> 
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
      })).
