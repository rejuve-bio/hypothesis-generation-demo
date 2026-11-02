:- use_module(library(http/thread_httpd)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).
:- use_module(library(http/http_error)).

server_start(Port) :- http_server(http_dispatch, [port(Port)]).
server_stop(Port) :- http_stop_server(Port, []).

:- http_handler('/api/hypgen/regulatory_effect_1', handle_regulatory_effect_1, []).
:- http_handler('/api/hypgen/regulatory_effect_2', handle_regulatory_effect_2, []).
:- http_handler('/api/hypgen/regulatory_effect_2_any', handle_regulatory_effect_2_any, []).
:- http_handler('/api/hypgen/regulatory_effect_3', handle_regulatory_effect_3, []).
:- http_handler('/api/hypgen/regulatory_effect_3_any', handle_regulatory_effect_3_any, []).
:- http_handler('/api/hypgen/regulatory_effect_4', handle_regulatory_effect_4, []).
:- http_handler('/api/hypgen/regulatory_effect_4_any', handle_regulatory_effect_4_any, []).

reply_invalid_query_arg(Arg, Value) :-
    reply_json(
        json{
            error: invalid_query_arg,
            arg: Arg,
            value: Value
        },
        [status(400)]
    ).

reply_goal_result(Payload, Matched) :-
    put_dict(matched, Payload, Matched, Response),
    reply_json(Response).

resolve_gene_name(GeneName, G) :-
    once(gene_name(G, GeneName)).


handle_regulatory_effect_1(Request) :-
    http_parameters(
        Request,
        [ snp(SnpId, [optional(false)]),
          gene(GeneName, [optional(false)])
        ]
    ),
    Snp = snp(SnpId),
    Payload = json{
        snp: SnpId,
        gene: GeneName
    },
    (   resolve_gene_name(GeneName, G)
    ->  (   regulatory_effect_1(Snp, G)
        ->  reply_goal_result(Payload, true)
        ;   reply_goal_result(Payload, false)
        )
    ;   reply_invalid_query_arg(gene, GeneName)
    ).

handle_regulatory_effect_2(Request) :-
    http_parameters(
        Request,
        [ snp(SnpId, [optional(false)]),
          gene(GeneName, [optional(false)]),
          cell_type(CellName, [optional(false)])
        ]
    ),
    Snp = snp(SnpId),
    Payload = json{
        snp: SnpId,
        gene: GeneName,
        cell_type: CellName
    },
    (   resolve_gene_name(GeneName, G)
    ->  (   regulatory_effect_2(Snp, G, CellName)
        ->  reply_goal_result(Payload, true)
        ;   reply_goal_result(Payload, false)
        )
    ;   reply_invalid_query_arg(gene, GeneName)
    ).

handle_regulatory_effect_3(Request) :-
    http_parameters(
        Request,
        [ snp(SnpId, [optional(false)]),
          gene(GeneName, [optional(false)]),
          tf(TfName, [optional(false)]),
          cell_type(CellName, [optional(false)])
        ]
    ),
    Snp = snp(SnpId),
    Payload = json{
        snp: SnpId,
        gene: GeneName,
        tf: TfName,
        cell_type: CellName
    },
    (   resolve_gene_name(GeneName, G)
    ->  (   resolve_gene_name(TfName, Tf)
        ->  (   regulatory_effect_3(Snp, G, Tf, CellName)
            ->  reply_goal_result(Payload, true)
            ;   reply_goal_result(Payload, false)
            )
        ;   reply_invalid_query_arg(tf, TfName)
        )
    ;   reply_invalid_query_arg(gene, GeneName)
    ).

handle_regulatory_effect_4(Request) :-
    http_parameters(
        Request,
        [ snp(SnpId, [optional(false)]),
          gene(GeneName, [optional(false)]),
          tf(TfName, [optional(false)]),
          cell_type(CellName, [optional(false)])
        ]
    ),
    Snp = snp(SnpId),
    Payload = json{
        snp: SnpId,
        gene: GeneName,
        tf: TfName,
        cell_type: CellName
    },
    (   resolve_gene_name(GeneName, G)
    ->  (   resolve_gene_name(TfName, Tf)
        ->  (   regulatory_effect_4(Snp, G, Tf, CellName)
            ->  reply_goal_result(Payload, true)
            ;   reply_goal_result(Payload, false)
            )
        ;   reply_invalid_query_arg(tf, TfName)
        )
    ;   reply_invalid_query_arg(gene, GeneName)
    ).

handle_snp_gene_query(Request, Goal) :-
    http_parameters(
        Request,
        [ snp(SnpId, [optional(false)]),
          gene(GeneName, [optional(false)])
        ]
    ),
    Snp = snp(SnpId),
    Payload = json{
        snp: SnpId,
        gene: GeneName
    },
    (   resolve_gene_name(GeneName, G)
    ->  (   call(Goal, Snp, G)
        ->  reply_goal_result(Payload, true)
        ;   reply_goal_result(Payload, false)
        )
    ;   reply_invalid_query_arg(gene, GeneName)
    ).

handle_regulatory_effect_2_any(Request) :-
    handle_snp_gene_query(Request, regulatory_effect_2).

handle_regulatory_effect_3_any(Request) :-
    handle_snp_gene_query(Request, regulatory_effect_3).

handle_regulatory_effect_4_any(Request) :-
    handle_snp_gene_query(Request, regulatory_effect_4).