%:- use_module(queries).
:- style_check(-discontiguous).
:- use_module(library(clpfd)).

:- dynamic hideme/1.
:- multifile hideme/1.

hideme([]).
hideme([Goal|Goals]) :-
  call(Goal),
  hideme(Goals).

% relevant_gene(G, S) :- in_tad_with(S, G).
% relevant_gene(G, S) :- eqtl_association(S, G).
% relevant_gene(G, S) :- activity_by_contact(S, G).
% relevant_gene(G, S) :- regulatory_effect(S, G).


% regulatory_effect(S, G) :- 
%     % format('Checking regulatory effect: S: ~w, G: ~w~n', [S, G]),
%     in_regulatory_region(S, Enh),
%     alters_tfbs(S, Tf, G),
%     % format('Alters TFBS: S: ~w, G: ~w~n', [S, G]),
%     binds_to(Tf, Tfbs),
%     overlaps_with(Tfbs, Enh), !.
%     % format('Regulatory effect: S: ~w, G: ~w~n', [S, G]).

regulatory_effect(S, G) :- 
    % format('Checking regulatory effect: S: ~w, G: ~w~n', [S, G]),
    in_regulatory_region(S, Enh),
    associated_with(Enh, G).
    % format('Associated with enhancer: S: ~w, G: ~w, Enh: ~w~n', [S, G, Enh]),
    % tf_snp(Tf, S),
    % regulates(Tf, G),
    % binds_to(Tf, Tfbs),
    % format('Regulatory effect: S: ~w, G: ~w, Enh: ~w, Tfbs: ~w~n', [S, G, Enh, Tfbs]),
    % overlaps_with(Tfbs, Enh), 
    % format('Overlaps with TFBS: S: ~w, G: ~w, Enh: ~w, Tfbs: ~w~n', [S, G, Enh, Tfbs]),
    !.

% overlaps_with_tf_enh(Enh, Tf) :-
%   format('Checking overlaps with TFBS: Enh: ~w, Tf: ~w~n', [Enh, Tf]),
%   chr(Enh, Chr),
%   start(Enh, Start),
%   end(Enh, End),
%   format('Enh: ~w, Start: ~w, End: ~w, Tf: ~w~n', [Enh, Start, End, Tf]).
%   % load_tfbs_data(Chr, Start, End, Tf),
  % format('Overlaps with TFBS: Enh: ~w, Tf: ~w~n', [Enh, Tf]).

coding_effect(S, G) :- 
  hideme([
    chr(S, Chr),
    start(S, Pos),
    ref(S, Ref),
    alt(S, Alt),
    has_coding_effect(G, Chr, Pos, Ref, Alt)
  ]).

in_regulatory_region(S, Enh) :-
    S = snp(_),
    (Enh = super_enhancer(E)
    ;Enh = enhancer(E)),
    within_k_distance(Enh, S, 50000). %50,000kb obtained from dbsup

alters_tfbs(S, Tf, G) :-
    find_and_rank_tfs(S, Tf, G).
    % format('Alters TFBS: S: ~w, Tf: ~w, G: ~w~n', [S, Tf, G]).


load_enh_tfbs(Enh) :-
  chr(Enh, Chr),
  start(Enh, Start),
  end(Enh, End),
  load_tfbs_data(Chr, Start, End).

codes_for(G, P) :-
    transcribed_to(G, T),
    translates_to(T, P).

in_tad_with(S, G1) :- 
    (closest_gene(S, G1)
    ;
    (closest_gene(S, G2),
    in_tad_region(G2, T),
    in_tad_region(G1, T))).

relevant_gene_coexpression(G1, S) :-
    relevant_gene(G2, S),
    coexpressed_with(G1, G2).

member_(G, O, 0) :- 
    codes_for(G, P),
    go_gene_product(O, P).

member_(G, O, D) :-
    D #= D0 + 1,
    rel_type(ontology_relationship(X, O), subclass),
    member_(G, X, D0).

relevant_go(O, S, SigGenes, Pval) :- 
    findall(G, relevant_gene_coexpression(gene(G), sequence_variant(S)), Gs),
    subset(SigGenes, Gs),
    Pval < 0.05.


% relevant_go(O, S) :-
%     ontology_term(O),
%     relevant_go_(O, S, SigGenes, Pval).


% relevant_go(O, S, SigGenes, Pval) :- 
%     O = hideme(ontology_term(X)),
%     relevant_gene(gene(G1), sequence_variant(S)),
%     findall(gene(G2), coexpressed_with(gene(G1), gene(G2)), Gs),
%     subset(SigGenes, Gs),
%     Pval < 0.05.

% member([G|Gs], O, P) :-
%     py_call(enrich:enrichr(go_0045598, [G|Gs])),

%Cell type specific

%is CT a relevant cell type for sequence variant S
% relevant_cell_type(CT, S) :-
%     in_dnase_hypersensitive_site(S, CT);
% histone_mark(S, CT). %enhancer h3 marks

% eqtl_association(S, G, CT) :-
%     eqtl(S, G), 
%     p_value(eqtl(S, G), Pval), 
%     tissue(eqtl(S, G), CT),
%     Pval < 0.05.

% relevant_gene(G, S, CT) :-
%     expressed_in(G, CT),
%     (in_tad_with(S, G),
%     eqtl_association(S, G, CT),
%     relevant_cell_type(CT, S)); 
%     regulatory_variant(R, S, G, CT).

% regulatory_variant(R, S, G, CT) :-
%     overlaps_with(S, R),
%     regulates(R, G),
%     tissue(regulates(R, G), CT).

% in_dnase_hypersensitive_site(S, CT) :- 
%     dnase_hypersensitivity_site(Id1), 
%     S = sequence_variant(Id2),
%     Id1 = Id2,
%     tissue(dnase_hypersensitivity_site(Id1), CT).

% histone_mark(S, CT) :-
%     histone_modification(Id1),
%     S = sequence_variant(Id2), 
%     Id1 = Id2,
%     tissue(histone_modification(Id1), CT).

% variant_in_tfbs(S, Tf, G, CT) :-
%     overlaps_with(motif(Tf), S),
%     regulates(Tf, G),
%     expressed_in(G, CT).


nv(X):- numbervars(X,0,_,[]).
