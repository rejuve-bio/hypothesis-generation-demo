:- use_module(library(mcintyre)).
:- mc.

:- begin_lpad.

relevant_gene(Gene, Snp):0.34 :-
  regulatory_effect(Snp, Gene).

relevant_gene(Gene,Snp):0.0176 :-
  eqtl_association(Snp, Gene).

relevant_gene(Gene, Snp):0.021 :-
  activity_by_contact(Snp, Gene).


% relevant_gene(Gene, Snp) :- regulatary_effect(Snp, Gene), sample_head([0.34, 0.66], 1, [Gene, Snp], NH), NH=1.
% relevant_gene(Gene, Snp) :- eqtl_association(Snp, Gene), sample_head([0.02, 0.982], 2, [Gene, Snp], NH), NH=1.
% relevant_gene(Gene, Snp) :- activity_by_contact(Snp, Gene), sample_head([0.021, 0.979], 3, [Gene, Snp], NH), NH=1.

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
    % !.

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

:- end_lpad.



