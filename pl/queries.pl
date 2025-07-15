:- dynamic tfbs_snp/2.
:- dynamic score/2.
:- dynamic effect/2.
:- dynamic tfbs/1.
:- dynamic binds_to/2.
:- dynamic chr/2.
:- dynamic start/2.
:- dynamic end/2.

candidate_genes(S, Genes) :-
    setof(Gene, in_tad_with(S, gene(Gene)), InTadGenes),
    setof(Gene, within_k_distance(gene(Gene), S, 500000), ClosestGenes),
    union(InTadGenes, ClosestGenes, Genes).
  %maplist(gene_name, InTadGenes, Genes).

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

overlaps_with(A, B) :-
             chr(A, Chr),
             chr(B, Chr),
             start(A, StartA),
             start(B, StartB),
             StartB < StartA,
             end(A, EndA),
             end(B, EndB),
             EndA < EndB.


within_k_distance(G, S, K) :-
    chr(S, Chr),
    chr(G, Chr),
    start(S, Pos),
    start(G, StartG),
    end(G, EndG),
    (abs(Pos - StartG) =< K, !
    ; abs(Pos - EndG) =< K).

abs_score_pair(Score-TF, AbsScore-TF) :-
    AbsScore is abs(Score).

% Get the TF with lowest loss score for a SNP.
find_and_rank_tfs(SNP, Tf, G) :-
    % load_motif_effects(SNP),
    setof(Score-TF, (tf_snp(TF, SNP), regulates(TF, G), 
      (effect(tf_snp(TF, SNP), loss) ; effect(tf_snp(TF, SNP), gain)), 
      score(tf_snp(TF, SNP), Score)), TFScorePairs),
    maplist(abs_score_pair, TFScorePairs, AbsScoreTFPairs),
    sort(1, @>=, AbsScoreTFPairs, RankedPairs),
    pairs_values(RankedPairs, RankedTFs),
    member(Tf, RankedTFs).

init_py :-
    py_add_lib_dir('/home/abdu/bio_ai/code/hypothesis-generation-demo').

load_motif_effects(SNP) :-
  (tfbs_snp(_, SNP) -> true % skip loading motif effects if data already exists. 
    ;
    init_py,
    SNP = snp(ID),
    chr(SNP, CHR),
    start(SNP, POS),
    ref(SNP, REF),
    alt(SNP, ALT),
    py_call(inference_util:get_motif_effect_data(
        CHR, POS, ID, REF, ALT), Results),
    maplist(assert_motif_data, Results)).

% :- table load_tfbs_data/2.

load_tfbs_data(Chr, Start, End, Tf) :- 
  init_py,
  gene_name(Tf, TfName),
  py_call(inference_util:get_tfbs_data(Chr, Start, End, TfName), Results),
  Results = [H|_],
  maplist(assert_tfbs_data, Results).


assert_motif_data(Data) :- 
  Variant = Data.variant, 
  TF = Data.tf, 
  Score = Data.score,
  Type = Data.type, 

  (gene_name(gene(Gene), TF) ->
  Term = tfbs_snp(gene(Gene), snp(Variant)),
  assertz(Term),
  assertz(score(Term, Score)),
  assertz(effect(Term, Type))
; true).

assert_tfbs_data(Data) :- 
  Id = Data.id,
  Chr = Data.chr, 
  Start = Data.start,
  End = Data.end,
  Tf = Data.tf,

  (gene_name(gene(Gene), Tf) -> 

    Term = tfbs(Id), 
    assertz(Term),
    assertz(chr(Term, Chr)),
    assertz(start(Term, Start)),
    assertz(end(Term, End)),
    assertz(binds_to(gene(Gene), Term))
  ; true %skip if gene is not found in the kb.
  ).

%has_coding_effect(G, Chr, Pos, Ref, Alt) :-
%    init_py,
%    py_call(inference_util:analyze_coding_effect(Chr, Pos, Ref, Alt), Results),
%    Results = [Data],
%    Status = Data.status,
%    Status = coding,
%    Hugo = Data.effect.hugo,
    %    gene_name(G, Hugo).

% Define the server endpoint
cravat_server('http://localhost:5060').

% Main predicate to analyze variants
has_coding_effect(G, Chr, Pos, Ref, Alt) :-
    cravat_server(Server),
    % format(atom(URL), '~w/analyze?chr=~w&pos=~w&ref=~w&alt=~w', 
    %        [Server, Chr, Pos, Ref, Alt]),
    % Make GET request and handle JSON properly
    http_get(URL, [Response], []),
    % format("Response: ~w~n", [Response]),S
    Response = json(List),
    member(status=Status, List),
    member(effect=json(EffectList), List),
    member(hugo=Hugo, EffectList),
    Status = coding,
    gene_name(G, Hugo).
