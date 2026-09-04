:- use_module(library(odbc)).

:- dynamic tfbs_snp/2.
:- dynamic score/2.
:- dynamic effect/2.
:- dynamic tfbs/1.
:- dynamic binds_to/2.
:- dynamic chr/2.
:- dynamic start/2.
:- dynamic end/2.
:- dynamic db_opened/0.
:- table tfbs_effect/4.

open_db :- 
  ( db_opened ->
      true
  ; odbc_connect('PGOT', _, 
                    [alias(opnt), 
                     open(once),
                     encoding(utf8)]),
    assertz(db_opened)
  ).

reconnectable_odbc_error(error(odbc(State, _Native, _Message), _)) :-
    member(State, ['57P01', '08003', '08006', '08001']).

db_query(SQL, Row) :-
  open_db,
  catch(
      odbc_query(opnt, SQL, Row),
      Error,
      ( reconnectable_odbc_error(Error) ->
          format(user_error,
                 'ODBC error ~q; reconnecting to PostgreSQL and retrying.~n',
                 [Error]),
          retractall(db_opened),
          catch(odbc_disconnect(opnt), _, true),
          open_db,
          odbc_query(opnt, SQL, Row)
      ; throw(Error)
      )
  ).

candidate_genes(S, Genes) :-
    setof(Gene, in_tad_with(S, gene(Gene)), InTadGenes),
    setof(Gene, within_k_distance(gene(Gene), S, 500000), ClosestGenes),
    union(InTadGenes, ClosestGenes, Genes).
  %maplist(gene_name, InTadGenes, Genes).

gene_id(Name, gene(Id)) :-
    open_db,
    upcase_atom(Name, UName),
    sql_quote(UName, QName),
    format(atom(SQL),
           'SELECT geneid FROM gencode_gene WHERE genename = ~w LIMIT 1',
           [QName]),
  db_query(SQL, row(Id)).

% Fuzzy cell/tissue type lookup: returns matching IDs from cell_ontology and biosample
cell_id(Name, cell_type(Id)) :-
    open_db,
    downcase_atom(Name, LName),
    atomic_list_concat(['%', LName, '%'], Pattern),
    sql_quote(Pattern, QPattern),
    format(atom(SQL),
           'SELECT ontologyid, celltype AS name FROM catlas_cell_ontology
             WHERE lower(celltype) LIKE ~w
            UNION
            SELECT biosampleid, biosamplename FROM biosample
             WHERE lower(biosamplename) LIKE ~w',
           [QPattern, QPattern]),
    db_query(SQL, row(Id, _MatchedName)).

gene_name(gene(Id), Name) :-
    nonvar(Id),
    open_db,
    upcase_atom(Id, UId),
    sql_quote(UId, QId),
    format(atom(SQL),
           'SELECT genename FROM gencode_gene WHERE geneid = ~w LIMIT 1',
           [QId]),
    db_query(SQL, row(Name)).
gene_name(gene(Id), Name) :-
    nonvar(Name),
    open_db,
    upcase_atom(Name, UName),
    sql_quote(UName, QName),
    format(atom(SQL),
           'SELECT geneid FROM gencode_gene WHERE genename = ~w LIMIT 1',
           [QName]),
        db_query(SQL, row(Id)).

variant_locus(RsId, Chr, Pos, Ref, Alt) :-
    open_db,
    sql_quote(RsId, QRsId),
    format(atom(SQL), 
     'SELECT chromosome, position, referenceallele, alternateallele
           FROM variant
           WHERE rsids @> ARRAY[~w]::varchar[]
          LIMIT 1', [QRsId]),
        db_query(SQL, row(Chr, Pos, Ref, Alt)).

variant_id(snp(RsId), Id) :-
    variant_locus(RsId, Chr, Pos, Ref, Alt),
    format(atom(Id), 'chr~w_~w_~w_~w', [Chr, Pos, Ref, Alt]).

% Closest gene to a variant (by distance to gene body)
closest_gene(snp(RsId), gene(GeneId)) :-
    open_db,
    sql_quote(RsId, QRsId),
    format(atom(SQL),
           'SELECT g.geneid
              FROM variant v
              JOIN gencode_gene g
                ON g.chromosome = concat(''chr'', v.chromosome)
               AND v.position BETWEEN (g.start - 500000) AND (g."end" + 500000)
             WHERE v.rsids @> ARRAY[~w]::varchar[]
               AND g.genetype = ''protein_coding''
             ORDER BY ABS(v.position - (g.start + g."end") / 2)
             LIMIT 1',
           [QRsId]),
    db_query(SQL, row(GeneId)).

sql_quote(Atom, Quoted) :-
    atom_string(Atom, S),
    split_string(S, "'", "", Parts),
    atomics_to_string(Parts, "''", Escaped),
    format(atom(Quoted), '\'~w\'', [Escaped]).

gene_body_distance(snp(RsId), gene(GeneId), Distance) :-
    open_db,
    sql_quote(RsId, QRsId),
    ( nonvar(GeneId) ->
        sql_quote(GeneId, QGeneId),
        format(atom(GeneFilter),
               ' AND g.geneid = ~w',
               [QGeneId])
    ; GeneFilter = ''
    ),
    format(atom(SQL),
           'SELECT DISTINCT g.geneid,
                            CASE
                              WHEN v.position BETWEEN g.start AND g."end" THEN 0
                              WHEN v.position < g.start THEN g.start - v.position
                              ELSE v.position - g."end"
                            END AS distance
              FROM variant v
              JOIN gencode_gene g
                ON g.chromosome = concat(''chr'', v.chromosome)
             WHERE v.rsids @> ARRAY[~w]::varchar[]
               AND g.genetype = ''protein_coding''~w',
           [QRsId, GeneFilter]),
    db_query(SQL, row(GeneId, Distance)).

within_k_distance(gene(GeneId), snp(RsId), K) :-
    open_db,
    sql_quote(RsId, QRsId),
    format(atom(SQL),
           'SELECT DISTINCT g.geneid
              FROM variant v
              JOIN gencode_gene g
                ON g.chromosome = concat(''chr'', v.chromosome)
             WHERE v.rsids @> ARRAY[~w]::varchar[]
               AND g.genetype = ''protein_coding''
               AND CASE
                     WHEN v.position BETWEEN g.start AND g."end" THEN 0
                     WHEN v.position < g.start THEN g.start - v.position
                     ELSE v.position - g."end"
                   END <= ~d',
           [QRsId, K]),
    db_query(SQL, row(GeneId)).

within_k_distance(enhancer(IntervalId), snp(RsId), K) :-
    open_db,
    sql_quote(RsId, QRsId),
    format(atom(SQL),
           'SELECT DISTINCT e.intervalid
              FROM variant v
              JOIN enhancer e
                ON e.chromosome = v.chromosome
             WHERE v.rsids @> ARRAY[~w]::varchar[]
               AND CASE
                     WHEN v.position BETWEEN e.start AND e."end" THEN 0
                     WHEN v.position < e.start THEN e.start - v.position
                     ELSE v.position - e."end"
                   END <= ~d',
           [QRsId, K]),
    db_query(SQL, row(IntervalId)).


% qtl_association(Type, RsId, GeneId, Tissue) :-
qtl_association(Type, snp(RsId), gene(GeneId)) :-
    open_db,
    sql_quote(Type, QType),
    sql_quote(RsId, QRsId),
    format(atom(SQL),
           'SELECT s.geneid
              FROM variant v
              JOIN credible_set_variants csv ON csv.variantid = v.variantid
              JOIN study s ON csv.studyid = s.studyid
             WHERE v.rsids @> ARRAY[~w]::varchar[]
               AND s.studytype = ~w
               AND csv.is95credibleset = true
               AND s.geneid IS NOT NULL',
           [QRsId, QType]),
    db_query(SQL, row(GeneId)).

qtl_coloc_gene(Type, snp(LeadRsId), gene(GeneId), H4, Tissue) :-
    open_db,
    ( nonvar(Type) ->
        sql_quote(Type, QType),
        format(atom(TypeFilter),
               ' AND c.rightstudytype = ~w',
               [QType])
    ; TypeFilter = ''
    ),
    ( nonvar(LeadRsId) ->
        sql_quote(LeadRsId, QLeadRsId),
        format(atom(LocusFilter),
               ' AND lead_v.rsids @> ARRAY[~w]::varchar[]',
               [QLeadRsId])
    ; LocusFilter = ''
    ),
    ( nonvar(GeneId) ->
        sql_quote(GeneId, QGeneId),
        format(atom(GeneFilter),
               ' AND s.geneid = ~w',
               [QGeneId])
    ; GeneFilter = ''
    ),
    format(atom(SQL),
           'SELECT DISTINCT lrs.lead_rsid,
                            s.geneid,
                            c.h4,
                            COALESCE(b.biosamplename, s.biosamplefromsourceid, s.biosampleid)
              FROM coloc c
              JOIN credible_set gcs
                ON gcs.studylocusid = c.leftstudylocusid
              JOIN variant lead_v
                ON lead_v.variantid = gcs.variantid
              CROSS JOIN LATERAL unnest(lead_v.rsids) AS lrs(lead_rsid)
              JOIN credible_set qcs
                ON qcs.studylocusid = c.rightstudylocusid
              JOIN study s
                ON s.studyid = qcs.studyid
              LEFT JOIN biosample b
                ON b.biosampleid = s.biosampleid
             WHERE s.geneid IS NOT NULL
               AND lrs.lead_rsid IS NOT NULL~w~w~w',
           [TypeFilter, LocusFilter, GeneFilter]),
      db_query(SQL, row(LeadRsId, GeneId, H4, Tissue)).

% Gene is in a TAD
in_tad_region(gene(GeneId), tad(TadId)) :-
    nonvar(GeneId), !,
    open_db,
    sql_quote(GeneId, QGeneId),
    format(atom(SQL),
           'SELECT tadid
              FROM tadmap_tad_gene
             WHERE geneid = ~w',
           [QGeneId]),
    db_query(SQL, row(TadId)).
in_tad_region(gene(GeneId), tad(TadId)) :-
    nonvar(TadId), !,
    open_db,
    sql_quote(TadId, QTadId),
    format(atom(SQL),
           'SELECT geneid
              FROM tadmap_tad_gene
             WHERE tadid = ~w',
           [QTadId]),
        db_query(SQL, row(GeneId)).

in_tad_with(S, G1) :- 
    closest_gene(S, G1).
in_tad_with(S, G1) :- 
    closest_gene(S, G2),
    in_tad_region(G2, Tad),
    in_tad_region(G1, Tad).

% Variant is within 100kb of a cCRE
in_regulatory_region(snp(RsId), enhancer(EnhId)) :-
    open_db,
    sql_quote(RsId, QRsId),
    format(atom(SQL),
           'WITH snp AS (
              SELECT chromosome, position FROM variant
              WHERE rsids @> ARRAY[~w]::varchar[] LIMIT 1
            )
            SELECT DISTINCT abc.chromosome || ''_'' || abc.ccrestart || ''_'' || abc.ccreend AS enhid,
                   abc.ccreid
              FROM snp, catlas_abc_scores abc
             WHERE abc.chromosome = ''chr'' || snp.chromosome
               AND abc.ccrestart >= snp.position - 100000
               AND abc.ccreend <= snp.position + 100000
               AND snp.position BETWEEN (abc.ccrestart - 100000) AND (abc.ccreend + 100000)',
           [QRsId]),
    db_query(SQL, row(EnhId, _CCREId)).

% cCRE linked to gene via ABC model with score
% EnhId is chr_start_end format; look up by coordinates
activity_by_contact(enhancer(EnhId), gene(GeneId), Score) :-
    open_db,
    atomic_list_concat([Chr, StartA, EndA], '_', EnhId),
    atom_number(StartA, Start),
    atom_number(EndA, End),
    sql_quote(Chr, QChr),
    format(atom(SQL),
           'SELECT DISTINCT geneid, abcscore
              FROM catlas_abc_scores
             WHERE geneid IS NOT NULL
               AND chromosome = ~w
               AND ccrestart = ~d
               AND ccreend = ~d
               AND abcscore > 0.015',
           [QChr, Start, End]),
    db_query(SQL, row(GeneId, Score)).

% cCRE is accessible in a given cell type (has ABC entry)
accessible_in(enhancer(EnhId), cell_type(OntologyId)) :-
    open_db,
    atomic_list_concat([Chr, StartA, EndA], '_', EnhId),
    atom_number(StartA, Start),
    atom_number(EndA, End),
    sql_quote(Chr, QChr),
    format(atom(SQL),
           'SELECT DISTINCT co.ontologyid
              FROM catlas_abc_scores abc
              JOIN catlas_cell_ontology co ON abc.celltype = co.celltype
             WHERE abc.chromosome = ~w
               AND abc.ccrestart = ~d
               AND abc.ccreend = ~d',
           [QChr, Start, End]),
      db_query(SQL, row(OntologyId)).

% TFs with binding sites overlapping a variant position
variant_in_tfbs(snp(RsId), gene(TfGeneId)) :-
    open_db,
    sql_quote(RsId, QRsId),
    format(atom(SQL),
           'WITH snp AS (
              SELECT chromosome, position FROM variant
              WHERE rsids @> ARRAY[~w]::varchar[] LIMIT 1
            )
            SELECT DISTINCT r.tfgeneid
              FROM snp
              JOIN remap_tfbs r
                ON r.chromosome = ''chr'' || snp.chromosome
               AND r.start <= snp.position
               AND r."end" >= snp.position
             WHERE r.tfgeneid IS NOT NULL',
           [QRsId]),
    db_query(SQL, row(TfGeneId)).

% TF regulates a target gene (searches DoRothEA, TFLink, hTFtarget)
regulates(gene(TfId), gene(GeneId)) :-
    nonvar(TfId),
    nonvar(GeneId), !,
    open_db,
    sql_quote(TfId, QTfId),
    sql_quote(GeneId, QGeneId),
    member(Table, [dorothea_tf_target, tflink_tf_target, htftarget_tf_target]),
    format(atom(SQL),
           'SELECT 1 FROM ~w WHERE tfid = ~w AND geneid = ~w LIMIT 1',
           [Table, QTfId, QGeneId]),
    db_query(SQL, row(_)).

regulates(gene(TfId), gene(GeneId)) :-
    nonvar(TfId),
    open_db,
    sql_quote(TfId, QTfId),
    member(Table, [dorothea_tf_target, tflink_tf_target, htftarget_tf_target]),
    format(atom(SQL),
           'SELECT DISTINCT geneid FROM ~w WHERE tfid = ~w',
           [Table, QTfId]),
        db_query(SQL, row(GeneId)).

regulates(gene(TfId), gene(GeneId)) :-
    nonvar(GeneId),
    open_db,
    sql_quote(GeneId, QGeneId),
    member(Table, [dorothea_tf_target, tflink_tf_target, htftarget_tf_target]),
    format(atom(SQL),
           'SELECT DISTINCT tfid FROM ~w WHERE geneid = ~w',
           [Table, QGeneId]),
        db_query(SQL, row(TfId)).

credible_set(Lead, Snp) :-
  in_credible_set(Lead, Snp, _PIP).

in_credible_set(snp(LeadRsId), snp(RsId), PIP) :-
    open_db,
    ( nonvar(LeadRsId) ->
        sql_quote(LeadRsId, QLeadRsId),
        format(atom(LeadFilter),
               ' AND lead_v.rsids @> ARRAY[~w]::varchar[]',
               [QLeadRsId])
    ; LeadFilter = ''
    ),
    ( nonvar(RsId) ->
        sql_quote(RsId, QRsId),
        format(atom(MemberFilter),
               ' AND member_v.rsids @> ARRAY[~w]::varchar[]',
               [QRsId])
    ; MemberFilter = ''
    ),
    format(atom(SQL),
           'SELECT lead_rsid, member_rsid, max(pip) AS pip
              FROM (
                    SELECT lrs.lead_rsid,
                           mrs.member_rsid,
                           (elem->>''posteriorProbability'')::double precision AS pip
                      FROM credible_set cs
                      JOIN variant lead_v
                        ON lead_v.variantid = cs.variantid
                     CROSS JOIN LATERAL jsonb_array_elements(cs.locus::jsonb) AS elem
                      JOIN variant member_v
                        ON member_v.variantid = elem->>''variantId''
                     CROSS JOIN LATERAL unnest(lead_v.rsids) AS lrs(lead_rsid)
                     CROSS JOIN LATERAL unnest(member_v.rsids) AS mrs(member_rsid)
                     WHERE (elem->>''is95CredibleSet'')::boolean = true
                       AND lrs.lead_rsid IS NOT NULL
                       AND mrs.member_rsid IS NOT NULL~w~w
                   ) ranked
             GROUP BY lead_rsid, member_rsid
            ORDER BY pip DESC NULLS LAST
            LIMIT 100',
           [LeadFilter, MemberFilter]),
      db_query(SQL, row(LeadRsId, RsId, PIP)).

% init_py :-
%     py_add_lib_dir('/home/abdu/code/hypothesis-generation-demo/scripts').


% tfbs_effect(snp(RsId), Tf, Score, Effect) :-
%   init_py,
%   variant_locus(RsId, C, Pos, Ref, Alt),
%   format(atom(Chr), 'chr~w', [C]),
%   py_call(deltasvm_snp:run_deltasvm(Chr, Pos, Ref, Alt), Results),
%   member(Result, Results),
%   get_dict(tf, Result, Gene),
%   get_dict(pbs, Result, Score),
%   get_dict(effect, Result, Effect),
%   gene_id(Gene, Tf).

tfbs_effect(snp(RsId), gene(TfGeneId), Score, Effect) :-
    open_db,
    sql_quote(RsId, QRsId),
    ( nonvar(TfGeneId) ->
        sql_quote(TfGeneId, QTfGeneId),
        format(atom(TfFilter),
               ' AND d.tfid = ~w',
               [QTfGeneId])
    ; TfFilter = ''
    ),
    format(atom(SQL),
           'SELECT DISTINCT d.tfid,
                            d.score,
                            lower(d.effect)
              FROM variant v
              JOIN deltasvm d
                ON d.variantid = v.variantid
             WHERE v.rsids @> ARRAY[~w]::varchar[]~w',
           [QRsId, TfFilter]),
    db_query(SQL, row(TfGeneId, Score, Effect)).

has_coding_effect(snp(RsId), gene(GeneId), Effect) :-
    open_db,
    sql_quote(RsId, QRsId),
    format(atom(SQL),
           'SELECT geneid, effect
              FROM variant_coding_effects
             WHERE rsid = ~w',
           [QRsId]),
  db_query(SQL, row(GeneId, Effect)).