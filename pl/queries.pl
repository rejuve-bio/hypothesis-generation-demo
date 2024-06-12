

natnum(0) :- 1 > 0, hideme(2 < 3), hideme(3 < 4), 0 = 0.
% natnum(s(X)) :- natnum(X), explanation("Successor of $X is an integer if $X is an integer").
natnum(s(X)) :- natnum(X).


% in_tad_with(sequence_variant(rs1421085), gene(ensg00000177508))
% member(gene(ensg00000172216), ontology_term(go_0045598))

% relevant_go(ontology_term(go_0045598), sequence_variant(rs1421085), [gene(ensg00000172216), gene(ensg00000170323)], 0.01)