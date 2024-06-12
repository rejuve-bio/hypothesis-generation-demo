from flask import Flask, request
from flask_restful import Resource, Api, reqparse

class EnrichAPI(Resource):
    def __init__(self, enrichr, llm, prolog_query):
        self.enrichr = enrichr
        self.llm = llm
        self.prolog_query = prolog_query

    def get(self):
        # parser = reqparse.RequestParser()
        # parser.add_argument('phenotype', type=str)
        # parser.add_argument('variant', type=str)
        # args = parser.parse_args()
        # phenotype, variant = args['phenotype'], args['variant']
        args = request.args
        phenotype, variant = args['phenotype'], args['variant']
        print(f"Got request for phenotype: {phenotype}, variant: {variant}")
        relevant_genes = self.prolog_query.get_relevant_gene(variant)
        relevant_gene = relevant_genes[0] #TODO fix this and show the user an option to select a gene
        enrich_tbl = self.enrichr.run(relevant_gene)
        relevant_gos = self.llm.get_relevant_go(phenotype, variant, enrich_tbl)
        return relevant_gos

class HypothesisAPI(Resource):
    def __init__(self, enrichr, llm, prolog_query):
        self.enrichr = enrichr
        self.llm = llm
        self.prolog_query = prolog_query


    def get(self):
        # parser = reqparse.RequestParser()
        # parser.add_argument('go_id', type=str, required=True)
        # parser.add_argument('variant_id', type=str, required=True)
        # parser.add_argument('genes', type=str, required=True)
        # parser.add_argument('pval', type=float, required=True)
        # args = parser.parse_args()
        args = request.args
        go_id, variant_id, genes, pval = args['go_id'], args['variant'], \
                            args['genes'], args['pval']
        genes = genes.split(";")
        print(f"genes: {genes}, length: {len(genes)}")
        ensembl_ids = self.enrichr.get_ensembl_ids(genes)
        proof_tree = self.prolog_query.get_go_proof(go_id, variant_id, ensembl_ids, pval)
        return proof_tree