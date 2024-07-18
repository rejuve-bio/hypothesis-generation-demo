from flask import Flask, request
from flask_restful import Resource, Api, reqparse

class EnrichAPI(Resource):
    def __init__(self, enrichr, llm, prolog_query):
        self.enrichr = enrichr
        self.llm = llm
        self.prolog_query = prolog_query

    def get(self):
        args = request.args
        phenotype, variant = args['phenotype'], args['variant']
        print(f"Got request for phenotype: {phenotype}, variant: {variant}")
        candidate_genes = self.prolog_query.get_candidate_genes(variant)
        print(f"Candidate genes: {candidate_genes}")
        causal_gene = self.llm.predict_casual_gene(phenotype, candidate_genes)["causal_gene"]
        print(f"Predicted causal gene: {causal_gene}")
        enrich_tbl = self.enrichr.run(causal_gene)
        relevant_gos = self.llm.get_relevant_go(phenotype, enrich_tbl)
        return {"causal_gene": causal_gene, "GO":  relevant_gos}

class HypothesisAPI(Resource):
    def __init__(self, enrichr, prolog_query, llm):
        self.enrichr = enrichr
        self.prolog_query = prolog_query
        self.llm = llm


    def get(self):
        args = request.args
        print(f"Got request for args: {args}")
        causal_gene, go_id, go_name, \
            variant_id, coexpressed_genes, pval, phenotype = args['causal_gene'] ,args['go_id'], \
                                args['go_name'], args['variant_id'], args['genes'], args['pval'], \
                                args['phenotype']
        coexpressed_gene_names = coexpressed_genes.split(";")
        # # print(f"genes: {genes}, length: {len(genes)}")
        # ensembl_ids = self.enrichr.get_ensembl_ids(genes)
        causal_gene_id = self.prolog_query.get_gene_ids([causal_gene.lower()])[0]
        coexpressed_gene_ids = self.prolog_query.get_gene_ids([g.lower() for g in coexpressed_gene_names])
        causal_graph = self.prolog_query.get_relevant_gene_proof(variant_id, causal_gene)
        nodes, edges = causal_graph["nodes"], causal_graph["edges"]
        gene_nodes = [n for n in nodes if n["type"] == "gene"]
        gene_ids = [n['id'] for n in gene_nodes]
        gene_entities = [f"gene({id})" for id in gene_ids]
        query = f"maplist(gene_name, {gene_entities}, X)".replace("'", "")
        gene_names = self.prolog_query.execute_query(query)
        for id, name, node in zip(gene_ids, gene_names, gene_nodes):
            node["id"] = id
            node["name"] = name.upper()
        
        variant_nodes = [n for n in nodes if n["type"] == "snp"]
        variant_rsids = [n['id'] for n in variant_nodes]
        variant_entities = [f"snp({id})" for id in variant_rsids]
        query = f"maplist(variant_id, {variant_entities}, X)".replace("'", "")

        variant_ids = self.prolog_query.execute_query(query)
        for variant_id, rsid, node in zip(variant_ids, variant_rsids, variant_nodes):
            variant_id = variant_id.replace("'", "")
            node["id"] = variant_id
            node["name"] = rsid
            #Get the edges for the variant and update them
            source_edges = [e for e in edges if e["source"] == rsid]
            target_edges = [e for e in edges if e["target"] == rsid]
            for edge in source_edges:
                edge["source"] = variant_id
            for edge in target_edges:
                edge["target"] = variant_id
        
        nodes.append({"id": go_id, "type": "go", "name": go_name})
        phenotype_id = self.prolog_query.execute_query(f"term_name(efo(X), {phenotype})")
        nodes.append({"id": phenotype_id, "type": "phenotype", "name": phenotype})
        edges.append({"source": go_id, "target": phenotype_id, "label": "involved_in"})
        for gene_id, gene_name in zip(coexpressed_gene_ids, coexpressed_gene_names):
            nodes.append({"id": gene_id, "type": "gene", "name": gene_name})
            edges.append({"source": gene_id, "target": go_id, "label": "enriched_in"})
            edges.append({"source": causal_gene_id, "target": gene_id, "label": "coexpressed_with"})

        causal_graph = {"nodes": nodes, "edges": edges}
        summary = self.llm.summarize_graph(causal_graph)
        response = {"summary": summary, "graph": causal_graph}
        return response