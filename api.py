from flask import Flask, request, jsonify
from flask_restful import Resource, Api, reqparse
from auth import token_required, JWT_SECRET_KEY
from datetime import datetime, timedelta
import jwt
from uuid import uuid4

class SignupAPI(Resource):
    def __init__(self, **kwargs):
        self.db = kwargs['db']

    def post(self):
        args = request.get_json()
        email = args.get('email')
        password = args.get('password')

        if not email or not password:
            return {'message': 'email and password are required'}, 400
        
        return self.db.create_user(email, password)

class LoginAPI(Resource):
    def __init__(self, **kwargs):
        self.db = kwargs['db']

    def post(self):
        args = request.get_json()
        email = args.get('email')
        password = args.get('password')

        if not email or not password:
            return {'message': 'email and password are required'}, 400
        response, status = self.db.verify_user(email, password)
        if status == 200:
            user_id = response.get('user_id')
            token = jwt.encode({'user_id': user_id, 'exp': datetime.utcnow() + timedelta(hours=12)}, JWT_SECRET_KEY, algorithm="HS256")
            return jsonify({'token': token})

        return response, status


class EnrichAPI(Resource):
    def __init__(self, enrichr, llm, prolog_query, db):
        self.enrichr = enrichr
        self.llm = llm
        self.prolog_query = prolog_query
        self.db = db

    @token_required
    def get(self, current_user_id):
        # Get the enrich_id from the query parameters
        enrich_id = request.args.get('id')
        if enrich_id:
            # Fetch a specific enrich by enrich_id and user_id
            enrich = self.db.get_enrich(current_user_id, enrich_id)
            if not enrich:
                return {"message": "Enrich not found or access denied."}, 404
            return enrich, 200

        # Fetch all hypotheses for the current user
        enrich = self.db.get_enrich(user_id=current_user_id)
        return enrich, 200

    @token_required
    def post(self, current_user_id):
        args = request.args
        phenotype, variant = args['phenotype'], args['variant']
        if self.db.check_enrich(current_user_id, phenotype, variant):
            enrich = self.db.get_enrich_by_phenotype_and_variant(phenotype, variant, current_user_id)
            # Return a 409 status code to notify the user that the enrichment has already been created and exists in the database.
            return {"id": enrich.get('id')}, 409
        
        print(f"Got request for phenotype: {phenotype}, variant: {variant}")
        candidate_genes = self.prolog_query.get_candidate_genes(variant)
        print(f"Candidate genes: {candidate_genes}")
        causal_gene = self.llm.predict_casual_gene(phenotype, candidate_genes)["causal_gene"]
        print(f"Predicted causal gene: {causal_gene}")
        causal_graph, proof = self.prolog_query.get_relevant_gene_proof(variant, causal_gene)

        if causal_graph is None:
            print(f"Failed to get proof for causal gene: {causal_gene}. Retrying with proof {proof}")
            # Re-prompt the llm with the proof
            causal_gene = self.llm.predict_casual_gene(phenotype, candidate_genes, rule=proof, prev_gene=causal_gene)["causal_gene"]
            # Re-try proof
            causal_graph, proof = self.prolog_query.get_relevant_gene_proof(variant, causal_gene)
            if causal_graph is None:
                return {"error": "Failed to get proof for causal gene"}
            
        print(f"Predicted causal gene: {causal_gene}")
        enrich_tbl = self.enrichr.run(causal_gene)
        relevant_gos = self.llm.get_relevant_go(phenotype, enrich_tbl)
        enrich_data = {
            "id": str(uuid4()),
            "created_on": datetime.utcnow().isoformat(timespec='milliseconds') + "Z",
            "variant": variant,
            "phenotype": phenotype,
            "causal_gene": causal_gene,
            "GO_terms": relevant_gos,
            "causal_graph": causal_graph
        }
        self.db.create_enrich(current_user_id, enrich_data)
        return {"id": enrich_data["id"]}
    
    @token_required
    def delete(self, current_user_id):
        enrich_id = request.args.get('id')
        if enrich_id:
            return self.db.delete_enrich(current_user_id, enrich_id)
        return {"message": "enrich id is required!"}

class HypothesisAPI(Resource):
    def __init__(self, enrichr, prolog_query, llm, db):
        self.enrichr = enrichr
        self.prolog_query = prolog_query
        self.llm = llm
        self.db = db

    @token_required
    def get(self, current_user_id):
        # Get the hypothesis_id from the query parameters
        hypothesis_id = request.args.get('id')

        if hypothesis_id:
            # Fetch a specific hypothesis by hypothesis_id and user_id
            hypothesis = self.db.get_hypotheses(current_user_id, hypothesis_id)
            if not hypothesis:
                return {"message": "Hypothesis not found or access denied."}, 404
            return hypothesis, 200

        # Fetch all hypotheses for the current user
        hypotheses = self.db.get_hypotheses(user_id=current_user_id)
        return hypotheses, 200

    @token_required
    def post(self, current_user_id):
        enrich_id = request.args.get('id')
        go_id = request.args.get('go')

        if self.db.check_hypothesis(current_user_id, enrich_id, go_id):
            hypothesis = self.db.get_hypothesis_by_enrich_and_go(enrich_id, go_id, current_user_id)
            summary = hypothesis.get('summary', 'No summary available')
            causal_graph = hypothesis.get('causal_graph', 'No graph available')
            return {"summary": summary, "graph": causal_graph}, 201
                    
        
        enrich_data = self.db.get_enrich(current_user_id, enrich_id)
        if not enrich_data:
            return {"message": "Invalid enrich_id or access denied."}, 404
                
        go_term = [go for go in enrich_data["GO_terms"] if go["id"] == go_id]
        print("this is go term: ", go_term)
        go_name = go_term[0]["name"]
        print("go_name: ", go_name)
        causal_gene = enrich_data['causal_gene']
        print("causal_gene: ", causal_gene)
        variant_id = enrich_data['variant']
        print("variant_id", variant_id)
        phenotype = enrich_data['phenotype']
        print("phenotype: ", phenotype)
        coexpressed_gene_names = go_term[0]["genes"] 
        print("coexpressed_genes: ", coexpressed_gene_names)
        causal_graph = enrich_data['causal_graph']
        print("this is the causal_graph: ", causal_graph)

        # coexpressed_gene_names = coexpressed_genes.split(";")
        causal_gene_id = self.prolog_query.get_gene_ids([causal_gene.lower()])[0]
        coexpressed_gene_ids = self.prolog_query.get_gene_ids([g.lower() for g in coexpressed_gene_names])
        
        print("this is the causal_graph: ", causal_graph)
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
            source_edges = [e for e in edges if e["source"] == rsid]
            target_edges = [e for e in edges if e["target"] == rsid]
            for edge in source_edges:
                edge["source"] = variant_id
            for edge in target_edges:
                edge["target"] = variant_id
        
        nodes.append({"id": go_id, "type": "go", "name": go_name})
        # phenotype_id = self.prolog_query.execute_query(f"term_name(efo(X), {phenotype})") #TODO: Fix this
        phenotype_id = "EFO_0001073"
        nodes.append({"id": phenotype_id, "type": "phenotype", "name": phenotype})
        edges.append({"source": go_id, "target": phenotype_id, "label": "involved_in"})
        for gene_id, gene_name in zip(coexpressed_gene_ids, coexpressed_gene_names):
            nodes.append({"id": gene_id, "type": "gene", "name": gene_name})
            edges.append({"source": gene_id, "target": go_id, "label": "enriched_in"})
            edges.append({"source": causal_gene_id, "target": gene_id, "label": "coexpressed_with"})

        causal_graph = {"nodes": nodes, "edges": edges}

        summary = self.llm.summarize_graph(causal_graph)

        hypothesis_data = {
            "id": str(uuid4()),
            "enrich_id": enrich_id,
            "go_id": go_id,
            "variant_id": variant_id,
            "phenotype": phenotype,
            "causal_gene": causal_gene,
            "graph": causal_graph,
            "summary": summary,
            "biological_context": ""
        }
        self.db.create_hypothesis(current_user_id, hypothesis_data)
        return {"summary": summary, "graph": causal_graph}, 201
    
    @token_required
    def delete(self, current_user_id):
        hypothesis_id = request.args.get('hypothesis_id')
        if hypothesis_id:
            return self.db.delete_hypothesis(current_user_id, hypothesis_id)
        return {"message": "hypothesis id is required!"}
    
class ChatAPI(Resource):
    def __init__(self, llm):
        self.llm = llm

    def post(self):
        query = request.form.get('query')
        graph = request.form.get('graph')
        response = self.llm.chat(query, graph)
        response = {"response": response}
        return response
