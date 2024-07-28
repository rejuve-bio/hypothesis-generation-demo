from flask import Flask, request, jsonify
from flask_restful import Resource, Api, reqparse
import jwt
from functools import wraps
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from uuid import uuid4
# Load environment variables from .env file
load_dotenv()

# JWT Secret Key
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")

def token_required(f):
    @wraps(f)
    def decorated(self, *args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'message': 'Token is missing!'}), 403
        
        try:
            # Remove 'Bearer' prefix if present
            if 'Bearer' in token:
                token = token.split()[1]
            
            data = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
            current_user_id = data['user_id']
        except Exception as e:
            return {'message': 'Token is invalid!'}, 403
        
        # Pass current_user_id and maintain other args
        return f(self, current_user_id, *args, **kwargs)
    return decorated
    
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
            token = jwt.encode({'user_id': user_id, 'exp': datetime.utcnow() + timedelta(hours=1)}, JWT_SECRET_KEY, algorithm="HS256")
            return jsonify({'token': token})

        return response, status

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
    def __init__(self, enrichr, prolog_query, llm, db):
        self.enrichr = enrichr
        self.prolog_query = prolog_query
        self.llm = llm
        self.db = db

    @token_required
    def get(self, current_user_id):
        # Get the hypothesis_id from the query parameters
        hypothesis_id = request.args.get('hypothesis_id')

        if hypothesis_id:
            # Fetch a specific hypothesis by hypothesis_id and user_id
            print("this is hypothesis id: ", hypothesis_id)
            hypothesis = self.db.get_hypotheses(current_user_id, hypothesis_id)
            if not hypothesis:
                return {"message": "Hypothesis not found or access denied."}, 404
            return hypothesis, 200

        # Fetch all hypotheses for the current user
        hypotheses = self.db.get_hypotheses(user_id=current_user_id)
        return hypotheses, 200

    @token_required
    def post(self, current_user_id):
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

        hypothesis_data = {
            "hypothesis_id": str(uuid4()),
            "variant_id": variant_id,
            "phenotype": phenotype,
            "causal_gene": causal_gene,
            "causal_graph": causal_graph,
            "summary": summary,
            "biological_context": ""
        }
        self.db.create_hypothesis(current_user_id, hypothesis_data)
        response = {"summary": summary, "graph": causal_graph}
        return response

    @token_required
    def delete(self, current_user_id):
        hypothesis_id = request.args.get('hypothesis_id')
        if hypothesis_id:
            return self.db.delete_hypothesis(current_user_id, hypothesis_id)
        return {"message": "hypothesis id is required!"}