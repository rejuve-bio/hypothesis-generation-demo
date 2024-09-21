import argparse
from flask import Flask
from flask_restful import Resource, Api
from flask_socketio import SocketIO
from enrich import Enrich
from semantic_search import SemanticSearch
from llm import LLM
from db import Database
from query_swipl import PrologQuery
from api import EnrichAPI, HypothesisAPI, ChatAPI, SignupAPI, LoginAPI
import os
from flask_cors import CORS

def parse_arguments():
    args = argparse.ArgumentParser()
    args.add_argument("--port", type=int, default=5001)
    args.add_argument("--host", type=str, default="0.0.0.0")
    # LLM arguments
    # args.add_argument("--llm", type=str, default="meta-llama/Meta-Llama-3-8B-Instruct")
    args.add_argument("--embedding-model", type=str, default="w601sxs/b1ade-embed-kd")
    # args.add_argument("--temperature", type=float, default=1.0)
    # Prolog arguments
    args.add_argument("--swipl-host", type=str, default="100.67.47.42")
    args.add_argument("--swipl-port", type=int, default=4242)
    # Enrich arguments
    args.add_argument("--ensembl-hgnc-map", type=str, required=True)
    args.add_argument("--hgnc-ensembl-map", type=str, required=True)
    args.add_argument("--go-map", type=str, required=True)
    return args.parse_args()

def setup_api(args):
    app = Flask(__name__)
    CORS(app)
    api = Api(app)
    socketio = SocketIO(app)

    # Use environment variables
    mongodb_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("DB_NAME")

    db = Database(mongodb_uri, db_name)

    enrichr = Enrich(args.ensembl_hgnc_map, args.hgnc_ensembl_map, args.go_map)
    try:
        hf_token = os.environ["HF_TOKEN"]
    except KeyError:
        hf_token = None
    # semantic_search = SemanticSearch(args.embedding_model, hf_token=hf_token)
    prolog_query = PrologQuery(args.swipl_host, args.swipl_port)
    llm = LLM()
    api.add_resource(EnrichAPI, "/enrich", resource_class_kwargs={"enrichr": enrichr, "llm": llm, "prolog_query": prolog_query, "db": db})
    api.add_resource(HypothesisAPI, "/hypothesis", resource_class_kwargs={"enrichr": enrichr, "prolog_query": prolog_query, "llm": llm, "db": db})
    api.add_resource(ChatAPI, "/chat", resource_class_kwargs={"llm": llm})
    api.add_resource(SignupAPI, '/signup', resource_class_kwargs={'db': db})
    api.add_resource(LoginAPI, '/login', resource_class_kwargs={'db': db})
    return app, socketio

def main():
    args = parse_arguments()
    app, socketio = setup_api(args)
    socketio.run(app, host=args.host, port=args.port, debug=True)

if __name__ == "__main__":
    main()