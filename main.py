# import eventlet
# eventlet.monkey_patch(select=True, socket=True)
import asyncio
import argparse
from flask import Flask, json
from flask_restful import Resource, Api
from flask_socketio import SocketIO
from loguru import logger

from enrich import Enrich
# from semantic_search import SemanticSearch
from llm import LLM
from db import Database
from query_swipl import PrologQuery
from api import (
    EnrichAPI, 
    HypothesisAPI, 
    ChatAPI, 
    BulkHypothesisDeleteAPI,
    init_socket_handlers,
    SummaryAPI
)
from dotenv import load_dotenv
import os
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from socketio_instance import socketio
from status_tracker import StatusTracker

def parse_arguments():
    args = argparse.ArgumentParser()
    args.add_argument("--port", type=int, default=5000)
    args.add_argument("--host", type=str, default="0.0.0.0")
    #LLM arguments
    # args.add_argument("--llm", type=str, default="meta-llama/Meta-Llama-3-8B-Instruct")
    args.add_argument("--embedding-model", type=str, default="w601sxs/b1ade-embed-kd")
    # args.add_argument("--temperature", type=float, default=1.0)
    #Prolog arguments
    args.add_argument("--swipl-host", type=str, default="localhost")
    args.add_argument("--swipl-port", type=int, default=4242)
    #Enrich arguments
    args.add_argument("--ensembl-hgnc-map", type=str, required=True)
    args.add_argument("--hgnc-ensembl-map", type=str, required=True)
    args.add_argument("--go-map", type=str, required=True)
    return args.parse_args()

def setup_api(args):
    load_dotenv()
 
    
    app = Flask(__name__)

    # JWT Configuration
    app.config['JWT_SECRET_KEY'] = os.getenv("JWT_SECRET_KEY")
    app.config['JWT_TOKEN_LOCATION'] = ['headers']
    app.config['JWT_HEADER_NAME'] = 'Authorization'
    app.config['JWT_HEADER_TYPE'] = 'Bearer'

    # Initialize JWTManager
    jwt = JWTManager(app)
    CORS(app, resources={r"/*": {"origins": ["*", "http://localhost:5173"]}}, supports_credentials=True, allow_headers=["Content-Type", "Authorization"]) 
    api = Api(app)

    # Initialize SocketIO with the app
    socketio.init_app(app)


    # Use environment variables
    mongodb_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("DB_NAME")

    db = Database(mongodb_uri, db_name)
    status_tracker = StatusTracker()
    status_tracker.initialize(db)

    # Register socket namespace
    # socketio.on_namespace(SocketNamespace('/', db))
    # socket_namespace = SocketNamespace('/', db)
    # socketio.on_namespace(socket_namespace)

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
    api.add_resource(ChatAPI, "/chat", resource_class_kwargs={"llm": llm, "db": db})
    api.add_resource(BulkHypothesisDeleteAPI, "/hypothesis/delete", resource_class_kwargs={"db": db})
    api.add_resource(SummaryAPI, "/summary", resource_class_kwargs={"db": db}) 

    # Initialize socket handlers AFTER socketio.init_app
    socket_namespace = init_socket_handlers(db)
    logger.info(f"Socket namespace initialized: {socket_namespace}")
    print(f"Socket namespace initialized: {socket_namespace}")
    
    return app, socketio


def main():
    args = parse_arguments()
    app, socketio = setup_api(args)


    socketio.run(
        app, 
        host=args.host, 
        port=args.port, 
        debug=True,
        use_reloader=False,
        allow_unsafe_werkzeug=True
    )
    

if __name__ == "__main__":
    main()
