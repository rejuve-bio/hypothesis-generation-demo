import argparse
from flask import Flask
from flask_restful import Resource, Api
from enrich import Enrich
from llm import LLM
from query_swipl import PrologQuery
from api import EnrichAPI, HypothesisAPI

def parse_arguments():
    args = argparse.ArgumentParser()
    args.add_argument("--port", type=int, default=5000)
    args.add_argument("--host", type=str, default="localhost")
    #LLM arguments
    args.add_argument("--llm", type=str, default="llama3")
    args.add_argument("--embedding-model", type=str, default="w601sxs/b1ade-embed-kd")
    args.add_argument("--ollama-host", type=str, default="localhost")
    args.add_argument("--ollama-port", type=int, default=11434)
    args.add_argument("--temperature", type=float, default=1.0)
    args.add_argument("--request-timeout", type=int, default=120)
    #Prolog arguments
    # args.add_argument("--swipl_host", type=str, default="localhost")
    args.add_argument("--swipl-port", type=int, default=4242)
    args.add_argument("--swipl-pass", type=str, required=True)
    #Enrich arguments
    args.add_argument("--ensembl-hgnc-map", type=str, required=True)
    args.add_argument("--hgnc-ensembl-map", type=str, required=True)
    args.add_argument("--go-map", type=str, required=True)
    return args.parse_args()

def setup_api(args):
    app = Flask(__name__)
    api = Api(app)
    enrichr = Enrich(args.ensembl_hgnc_map, args.hgnc_ensembl_map, args.go_map)
    llm = LLM(args.llm, args.embedding_model, 
              ollama_host=args.ollama_host, ollama_port=args.ollama_port, 
              temperature=args.temperature, request_timeout=args.request_timeout)
    prolog_query = PrologQuery(args.swipl_port, args.swipl_pass)
    api.add_resource(EnrichAPI, "/enrich", resource_class_kwargs={"enrichr": enrichr, "llm": llm, "prolog_query": prolog_query})
    api.add_resource(HypothesisAPI, "/hypothesis", resource_class_kwargs={"enrichr": enrichr, "llm": llm, "prolog_query": prolog_query})
    return app

def main():
    args = parse_arguments()
    app = setup_api(args)
    app.run(host=args.host, port=args.port)

if __name__ == "__main__":
    main()