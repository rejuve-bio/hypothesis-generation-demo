from loguru import logger
import requests

class PrologQuery:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server = f"http://{self.host}:{self.port}"

    def get_candidate_genes(self, variant_id):
        """
        Given a SNP, get candidate genes that are proximal to it.
        """
        logger.info(f"Getting candidate genes for variant {variant_id}")
        payload = {"rsid": variant_id}
        res = requests.get(f"{self.server}/api/hypgen/candidate_genes", params=payload)
        if not res.ok:
            raise RuntimeError(f"get_relevant_gene_proof failed. Prolog server response: {res.text}")
        result = res.json()
        genes = [g.upper() for g in result["candidate_genes"]]
        return genes
    
    def get_relevant_gene_proof(self, variant_id, samples):
        payload = {"rsid": variant_id, "samples": samples}
        res = requests.get(f"{self.server}/api/hypgen", params=payload)
        if not res.ok:
            raise RuntimeError(f"get_relevant_gene_proof failed. Prolog server response: {res.text}")
        
        return res.json()
        