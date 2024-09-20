import json
import pickle
from pengines.Builder import PengineBuilder
from pengines.Pengine import Pengine

class PrologQuery:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.pengine_builder = PengineBuilder(urlserver=f"http://{self.host}:{self.port}")
        # self.pengine = Pengine(builder=pengine_builder)
        # self.pengine.create()

    def get_candidate_genes(self, variant_id):
        """
        Given a SNP, get candidate genes that are proximal to it.
        """
        query = f"candidate_genes(snp({variant_id}), Genes)"
        pengine = Pengine(builder=self.pengine_builder)
        pengine.doAsk(pengine.ask(query))
        result = pengine.currentQuery.availProofs[0]
        genes = [g.upper() for g in result["Genes"]]
        return genes
    
    def get_relevant_gene_proof(self, variant_id, gene):
        gene_id = self.get_gene_ids([gene])[0]
        query  = f"json_proof_tree(relevant_gene(gene({gene_id}), snp({variant_id})), Graph)"
        print("this is prolog query: ", query)
        pengine = Pengine(builder=self.pengine_builder)
        pengine.doAsk(pengine.ask(query))
        graph = pengine.currentQuery.availProofs[0]["Graph"]
        # pengine.doStop()
        return json.loads(graph)
    
    def get_gene_ids(self, genes):
        genes = [g.lower() for g in genes]
        query = f"maplist(gene_id, {genes}, GeneIds)"
        pengine = Pengine(builder=self.pengine_builder)
        pengine.doAsk(pengine.ask(query))
        gene_ids = pengine.currentQuery.availProofs[0]["GeneIds"]
        # self.pengine.doStop()
        return gene_ids

    def execute_query(self, query):
        pengine = Pengine(builder=self.pengine_builder)
        pengine.doAsk(pengine.ask(query))
        return pengine.currentQuery.availProofs[0]["X"]
       
            
            
        


if __name__ == "__main__":
    prolog_query = PrologQuery(host="100.67.47.42", port=4242)
    result = prolog_query.execute_query("maplist(variant_id, [snp(rs1421085)], X)")
#     # result = prolog_query.get_candidate_genes("rs1421085")
    print(result)
            
