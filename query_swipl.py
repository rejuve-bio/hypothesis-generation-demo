from swiplserver import PrologMQI, PrologThread
import json
import pickle

class PrologQuery:

    def __init__(self, mqi_port, mqi_pass):
        self.mqi_port = mqi_port
        self.mqi_pass = mqi_pass

    def get_relevant_gene(self, variant_id):
        """
        Given a SNP get the genes which are relevant to it either via eqtl association or proximity.
        """

        with PrologMQI(launch_mqi=False, port=self.mqi_port, password=self.mqi_pass) as mqi:
            with mqi.create_thread() as prolog_thread:
                # result = prolog_thread.query("json_proof(natnum(s(s(0))), PT).")
                var = "G"
                result = prolog_thread.query(f"relevant_gene({var}, sequence_variant({variant_id})).")
                if result:
                    genes = []
                    for r in result:
                        gene = r[var]["args"][0]
                        genes.append(gene)

                    genes = [g.upper() for g in genes]
                    return genes

    def get_go_proof(self, go_term, variant_id, sig_genes, pval):

        """
        Given a GO term, SNP and genes that are significantly enriched in the GO term
         get the proof tree explaining the causal association between the go_term and the variant.
        """
        sig_genes = [g.lower() for g in sig_genes]
        go_term = f"go_{go_term.split(':')[-1]}"
        with PrologMQI(launch_mqi=False, port=self.mqi_port, password=self.mqi_pass) as mqi:
            with mqi.create_thread() as prolog_thread:
                result = prolog_thread.query(f"json_proof(relevant_go({go_term}, {variant_id}, {sig_genes}, {pval}), PT)")
                return json.loads(result[0]["PT"])


if __name__ == "__main__":
    prolog_query = PrologQuery(4242, "pass123")
    result = prolog_query.get_go_proof("GO:0045598", "rs1421085",
                                       ["ensg00000172216", "ensg00000170323"], 0.001)
    print(result)
            
