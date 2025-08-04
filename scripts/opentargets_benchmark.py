__author__ = "Abdulrahman S. Omar <xabush@signularitynet.io>"
import os
import numpy as np
import pandas as pd
import typer
import requests
from urllib.parse import urlencode
from loguru import logger
from sklearn.metrics import precision_score, recall_score, balanced_accuracy_score
import pickle
from tqdm import tqdm

app = typer.Typer()


def extract_variant_gene_info(benchmark_df):
    res = []
    for i, row in benchmark_df.iterrows():
        variant = str(row["sentinel_variant.rsid"]).strip()
        pos = int(row["sentinel_variant.locus_GRCh38.position"])
        ref = row["sentinel_variant.alleles.reference"].strip().lower()
        alt = row["sentinel_variant.alleles.alternative"].strip().lower()
        gene_id = str(row["gold_standard_info.gene_id"]).strip().lower()
        trait_name = str(row["trait_info.reported_trait_name"]).strip().lower()
        res.append({
            "variant": variant,
            "pos": pos,
            "ref": ref,
            "alt": alt,
            "gene_id": gene_id,
            "trait_name": trait_name
        })
    return res


def evaluate_inference_rules(benchmark_df, host, port):
    url = f"http://{host}:{port}/api/hypgen"
    ground_truth_mappings = extract_variant_gene_info(benchmark_df)
    total = len(ground_truth_mappings)
    pred_res = {"rsid": [], "pos": [], "ref":[], "alt":[],
                "ground_truth": [], "trait": [], 
                "pred": [], "num_pred": []}
    for i, mapping in enumerate(tqdm(ground_truth_mappings)):
        try:    
            gene = mapping["gene_id"]
            trait = mapping["trait_name"]
            pos = mapping["pos"]
            ref = mapping["ref"]
            alt = mapping["alt"]    
            logger.info(f"Processing {i+1}/{total} samples - {gene} - {trait}, {pos}, {ref}, {alt}")
            # pass rsid to prolog
            req_param = {"pos": pos, "ref": ref, "alt": alt}
            response = requests.get(f"{url}?{urlencode(req_param)}", timeout=600)
            response.raise_for_status()
            response = response.json()
            result = response["response"]
            pred_res["rsid"].append(response["rsid"])
            pred_res["pos"].append(pos)
            pred_res["ref"].append(ref)
            pred_res["alt"].append(alt)
            pred_res["ground_truth"].append(gene)
            pred_res["trait"].append(trait)
            #query = f"chr(snp({rsid}), Result)"
            if len(result) == 0:
                # No proof found
                pred_res["pred"].append(0)
                pred_res["num_pred"].append(0)
            else:
                # check if gene is in the returned result
                if gene in result:
                    pred_res["pred"].append(1)
                else:
                    pred_res["pred"].append(0)
                pred_res["num_pred"].append(len(result))
        except Exception as e:
            logger.error(f"Error processing sample {i} - {gene} - {trait}, {pos}, {ref}, {alt}")
            logger.error(e)

    logger.info(f"Done processing {total} samples")

    return pred_res


# Write function to run swip



@app.command()
def main(benchmark: str, output: str, 
         host: str = "localhost", port: int = 4242):

    benchmark_df = pd.read_table(benchmark)
    # Get the output dir and save log there
    logger.add(f"{output}.log", rotation="10 MB")
    pred_res = evaluate_inference_rules(benchmark_df, host, port)
    # Save results to pickle
    import pickle
    with open(f"{output}.pkl", "wb") as f:
        pickle.dump(pred_res, f)
    pred_res_df = pd.DataFrame(pred_res)
    pred_res_df.to_csv(f"{output}.csv", index=False) 

if __name__ == "__main__":
    app()
