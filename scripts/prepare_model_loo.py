import os
import pandas as pd
import requests
import typer
import numpy as np
from loguru import logger
from tqdm import tqdm
from sklearn.model_selection import train_test_split

app = typer.Typer()

# This one takes chromosomes and generates leave-one-chromosome-out models.

def get_model_per_row(row, host, port, pos_table, neg_table):
    chr, pos = f'chr{row["sentinel_variant.locus_GRCh38.chromosome"]}', row["sentinel_variant.locus_GRCh38.position"]
    alt, ref = row["sentinel_variant.alleles.alternative"].lower(), row["sentinel_variant.alleles.reference"].lower()
    gene = row["gold_standard_info.gene_id"].lower()
    url = f"http://{host}:{port}/api/hypgen/candidate_genes/locus?chr={chr}&pos={pos}&ref={ref}&alt={alt}"
    response = requests.get(url).json()
    if response.get("error", None):
        logger.error(f"Got an error response from SWI-Prolog server. Response message:\n{response['error']}")
        return
    rsid, candidate_genes = response["rsid"], response["candidate_genes"] # update the candidate gene list for this snp if it is already in the table
    
    if rsid != "":
        curr_candidate_genes = []
        if rsid in pos_table and pos_table[rsid] == gene: # this relationship already exists, pass
            return None
        if rsid in neg_table and gene in neg_table[rsid]: # negative example aready exists, remove it
            curr_candidate_genes =  neg_table[rsid]
            curr_candidate_genes.remove(gene)
        
        pos_table[rsid] = gene
        # Add negative examples
        for cgene in candidate_genes:
            if cgene != gene and cgene not in curr_candidate_genes:
                curr_candidate_genes.append(cgene)
        
        neg_table[rsid] = curr_candidate_genes # update the negative examples

def chr_models(gold_standard: pd.DataFrame, output_dir: str,
                   train_idxs, test_idxs,
                   host:str = "localhost", port:int = 4242):

    train_pos_tbl, train_neg_tbl = {}, {}
    test_pos_tbl, test_neg_tbl = {}, {}
    for index in tqdm(train_idxs):
        row = gold_standard.iloc[index]
        get_model_per_row(row, host, port, train_pos_tbl, train_neg_tbl)
    
    for index in tqdm(test_idxs):
        row = gold_standard.iloc[index]
        get_model_per_row(row, host, port, test_pos_tbl, test_neg_tbl)
    
    i = 1
    k = 0
    with open(f"{output_dir}/models.pl", "w") as f:
        # f.write(":- discontiguous relevant_gene/3.\n")
        # f.write(":- discontiguous neg/1.\n")
        for rsid in train_pos_tbl:
            gene = train_pos_tbl[rsid]
            cgenes = train_neg_tbl[rsid]
            f.write(f"relevant_gene({i}, gene({gene}), snp({rsid})).\n")
            i += 1
            for cgene in cgenes:
                f.write(f"neg(relevant_gene({i}, gene({cgene}), snp({rsid}))).\n")
                i += 1
        k = i
        for rsid in test_pos_tbl:
            gene = test_pos_tbl[rsid]
            cgenes = test_neg_tbl[rsid]
            f.write(f"relevant_gene({i}, gene({gene}), snp({rsid})).\n")
            i += 1
            for cgene in cgenes:
                f.write(f"neg(relevant_gene({i}, gene({cgene}), snp({rsid}))).\n")
                i += 1
                
    train_models = [j for j in range(1, i + 1)]
    test_models = [j for j in range(i, k + 1)]
    
    # Write the model indices to a file
    with open(f"{output_dir}/train_models.txt", "w") as f:
        for model in train_models:
            f.write(f"{model}\n")

    with open(f"{output_dir}/test_models.txt", "w") as f:
        for model in test_models:
            f.write(f"{model}\n")


def prepare_model_interp(dataset: str,  output_dir: str, 
                         train_chrs, test_chrs, host: str = "localhost", port: int = 4242):
    """
    Given a Gold standard datasets, prepare model interpretations with positive and negative samples.
    Negative examples are gene within 500kb of the variant that are not a causal gene.
    """
    if not os.path.exists(output_dir):
        raise ValueError(f"Output directory {output_dir} does not exist")

    allchr_gold_standard = pd.read_table(dataset)

    chrs = allchr_gold_standard["sentinel_variant.locus_GRCh38.chromosome"].unique()
    # Make sure train_chrs and test_chrs are valid and disjoint
    for c in train_chrs + test_chrs:
        if c not in chrs:
            raise ValueError(f"Chromosome {c} not found in dataset chromosomes: {chrs}")
    for c in train_chrs:
        if c in test_chrs:
            raise ValueError(f"Chromosome {c} found in both train and test chromosomes")
    train_idxs = allchr_gold_standard[allchr_gold_standard["sentinel_variant.locus_GRCh38.chromosome"].isin(train_chrs)].index.tolist()
    test_idxs = allchr_gold_standard[allchr_gold_standard["sentinel_variant.locus_GRCh38.chromosome"].isin(test_chrs)].index.tolist()
    
    logger.info(f"Number of training samples for chrs {train_chrs}: {len(train_idxs)}")
    logger.info(f"Number of testing samples for chrs {test_chrs}: {len(test_idxs)}")
    
    # Write log file to record the train and test chromosomes
    with open(f"{output_dir}/train_test_chromosomes.txt", "w") as f:
        f.write(f"Train chromosomes: {train_chrs}\n")
        f.write(f"Test chromosomes: {test_chrs}\n")

    chr_models(allchr_gold_standard, output_dir,
                   train_idxs, test_idxs,
                   host, port)
    print("Model preparation complete.")

    
@app.command()
def main(
      dataset: str = typer.Option(..., "--dataset", "-d", help="Path to dataset"),
    #   seed: int = typer.Option(..., "--seed", "-s"),
      output_dir: str = typer.Option(..., "--output-dir", "-o"),
      chrs: str = typer.Option(..., "--chrs", "-c", help="Comma separated list of chromosomes LOO"),
      host: str = typer.Option("localhost", "--host"),
      port: int = typer.Option(4242, "--port"),
  ):

    chrs = chrs.split(",")
    if not os.path.exists(output_dir):
        logger.info(f"Output Directory {output_dir} doesn't exit, Creating it...")
        os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir}/input_chrs.txt", "w") as f:
        f.write(f"Input chrs: {chrs}")
    # Implement Leave-One-Chromosome-Out
    for i in range(len(chrs)):
        test_chrs = [chrs[i]]
        train_chrs = chrs[:i] + chrs[i+1:]
        chr_output_dir = os.path.join(output_dir, f"chr{test_chrs[0]}")
        if not os.path.exists(chr_output_dir):
            os.makedirs(chr_output_dir, exist_ok=True)
        print(f"Preparing models for LOO chromosome: {test_chrs[0]} ....")
        prepare_model_interp(dataset, chr_output_dir, train_chrs, test_chrs, host, port,)
        print(f"Done for LOO chromosome: {test_chrs[0]}.")

if __name__ == "__main__":
    typer.run(main)
