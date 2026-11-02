import os
import pandas as pd
import requests
import typer
import numpy as np
from loguru import logger
from tqdm import tqdm
from sklearn.model_selection import train_test_split

app = typer.Typer()


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

def per_chr_models(gold_standard: pd.DataFrame, chr: str, seed: int, 
                   output_dir: str, test_size: float = 0.2, 
                   host:str = "localhost", port:int = 4242):
    num_samples = gold_standard.shape[0]
    logger.info(f"Number of samples in gold standard for chr {chr}: {num_samples}")
    # Generate random indices for test set
    np.random.seed(seed)
    # train_indices = np.random.choice(num_samples, int(num_samples * (1 - test_size)), replace=False)
    # test_indices = np.setdiff1d(np.arange(num_samples), train_indices)

    # train_set = gold_standard.iloc[train_indices]
    # test_set = gold_standard.iloc[test_indices]
    train_set, test_set = train_test_split(gold_standard, test_size=test_size, random_state=seed, shuffle=True)
    logger.info(f"Number of samples in train set for chr {chr}: {train_set.shape[0]}")
    logger.info(f"Number of samples in test set for chr {chr}: {test_set.shape[0]}")

    train_pos_tbl, train_neg_tbl = {}, {}
    test_pos_tbl, test_neg_tbl = {}, {}
    for _, row in tqdm(train_set.iterrows()):
        get_model_per_row(row, host, port, train_pos_tbl, train_neg_tbl)
    
    for _, row in tqdm(test_set.iterrows()):
        get_model_per_row(row, host, port, test_pos_tbl, test_neg_tbl)
    
    # Make sure positive examples are disjoint between train and test
    train_pairs = set([f"{s}-{train_pos_tbl[s]}" for s in train_pos_tbl])
    test_pairs = set([f"{s}-{test_pos_tbl[s]}" for s in test_pos_tbl])
    
    intersec = train_pairs.intersection(test_pairs)
    
    if len(intersec) > 0:
        logger.warning(f"Found {len(intersec)} overlapping positive pairs between train and test")
        logger.info("Fixing it by removing them from test...")
        for pair in intersec:
            ls = pair.split("-")
            snp, gene = ls[0], ls[1]
            if snp in test_pos_tbl:
                test_pos_tbl.pop(snp, None)
                test_neg_tbl.pop(gene, None)
    
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
        i += 1
        for rsid in test_pos_tbl:
            gene = test_pos_tbl[rsid]
            cgenes = test_neg_tbl[rsid]
            f.write(f"relevant_gene({i}, gene({gene}), snp({rsid})).\n")
            i += 1
            for cgene in cgenes:
                f.write(f"neg(relevant_gene({i}, gene({cgene}), snp({rsid}))).\n")
                i += 1
                
    train_models = [j for j in range(1, k + 1)]
    test_models = [j for j in range(k + 1 , i)]
    
    # Write the model indices to a file
    with open(f"{output_dir}/train_models.txt", "w") as f:
        for model in train_models:
            f.write(f"{model}\n")

    with open(f"{output_dir}/test_models.txt", "w") as f:
        for model in test_models:
            f.write(f"{model}\n")


def prepare_model_interp(dataset: str, seed: int, output_dir: str, folds: int,
                         test_size: float = 0.2, host: str = "localhost", port: int = 4242):
    """
    Given a Gold standard datasets, prepare model interpretations with positive and negative samples.
    Negative examples are gene within 500kb of the variant that are not a causal gene.
    """
    if not os.path.exists(output_dir):
        raise ValueError(f"Output directory {output_dir} does not exist")

    allchr_gold_standard = pd.read_table(dataset)

    chrs = allchr_gold_standard["sentinel_variant.locus_GRCh38.chromosome"].unique()
    logger.info(f"Found {len(chrs)} chrs: {chrs}")
    np.random.seed(seed)
    chr_seeds = {c : np.random.choice(np.arange(100), folds, replace=False) for c in chrs}

    for c in ["16"]: # TODO: Remove me 
        gold_standard_df = allchr_gold_standard[allchr_gold_standard["sentinel_variant.locus_GRCh38.chromosome"] == c]
        chr_output_dir = os.path.join(output_dir, f"chr{c}")
        if not os.path.exists(chr_output_dir):
            os.makedirs(chr_output_dir, exist_ok=True)
        logger.info(f"Writing samples for chr: {c} ....")
        for i in range(folds):
            fold_output_dir = os.path.join(chr_output_dir, f"fold_{i}")
            if not os.path.exists(fold_output_dir):
                os.makedirs(fold_output_dir)

            per_chr_models(gold_standard_df, c, chr_seeds[c][i], fold_output_dir, test_size, host, port)
        logger.info(f"Done for chr: {chr}.")

    logger.info("Done.")

    
@app.command()
def main(
      dataset: str = typer.Option(..., "--dataset", "-d", help="Path to dataset"),
      seed: int = typer.Option(..., "--seed", "-s"),
      output_dir: str = typer.Option(..., "--output-dir", "-o"),
      folds: int = typer.Option(5, "--folds"),
      test_size: float = typer.Option(0.2, "--test-size"),
      host: str = typer.Option("localhost", "--host"),
      port: int = typer.Option(4242, "--port"),
  ):

    np.random.seed(seed)
    prepare_model_interp(dataset, seed, output_dir, folds, test_size, host, port,)

if __name__ == "__main__":
    typer.run(main)
