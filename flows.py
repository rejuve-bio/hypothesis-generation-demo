import asyncio
import os
import time
from flask import json, jsonify
from loguru import logger
import numpy as np
from prefect import flow, task
from prefect.deployments import run_deployment
from status_tracker import TaskState
from tasks import calculate_ld_for_regions, check_enrich, check_ld_dimensions, check_ld_semidefiniteness, download_and_prepare_vcfs, expand_snp_regions, extract_gene_types, formattating_credible_sets, generate_binary_from_vcf, generate_snplist_file, get_candidate_genes, get_credible_sets, get_gene_region_files, grouping_cojo, mapping_cojo, merge_plink_binaries, predict_causal_gene, get_relevant_gene_proof, retry_predict_causal_gene, retry_get_relevant_gene_proof, create_enrich_data, run_cojo_analysis
from tasks import check_hypothesis, get_enrich, get_gene_ids, execute_gene_query, execute_variant_query,summarize_graph, create_hypothesis, execute_phenotype_query
from tasks import load_gwas_data, preprocess_gwas_data, filter_significant_snps, prepare_cojo_file, extract_region_snps, run_susie_analysis
import pandas as pd
from uuid import uuid4
from datetime import datetime, timezone
from prefect.task_runners import ConcurrentTaskRunner

from utils import emit_task_update, get_analysis_state, get_user_file_path, save_analysis_state

### Enrichment Flow
@flow(log_prints=True, name="enrichment_flow")
async def enrichment_flow(enrichr, llm, prolog_query, db, current_user_id, phenotype, variant, hypothesis_id):
    try:
        enrich = check_enrich(db, current_user_id, phenotype, variant, hypothesis_id)
        if enrich:
            print("Retrieved enrich data from saved db")
            return {"id": enrich.get('id')}, 200

        candidate_genes = get_candidate_genes(prolog_query, variant, hypothesis_id)
        time.sleep(3)
        causal_gene = predict_causal_gene(llm, phenotype, candidate_genes, hypothesis_id)
        time.sleep(3)
        causal_graph, proof = get_relevant_gene_proof(prolog_query, variant, causal_gene, hypothesis_id)

        # mock causal_graph
        causal_graph = {'nodes': [{'id': 'rs1421085', 'type': 'snp'}, {'id': 'ensg00000177508', 'type': 'gene'}, {'id': 'rs1421085', 'type': 'snp'}, {'id': 'ensg00000177508', 'type': 'gene'}, {'id': 'rs1421085', 'type': 'snp'}, {'id': 'chr16_53741418_53785410_grch38', 'type': 'super_enhancer'}, {'id': 'chr16_53741418_53785410_grch38', 'type': 'super_enhancer'}, {'id': 'ensg00000140718', 'type': 'gene'}, {'id': 'rs1421085', 'type': 'snp'}, {'id': 'ensg00000125798', 'type': 'gene'}, {'id': 'ensg00000125798', 'type': 'gene'}, {'id': 'ensg00000177508', 'type': 'gene'}, {'id': 'ensg00000125798', 'type': 'gene'}, {'id': 'chr16_53744537_53744917_grch38', 'type': 'tfbs'}, {'id': 'chr16_53744537_53744917_grch38', 'type': 'tfbs'}, {'id': 'chr16_53741418_53785410_grch38', 'type': 'super_enhancer'}], 'edges': [{'source': 'rs1421085', 'target': 'ensg00000177508', 'label': 'eqtl_association'}, {'source': 'rs1421085', 'target': 'ensg00000177508', 'label': 'in_tad_with'}, {'source': 'rs1421085', 'target': 'chr16_53741418_53785410_grch38', 'label': 'in_regulatory_region'}, {'source': 'chr16_53741418_53785410_grch38', 'target': 'ensg00000140718', 'label': 'associated_with'}, {'source': 'rs1421085', 'target': 'ensg00000125798', 'label': 'alters_tfbs'}, {'source': 'ensg00000125798', 'target': 'ensg00000177508', 'label': 'regulates'}, {'source': 'ensg00000125798', 'target': 'chr16_53744537_53744917_grch38', 'label': 'binds_to'}, {'source': 'chr16_53744537_53744917_grch38', 'target': 'chr16_53741418_53785410_grch38', 'label': 'overlaps_with'}]}

        if causal_graph is None:
            causal_gene = retry_predict_causal_gene(llm, phenotype, candidate_genes, proof, causal_gene, hypothesis_id)
            causal_graph, proof = retry_get_relevant_gene_proof(prolog_query, variant, causal_gene, hypothesis_id)
            print("Retried causal gene: ", causal_gene)
            print("Retried causal graph: ", causal_graph)

        enrich_tbl = enrichr.run(causal_gene)
        relevant_gos = llm.get_relevant_go(phenotype, enrich_tbl)


        enrich_id = create_enrich_data(db, variant, phenotype, causal_gene, relevant_gos, causal_graph, current_user_id, hypothesis_id)
        
        return {"id": enrich_id}, 201
        
    except Exception as e:
        logger.error(f"Error in enrichment flow: {str(e)}")
        raise

### Hypothesis Flow
@flow(log_prints=True)
def hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id, db, prolog_query, llm):
    hypothesis = check_hypothesis(db, current_user_id, enrich_id, go_id, hypothesis_id)
    if hypothesis:
        print("Retrieved hypothesis data from saved db")
        return {"summary": hypothesis.get('summary'), "graph": hypothesis.get('graph')}, 200

    enrich_data = get_enrich(db, current_user_id, enrich_id, hypothesis_id)
    if not enrich_data:
        return {"message": "Invalid enrich_id or access denied."}, 404

    go_term = [go for go in enrich_data["GO_terms"] if go["id"] == go_id]
    go_name = go_term[0]["name"]
    causal_gene = enrich_data['causal_gene']
    variant_id = enrich_data['variant']
    variant = enrich_data['variant']
    phenotype = enrich_data['phenotype']
    coexpressed_gene_names = go_term[0]["genes"]
    causal_graph = enrich_data['causal_graph']

    print(f"Enrich data: {enrich_data}")

    # causal_gene_id = get_gene_ids(prolog_query, [causal_gene.lower()], hypothesis_id)[0]
    time.sleep(3)
    causal_gene_id = get_gene_ids(1, hypothesis_id)[0]
    time.sleep(3)
    coexpressed_gene_ids = get_gene_ids(2, hypothesis_id)
    # coexpressed_gene_ids = get_gene_ids(prolog_query, [g.lower() for g in coexpressed_gene_names], hypothesis_id)

    nodes, edges = causal_graph["nodes"], causal_graph["edges"]

    gene_nodes = [n for n in nodes if n["type"] == "gene"]
    gene_ids = [n['id'] for n in gene_nodes]
    gene_entities = [f"gene({id})" for id in gene_ids]
    query = f"maplist(gene_name, {gene_entities}, X)".replace("'", "")

    gene_names = execute_gene_query(prolog_query, query, hypothesis_id)
    for id, name, node in zip(gene_ids, gene_names, gene_nodes):
        node["id"] = id
        node["name"] = name.upper()
    
    variant_nodes = [n for n in nodes if n["type"] == "snp"]
    variant_rsids = [n['id'] for n in variant_nodes]
    variant_entities = [f"snp({id})" for id in variant_rsids]
    query = f"maplist(variant_id, {variant_entities}, X)".replace("'", "")

    time.sleep(3)
    variant_ids = execute_variant_query(prolog_query, query, hypothesis_id)
    for variant_id, rsid, node in zip(variant_ids, variant_rsids, variant_nodes):
        variant_id = variant_id.replace("'", "")
        node["id"] = variant_id
        node["name"] = rsid
        source_edges = [e for e in edges if e["source"] == rsid]
        target_edges = [e for e in edges if e["target"] == rsid]
        for edge in source_edges:
            edge["source"] = variant_id
        for edge in target_edges:
            edge["target"] = variant_id
            
    nodes.append({"id": go_id, "type": "go", "name": go_name})
    time.sleep(3)
    phenotype_id = execute_phenotype_query(prolog_query, phenotype, hypothesis_id)

    nodes.append({"id": phenotype_id, "type": "phenotype", "name": phenotype})
    edges.append({"source": go_id, "target": phenotype_id, "label": "involved_in"})
    for gene_id, gene_name in zip(coexpressed_gene_ids, coexpressed_gene_names):
        nodes.append({"id": gene_id, "type": "gene", "name": gene_name})
        edges.append({"source": gene_id, "target": go_id, "label": "enriched_in"})
        edges.append({"source": causal_gene_id, "target": gene_id, "label": "coexpressed_with"})


    causal_graph = {"nodes": nodes, "edges": edges}

    summary = summarize_graph(llm, causal_graph, hypothesis_id)

    
    hypothesis_id = create_hypothesis(db, enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id, hypothesis_id)

    
    return {"summary": summary, "graph": causal_graph}, 201

@task(cache_key_fn=None, cache_policy=None)
async def run_enrichment_task(enrichr, llm, prolog_query, db, current_user_id, phenotype, variant, hypothesis_id):
    """Async task to run the enrichment flow"""
    try:

        # Run the enrichment flow
        flow_result = await enrichment_flow(
            enrichr, 
            llm, 
            prolog_query, 
            db, 
            current_user_id, 
            phenotype, 
            variant, 
            hypothesis_id
        )
        
        # Update hypothesis with enrichment ID
        db.update_hypothesis(hypothesis_id, {
            "enrich_id": flow_result[0].get('id')
        })


        logger.info(f"Enrichment flow completed: {flow_result}")
        print(f"Enrichment flow completed: {flow_result}")
        
        return flow_result
    except Exception as e:
        logger.error(f"Enrichment flow failed: {str(e)}")
        
        # Update hypothesis with error state
        db.update_hypothesis(hypothesis_id, {
            "status": "failed",
            "error": str(e),
            "updated_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
        })

        # Emit failure update
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="enrichment",
            state=TaskState.FAILED,
            error=str(e),
            progress=0
        )
        raise

@flow(name="async_enrichment_process", task_runner=ConcurrentTaskRunner())
async def async_enrichment_process(enrichr, llm, prolog_query, db, current_user_id, phenotype, variant, hypothesis_id):
    """Wrapper flow to run enrichment process"""
    return await run_enrichment_task(
        enrichr, llm, prolog_query, db, 
        current_user_id, phenotype, variant, hypothesis_id
    )

@flow(log_prints=True)
def preprocessing_flow(current_user_id, population, gwas_file_path):
    """Flow to handle fine mapping"""

    POPULATION = population
    SAMPLE_PANEL_URL = "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/integrated_call_samples_v3.20130502.ALL.panel"
    OUTPUT_DIR = f"./data/susie/{current_user_id}"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 0: Load and preprocess GWAS data
    if gwas_file_path:
        file_path = gwas_file_path
    else:
        file_path = "./data/susie/gwas/21001_raw.gwas.imputed_v3.both_sexes.tsv.bgz"  # Default file
    
    print("Loading GWAS data")
    gwas_data_df = load_gwas_data(file_path)
    print("Preprocessing GWAS data")
    gwas_data_df = preprocess_gwas_data(gwas_data_df)

    
    # Filter significant SNPs
    print("Filtering significant SNPs")
    significant_snp_df = filter_significant_snps(gwas_data_df, output_dir=OUTPUT_DIR)


    # Step 1: Download and prepare VCF files
    print("Downloading and preparing VCF files")
    vcf_files = download_and_prepare_vcfs(
        output_dir=OUTPUT_DIR,
        population=POPULATION,
        sample_panel_url=SAMPLE_PANEL_URL,
    )

    # Step 2: Generate snplist file
    print("Generating snplist file")
    gwas_snplist_file = generate_snplist_file(
        gwas_snps=significant_snp_df,
        output_dir=OUTPUT_DIR
    )
    print("GWAS snplist file: ", gwas_snplist_file)
    
    # Step 3: Generate binary files from VCFs
    print("Generating binary files from VCFs")
    binary_files = generate_binary_from_vcf(
        vcf_files=vcf_files,
        gwas_snplist_file=gwas_snplist_file,
        output_dir=OUTPUT_DIR,
        population=POPULATION
    )
    print("Binary files: ", binary_files)
    
    # Step 4: Merge binary files
    print("Merging binary files")
    merged_binary = merge_plink_binaries(
        binary_files=binary_files,
        output_dir=OUTPUT_DIR,
        population=POPULATION
    )
    print("Merged binary file: ", merged_binary)


    # Step 5: Prepare COJO file
    print("Preparing COJO file")
    cojo_file_path = prepare_cojo_file(
        significant_snp_df=significant_snp_df, 
        output_dir=OUTPUT_DIR
    )
    
    print("merged binary file: ", merged_binary)
    print("cojo file path: ", cojo_file_path)

    # Step 6: Run COJO analysis
    print("Running COJO analysis")
    cojo_results_path = run_cojo_analysis(
        merged_binary_path=merged_binary,
        cojo_file_path=cojo_file_path,
        output_dir=OUTPUT_DIR,
        maf_threshold=0.05
    )
    print("COJO results path: ", cojo_results_path)
    # cojo_results_path = "./data/susie/cojo/all_chr/all_chr_cojo.jma.cojo"

    # Step 7: Expanding region of each independet snp identified by cojo
    print("Expanding SNP regions")
    expanded_regions = expand_snp_regions(
        cojo_results_path=cojo_results_path,
        significant_snp_df=significant_snp_df,
        output_dir=OUTPUT_DIR,
        window_size=500000
    )


    # Step 7.1: Map COJO results with gene type
    print("Mapping COJO results with gene type")
    mapped_cojo_results = mapping_cojo(cojo_results_path, output_dir=OUTPUT_DIR)
    print("Mapped COJO results: ", mapped_cojo_results)

    # # Step 7.2: Group mapped COJO results by gene type
    print("Grouping mapped COJO results by gene type")
    grouped_cojo_results = grouping_cojo(
        mapped_cojo_snps=mapped_cojo_results, 
        expanded_region_files=expanded_regions, 
        output_dir=OUTPUT_DIR
        )
    print("Grouped COJO results: ", grouped_cojo_results)

    # Save the state for the second flow
    analysis_state = {
        "merged_binary": merged_binary,
        "grouped_cojo_results": grouped_cojo_results,
        "population": population,
        "output_dir": OUTPUT_DIR
    }
    
    # Save the state to a JSON file
    state_dir = os.path.join('data', 'states', current_user_id)
    os.makedirs(state_dir, exist_ok=True)
    
    with open(os.path.join(state_dir, 'analysis_state.json'), 'w') as f:
        json.dump(analysis_state, f, default=str) 
    
    return extract_gene_types(grouped_cojo_results)

@flow(log_prints=True)
def finemapping_analysis_flow(current_user_id, selected_genes):
    """Second flow: Analyze the selected gene(s)
    
    Args:
        current_user_id: The ID of the current user
        selected_genes: A list of gene types to analyze
        
    Returns:
        A dictionary of credible sets for each gene type
    """
    # Retrieve saved state from the first flow
    state = get_analysis_state(current_user_id)
    merged_binary = state["merged_binary"]
    grouped_cojo_results = state["grouped_cojo_results"]
    OUTPUT_DIR = state["output_dir"]
    
    # Dictionary to store results for each gene type
    all_results = {}
    
    # Process each gene type
    for selected_gene in selected_genes:
        print(f"Processing gene type: {selected_gene}")
        
        # Get the specific region files for the selected gene
        gene_region_files = get_gene_region_files(grouped_cojo_results, selected_gene)
        print(f"Gene region files for {selected_gene}: {gene_region_files}")
        
        if not gene_region_files:
            print(f"No region files found for gene type: {selected_gene}")
            all_results[selected_gene] = []
            continue

        # Step 8: Generate LD matrices for each expanded region
        print("Generating LD matrices")
        ld_dir = calculate_ld_for_regions(
            region_files=gene_region_files, 
            plink_bfile=merged_binary,
            output_dir=OUTPUT_DIR
        )
        print("LD matrices directory: ", ld_dir)

        region_file = gene_region_files[0]
        print("Region file: ", region_file)
        region_name = os.path.basename(region_file).split('_snps')[0]
        print("Region name: ", region_name)
        
        ld_file = f"{ld_dir}/{region_name}_snps_ld.ld"
        ld_r = pd.read_csv(ld_file, sep="\t", header=None)
        R_df = ld_r.values
        
        # Load the expanded region file
        expanded_region_snps = pd.read_csv(region_file, sep="\t")
        
        bim_file_path = f"{OUTPUT_DIR}/plink_binary/merged_{state['population'].lower()}.bim"
        
        # Step 9: Check dimensionality of LD matrices
        print("Checking LD dimensions")
        filtered_snp = check_ld_dimensions(R_df, expanded_region_snps, bim_file_path)
        
        # Step 10: Check if LD matrix is positive semi-definite
        print("Checking if LD matrix is positive semi-definite") 
        R_df = check_ld_semidefiniteness(R_df)
        
        # Step 11: Run SuSiE analysis
        print("Running SuSiE analysis")
        fit = run_susie_analysis(
            filtered_snp, 
            R_df,
            n=503,  
            L=10    
        )
        
        # Step 12: Format credible sets
        print("Formatting credible sets")
        credible_sets = formattating_credible_sets(filtered_snp, fit, R_df)
        
        # Store results for this gene type
        all_results[selected_gene] = credible_sets.to_dict(orient="records")
    
    return all_results

    