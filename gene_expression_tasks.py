import os
import subprocess
import json
import pandas as pd
import numpy as np
from pathlib import Path
from prefect import task
from loguru import logger
import requests
from scipy.stats import pearsonr
from scipy import sparse
import cellxgene_census
import tiledbsoma as soma
import multiprocessing
import re
from config import Config
import shutil
import pickle

try:
    import warnings
    warnings.filterwarnings("ignore", category=SyntaxWarning, module=r"pronto(\.|$)")
    from pronto import Ontology
    warnings.filterwarnings("ignore", category=SyntaxWarning, module="pronto")
    PRONTO_AVAILABLE = True
except ImportError:
    logger.warning("Pronto not available")
    PRONTO_AVAILABLE = False


@task(log_prints=True)
def setup_ldsc_environment(base_dir):
    """LDSC is already installed in Docker, just return the path"""
    return "/opt/ldsc"

@task(log_prints=True)
def run_ldsc_analysis(ldsc_dir, gwas_file, output_prefix):
    """Run LDSC using the conda environment"""
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_prefix)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")
    
    # Convert to absolute paths
    gwas_file = os.path.abspath(gwas_file)
    output_prefix = os.path.abspath(output_prefix)
    
    # Use the LDSC data directory
    ldsc_data_dir = "data/ldsc"
    
    # Always copy the GWAS file to ensure we use the latest version
    gwas_filename = os.path.basename(gwas_file)
    local_gwas_path = os.path.join(ldsc_data_dir, gwas_filename)
    shutil.copy2(gwas_file, local_gwas_path)
    logger.info(f"Copied GWAS file to: {local_gwas_path}")
    
    # Use the wrapper script from Dockerfile
    cmd = [
        '/usr/local/bin/ldsc',  # Uses the wrapper script that activates conda env
        '--h2-cts', gwas_filename,
        '--ref-ld-chr', '1000G_Phase3_baselineLD_ldscores/baselineLD.',
        '--out', output_prefix,
        '--ref-ld-chr-cts', 'Multi_tissue_gene_expr_gtex.ldcts',
        '--w-ld-chr', '1000G_Phase3_weights_hm3_no_MHC/weights.hm3_noMHC.'
    ]
    
    original_dir = os.getcwd()
    os.chdir(ldsc_data_dir)
    
    try:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, universal_newlines=True
        )
        
        output_lines = []
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                logger.info(output.strip())
                output_lines.append(output.strip())
        
        rc = process.poll()
        if rc != 0:
            logger.error(f"LDSC command failed with return code: {rc}")
            raise RuntimeError(f"LDSC failed with return code: {rc}")
        
        logger.info("LDSC command completed successfully")
        return True
    finally:
        os.chdir(original_dir)


@task(log_prints=True)
def process_ldsc_results(results_dir, output_prefix, top_n=10):
    """Process LDSC results"""
    result_file = Path(results_dir) / f"{output_prefix}.cell_type_results.txt"
    
    if not result_file.exists():
        raise FileNotFoundError(f"LDSC results file not found: {result_file}")
    
    logger.info(f"Reading LDSC results from: {result_file}")
    df = pd.read_csv(result_file, sep="\t")
    df_filtered = df[df.iloc[:, 3] < 0.01]
    df_sorted = df_filtered.sort_values(by=df.columns[1], ascending=False)
    top_tissues_df = df_sorted.head(top_n)
    
    # Clean tissue names 
    top_tissues = [
        re.sub(r"_\(", "_", name).replace(")", "") 
        for name in top_tissues_df['Name'].tolist()
    ]
    
    logger.info(f"Processed {len(df)} total tissues, {len(df_filtered)} significant, returning top {len(top_tissues)}")
    return top_tissues, top_tissues_df.to_dict('records')


@task(log_prints=True)
def load_ontology_mappings(work_dir):
    """Load mappings - uses shared cache directory from config"""
    config = Config.from_env()
    cache_dir = Path(config.ontology_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    # Download ontology files to shared cache
    uberon_file = cache_dir / "uberon.owl"
    tissue_desc_file = cache_dir / "tissue_descendants.json"
    
    if not uberon_file.exists():
        logger.info("Downloading uberon.owl to shared cache...")
        response = requests.get("http://purl.obolibrary.org/obo/uberon.owl")
        response.raise_for_status()
        with open(uberon_file, 'wb') as f:
            f.write(response.content)
        logger.info(f"Saved uberon.owl to {uberon_file}")
    else:
        logger.info(f"Using cached uberon.owl from {uberon_file}")
    
    if not tissue_desc_file.exists():
        logger.info("Downloading tissue_descendants.json to shared cache...")
        response = requests.get("https://raw.githubusercontent.com/chanzuckerberg/cellxgene-ontology-guide/latest/ontology-assets/tissue_descendants.json")
        response.raise_for_status()
        tissue_descendants_data = response.json()
        with open(tissue_desc_file, 'w') as f:
            json.dump(tissue_descendants_data, f, indent=4)
        logger.info(f"Saved tissue_descendants.json to {tissue_desc_file}")
    else:
        logger.info(f"Using cached tissue_descendants.json from {tissue_desc_file}")
        with open(tissue_desc_file, 'r') as f:
            tissue_descendants_data = json.load(f)
    
    # Load GTEx mapping
    gtex_mapping_file = Path("data/ldsc/gtex_tissue_mappings_updated.tsv")
    
    if gtex_mapping_file.exists():
        # TSV format
        gtex_df = pd.read_csv(gtex_mapping_file, sep='\t')
        gtex_uberon_mapping = dict(zip(gtex_df.iloc[:, 0], gtex_df.iloc[:, 1]))
        logger.info(f"Loaded {len(gtex_uberon_mapping)} GTEx tissue mappings from TSV")
    else:
        # Dict format
        gtex_dict_file = Path("data/ldsc/gtex_uberon_mapping.txt")
        if gtex_dict_file.exists():
            mapping = {}
            with open(gtex_dict_file) as f:
                content = f.read().lstrip()
                exec(content, {}, mapping)
            gtex_uberon_mapping = mapping['gtex_uberon_mapping']
            logger.info(f"Loaded {len(gtex_uberon_mapping)} GTEx tissue mappings from dict file")
        else:
            raise FileNotFoundError("No GTEx mapping file found (tried TSV and dict formats)")
    
    # Create CellxGene mapping
    cellxgene_uberon_map = {}
    for parent_uberon_id in tissue_descendants_data.keys():
        cellxgene_uberon_map[parent_uberon_id] = parent_uberon_id
    
    for parent_uberon_id, descendants in tissue_descendants_data.items():
        if isinstance(descendants, list):
            for descendant_id in descendants:
                cellxgene_uberon_map[descendant_id] = parent_uberon_id
    
    # Load ontology
    uberon_ontology = None
    if PRONTO_AVAILABLE and uberon_file.exists():
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                uberon_ontology = Ontology(str(uberon_file))
            logger.info("UBERON ontology loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load ontology: {e}")
    
    return gtex_uberon_mapping, cellxgene_uberon_map, uberon_ontology


@task(log_prints=True)
def map_tissues_to_cellxgene(top_tissues, gtex_uberon_mapping, cellxgene_uberon_map, uberon_ontology):
    """Map tissues with full ontology traversal"""
    
    def get_tissue_name_from_ontology(uberon_id, ontology):
        if not ontology:
            return None
        try:
            term = ontology[uberon_id]
            return term.name
        except:
            return None
    
    def map_gtex_to_cellxgene_tissue(gtex_tissue_name, gtex_uberon_map, cellxgene_uberon_map, ontology):
        logger.info(f"\n--- Mapping GTEx: '{gtex_tissue_name}' ---")
        
        gtex_uberon_id = gtex_uberon_map.get(gtex_tissue_name)
        if not gtex_uberon_id:
            return None, "no_direct_uberon_found", f"No direct UBERON ID found for GTEx tissue '{gtex_tissue_name}'"
        
        logger.info(f"GTEx UBERON ID: {gtex_uberon_id}")
        
        # Convert format: UBERON_0001874 -> UBERON:0001874
        cellxgene_uberon_id = gtex_uberon_id.replace('_', ':')
        logger.info(f"Converted to CellxGene format: {cellxgene_uberon_id}")
        
        # Direct match check
        if cellxgene_uberon_id in cellxgene_uberon_map:
            mapped_parent = cellxgene_uberon_map[cellxgene_uberon_id]
            if mapped_parent == cellxgene_uberon_id:
                logger.info(f"Direct match found: {cellxgene_uberon_id}")
                tissue_name = get_tissue_name_from_ontology(cellxgene_uberon_id, ontology)  # Use converted ID
                notes = f"Direct UBERON ID match found: {cellxgene_uberon_id}"
                if tissue_name:
                    notes += f" ({tissue_name})"
                return cellxgene_uberon_id, "direct", notes
            else:
                logger.info(f"Descendant match found: {cellxgene_uberon_id} is descendant of {mapped_parent}")
                gtex_tissue_name_ont = get_tissue_name_from_ontology(cellxgene_uberon_id, ontology)  # Use converted ID
                parent_tissue_name = get_tissue_name_from_ontology(mapped_parent, ontology)
                notes = f"GTEx tissue '{cellxgene_uberon_id}'"
                if gtex_tissue_name_ont:
                    notes += f" ({gtex_tissue_name_ont})"
                notes += f" is a descendant of CellxGene tissue '{mapped_parent}'"
                if parent_tissue_name:
                    notes += f" ({parent_tissue_name})"
                return mapped_parent, "descendant", notes
        
        # Hierarchical search using ontology - use converted ID
        if ontology:
            try:
                gtex_term = ontology[cellxgene_uberon_id]  # Use converted ID 
                logger.info(f"GTEx UBERON Term Name: {gtex_term.name}")
                
                # Get all ancestors
                gtex_ancestor_ids = set()
                try:
                    for ancestor_term in gtex_term.superclasses(with_self=True):
                        gtex_ancestor_ids.add(str(ancestor_term.id))
                except AttributeError:
                    gtex_ancestor_ids.add(str(gtex_term.id))
                    logger.warning("Could not retrieve ancestors, using only the term itself")
                
                logger.info(f"Found {len(gtex_ancestor_ids)} ancestor terms")
                
                # Find best match among ancestors
                best_match = None
                match_level = float('inf')
                best_notes = ""
                
                for cellxgene_mapped_id in cellxgene_uberon_map.keys():
                    if cellxgene_mapped_id in gtex_ancestor_ids:
                        try:
                            cellxgene_term = ontology[cellxgene_mapped_id]
                            distance = 0 if cellxgene_mapped_id == cellxgene_uberon_id else 1
                            
                            if distance < match_level:
                                best_match = cellxgene_mapped_id
                                match_level = distance
                                best_notes = f"Broader match: GTEx '{cellxgene_uberon_id}' ({gtex_term.name}) related to CellxGene '{cellxgene_mapped_id}' ({cellxgene_term.name}). Distance: {distance}"
                        except KeyError:
                            continue
                
                if best_match:
                    return best_match, "broader_match_found", best_notes
                else:
                    return None, "no_cellxgene_match", f"No suitable CellxGene Census tissue found for GTEx UBERON ID '{cellxgene_uberon_id}'"
            
            except KeyError:
                return None, "no_uberon_in_ontology", f"UBERON ID '{cellxgene_uberon_id}' not found in ontology"  # Use converted ID in error message
            except Exception as e:
                return None, "ontology_error", f"Error during ontology traversal: {e}"
        else:
            return None, "ontology_not_loaded", "UBERON ontology not loaded, cannot perform hierarchical mapping"
    
    ontology_mapping_results = {}
    
    for gtex_tissue in top_tissues:
        gtex_uberon_id = gtex_uberon_mapping.get(gtex_tissue)
        gtex_ontology_name = get_tissue_name_from_ontology(gtex_uberon_id, uberon_ontology) if gtex_uberon_id else None
        
        mapped_parent_id, match_type, notes = map_gtex_to_cellxgene_tissue(
            gtex_tissue, gtex_uberon_mapping, cellxgene_uberon_map, uberon_ontology
        )
        
        parent_ontology_name = get_tissue_name_from_ontology(mapped_parent_id, uberon_ontology) if mapped_parent_id else None
        
        # Determine descendant info
        if mapped_parent_id and gtex_uberon_id and mapped_parent_id != gtex_uberon_id:
            descendant_id = gtex_uberon_id
            descendant_name = gtex_ontology_name
        else:
            descendant_id = None
            descendant_name = None
        
        ontology_mapping_results[gtex_tissue] = {
            "gtex_tissue_name": gtex_tissue,
            "gtex_uberon_id": gtex_uberon_id,
            "gtex_ontology_name": gtex_ontology_name,
            "cellxgene_parent_uberon_id": mapped_parent_id,
            "cellxgene_parent_ontology_name": parent_ontology_name,
            "cellxgene_descendant_uberon_id": descendant_id,
            "cellxgene_descendant_ontology_name": descendant_name,
            "match_type": match_type,
            "notes": notes
        }
        
        logger.info(f"Result for '{gtex_tissue}': {json.dumps(ontology_mapping_results[gtex_tissue], indent=2)}")
    
    return ontology_mapping_results

def process_batch(start, *, batch_size, other_gene_joinids, other_genes,
                 sub_joinids, gene_expr_sub, census_version, sub_joinid_to_idx):
    """Batch processing function for co-expression"""
    with cellxgene_census.open_soma(census_version=census_version) as census:
        experiment = census["census_data"]["homo_sapiens"]
        end = min(start + batch_size, len(other_gene_joinids))
        batch_joinids = other_gene_joinids[start:end].tolist()
        batch_genes = other_genes[start:end]
        local_correlations = {}
        
        if not batch_joinids:
            return local_correlations
        
        batch_iter = experiment.ms["RNA"].X["raw"].read((sub_joinids.tolist(), batch_joinids)).tables()
        row_indices, col_indices, values_list = [], [], []
        batch_joinid_to_idx = {jid: idx for idx, jid in enumerate(batch_joinids)}

        for table_batch in batch_iter:
            cell_jids = table_batch["soma_dim_0"].to_numpy()
            gene_jids = table_batch["soma_dim_1"].to_numpy()
            vals = table_batch["soma_data"].to_numpy()
            
            if len(cell_jids) == 0 or len(gene_jids) == 0:
                continue
                
            cell_idxs = np.array([sub_joinid_to_idx.get(cjid, -1) for cjid in cell_jids], dtype=np.int32)
            gene_idxs = np.array([batch_joinid_to_idx.get(gjid, -1) for gjid in gene_jids], dtype=np.int32)
            valid = (cell_idxs != -1) & (gene_idxs != -1)
            
            row_indices.extend(cell_idxs[valid])
            col_indices.extend(gene_idxs[valid])
            values_list.extend(vals[valid])

        if row_indices:
            batch_matrix = sparse.coo_matrix(
                (values_list, (row_indices, col_indices)),
                shape=(len(gene_expr_sub), len(batch_joinids)),
                dtype=np.float32
            ).toarray()
        else:
            batch_matrix = np.zeros((len(gene_expr_sub), len(batch_joinids)), dtype=np.float32)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            for i, gene_id in enumerate(batch_genes):
                if np.var(batch_matrix[:, i], ddof=1) > 0:
                    try:
                        corr, p_value = pearsonr(gene_expr_sub, batch_matrix[:, i])
                        if p_value < 0.05 and not np.isnan(p_value):
                            local_correlations[gene_id] = corr
                    except:
                        continue

        return local_correlations

def get_coexpression_matrix_for_tissue(gene, tissue_uberon_id, cell_type=None, k=500):
    
    def get_coexpression_matrix(gene, tissue_uberon_id, cell_type, k=500, batch_size=1000, use_prefilter=False):
        with cellxgene_census.open_soma(census_version="2024-07-01") as census:
            experiment = census["census_data"]["homo_sapiens"]
            
            # Use tissue_ontology_term_id to filter by UBERON ID
            if cell_type:
                # Filter by both tissue and cell type
                value_filter = f"tissue_ontology_term_id == '{tissue_uberon_id}' and cell_type == '{cell_type}'"
            else:
                # Filter by tissue only
                value_filter = f"tissue_ontology_term_id == '{tissue_uberon_id}'"
            
            try:
                axis_query = experiment.axis_query(
                    measurement_name="RNA",
                    obs_query=soma.AxisQuery(value_filter=value_filter)
                )
            except ValueError:
                logger.warning(f"No cells found for tissue UBERON ID '{tissue_uberon_id}'")
                return [], [], []

            obs_joinids = axis_query.obs_joinids().to_numpy()

            if len(obs_joinids) > 100000:
                obs_joinids = obs_joinids[:100000] 
                n = 100000
            else:
                n = len(obs_joinids)
                
            if n == 0:
                logger.warning(f"No cells found for tissue UBERON ID '{tissue_uberon_id}'")
                return [], [], []

            var_df = experiment.ms["RNA"].var.read(
                column_names=["soma_joinid", "feature_id"]
            ).concat().to_pandas().set_index("feature_id")
            genes = var_df.index.tolist()

            if gene not in genes:
                logger.warning(f"Gene of interest '{gene}' not found in dataset")
                return [], [], genes

            gene_joinid = var_df.loc[gene]["soma_joinid"]

            # Get gene expression
            gene_table_iter = experiment.ms["RNA"].X["raw"].read((obs_joinids.tolist(), [gene_joinid])).tables()
            gene_expr = np.zeros(n, dtype=np.float32)
            joinid_to_idx = {jid: idx for idx, jid in enumerate(obs_joinids)}
            
            for batch in gene_table_iter:
                obs_jids = batch["soma_dim_0"].to_numpy()
                values = batch["soma_data"].to_numpy()
                
                for obs_jid, value in zip(obs_jids, values):
                    if obs_jid in joinid_to_idx:
                        idx = joinid_to_idx[obs_jid]
                        gene_expr[idx] = value

            # Filter cells with non-zero expression
            nonzero_mask = gene_expr > 0
            if np.sum(nonzero_mask) < 10:
                logger.warning(f"Too few cells with non-zero expression for gene '{gene}' in tissue UBERON ID '{tissue_uberon_id}'")
                return [], [], genes

            sub_joinids = obs_joinids[nonzero_mask]
            gene_expr_sub = gene_expr[nonzero_mask]

            # Get other genes
            mask = np.array([g != gene for g in genes])
            other_gene_joinids = var_df.loc[mask, "soma_joinid"].values
            other_genes = np.array(genes)[mask]

            # Pre-compute cell index mapping
            sub_joinid_to_idx = {jid: idx for idx, jid in enumerate(sub_joinids)}

            # Process correlations in batches
            correlations = {}
            num_genes = len(other_gene_joinids)
            batch_size = min(batch_size, 1000)
            
            for batch_start in range(0, num_genes, batch_size):
                batch_end = min(batch_start + batch_size, num_genes)
                batch_gene_joinids = other_gene_joinids[batch_start:batch_end]
                batch_genes = other_genes[batch_start:batch_end]
                
                # Get expression data for this batch
                batch_table_iter = experiment.ms["RNA"].X["raw"].read(
                    (sub_joinids.tolist(), batch_gene_joinids.tolist())
                ).tables()
                
                batch_expr = np.zeros((len(sub_joinids), len(batch_gene_joinids)), dtype=np.float32)
                
                for table in batch_table_iter:
                    obs_jids = table["soma_dim_0"].to_numpy()
                    gene_jids = table["soma_dim_1"].to_numpy()
                    values = table["soma_data"].to_numpy()
                    
                    for obs_jid, gene_jid, value in zip(obs_jids, gene_jids, values):
                        if obs_jid in sub_joinid_to_idx:
                            obs_idx = sub_joinid_to_idx[obs_jid]
                            gene_idx = np.where(batch_gene_joinids == gene_jid)[0]
                            if gene_idx.size > 0:
                                batch_expr[obs_idx, gene_idx[0]] = value
                
                # Calculate correlations for this batch
                for i, gene_symbol in enumerate(batch_genes):
                    other_expr = batch_expr[:, i]
                    if np.sum(other_expr > 0) >= 10:
                        try:
                            corr = np.corrcoef(gene_expr_sub, other_expr)[0, 1]
                            if not np.isnan(corr):
                                correlations[gene_symbol] = corr
                        except:
                            continue

            # Sort by correlation and get top k
            sorted_correlations = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
            
            top_positive = sorted_correlations[:k]
            top_negative = sorted_correlations[-k:]
            
            return top_positive, top_negative, genes

    # Run the analysis
    return get_coexpression_matrix(gene, tissue_uberon_id, cell_type, k)

@task(log_prints=True, cache_policy=None)
def run_combined_ldsc_tissue_analysis(gene_expression, projects_handler, munged_file, output_dir, project_id, user_id):
    """Combined LDSC + tissue analysis as part of main analysis pipeline"""
    analysis_run_id = None
    try:
        logger.info("[PIPELINE] Starting combined LDSC + tissue analysis")
        
        # Create work directory for LDSC
        ldsc_work_dir = f"{output_dir}/ldsc_analysis"
        os.makedirs(ldsc_work_dir, exist_ok=True)
        
        # Create analysis run record
        analysis_run_id = gene_expression.create_gene_expression_run(
            gwas_file=munged_file,
            gene_of_interest="project_analysis",  # Indicates project-level analysis
            project_id=project_id,
            user_id=user_id
        )
        
        # Update status to running
        gene_expression.update_gene_expression_run_status(analysis_run_id, 'running')
        
        # Step 1: Setup LDSC environment
        logger.info("[PIPELINE] Setting up LDSC environment...")
        ldsc_dir = setup_ldsc_environment.submit(ldsc_work_dir).result()
        
        # Step 2: Run LDSC analysis
        logger.info("[PIPELINE] Running LDSC heritability analysis...")
        output_prefix = f"{output_dir}/ldsc_project_analysis"
        ldsc_success = run_ldsc_analysis.submit(ldsc_dir, munged_file, output_prefix).result()
        
        if not ldsc_success:
            raise RuntimeError("LDSC analysis failed")
        
        # Step 3: Process LDSC results
        logger.info("[PIPELINE] Processing LDSC results...")
        top_tissues, ldsc_results_data = process_ldsc_results.submit(
            output_dir, "ldsc_project_analysis", 10
        ).result()
        
        # Step 4: Tissue mapping analysis
        logger.info("[PIPELINE] Running tissue mapping analysis...")
        work_dir = f"data/gene_expression/{user_id}/{project_id}"
        os.makedirs(work_dir, exist_ok=True)
        
        # Load ontology mappings
        gtex_uberon_mapping, cellxgene_uberon_map, uberon_ontology = load_ontology_mappings.submit(work_dir).result()
        
        # Map tissues to CellxGene format
        tissue_names = [t.get('Name', '') for t in ldsc_results_data]
        ontology_mapping_results = map_tissues_to_cellxgene.submit(
            tissue_names, gtex_uberon_mapping, cellxgene_uberon_map, uberon_ontology
        ).result()
        
        # Step 5: Save comprehensive results to database        
        gene_expression.save_ldsc_results(analysis_run_id, ldsc_results_data)
        
        # Save tissue mappings
        gene_expression.save_tissue_mappings(analysis_run_id, ontology_mapping_results)
        
        # Update status to completed
        gene_expression.update_gene_expression_run_status(analysis_run_id, 'ldsc_tissue_completed')
        
        significant_count = len([t for t in ldsc_results_data if t.get('Coefficient_P_value', 1) < 0.05])
        
        logger.info(f"[PIPELINE] LDSC + tissue analysis completed successfully!, Analyzed {len(ldsc_results_data)} tissues, found {significant_count} significant")
        
        return {
            "success": True,
            "analysis_run_id": analysis_run_id,
            "tissues_analyzed": len(ldsc_results_data),
            "significant_tissues": significant_count,
            "top_tissues": ldsc_results_data[:10]
        }
        
    except Exception as e:
        logger.error(f"[PIPELINE] Combined LDSC + tissue analysis failed: {str(e)}")
        if analysis_run_id:
            gene_expression.update_gene_expression_run_status(analysis_run_id, 'failed')
        raise