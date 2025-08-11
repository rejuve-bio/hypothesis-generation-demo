import marimo

__generated_with = "0.1.0"
app = marimo.App()

@app.cell
def __():
    import marimo as mo
    import subprocess
    import os
    import pandas as pd
    import numpy as np
    from pathlib import Path
    import sys
    
    mo.md("""
    # LDSC Cell-Type-Specific Heritability Analysis
    
    LDSC analysis for BMI using GTEx multi-tissue gene expression data.
    
    ## Prerequisites
    - LDSC software installed
    - Reference data downloaded (1000G Phase 3, baseline LD scores)
    - GTEx multi-tissue gene expression LD scores
    - GWAS summary statistics (munged format)
    """)
    return mo, np, os, pd, subprocess, sys, Path

@app.cell
def cell_setup_dirs(Path, subprocess):
    BASE_DIR = Path("/mnt/hdd_1/rediet/hypothesis-generation-demo/ldsc_ph")
    LDSC_DIR = BASE_DIR / "ldsc"
    LDSC_REPO_DIR = LDSC_DIR / "ldsc_repo"

    ldsc_script = LDSC_REPO_DIR / "ldsc.py"
    if not ldsc_script.exists():
        print("Cloning LDSC repository...")
        LDSC_REPO_DIR.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            "git", "clone", "https://github.com/bulik/ldsc.git", str(LDSC_REPO_DIR)
        ], check=True)
        print(f"LDSC repository cloned successfully to {LDSC_REPO_DIR}")
        
        req_file = LDSC_REPO_DIR / "requirements.txt"
        if req_file.exists():
            print("Installing Python dependencies from requirements.txt...")
            subprocess.run([
                "pip", "install", "-r", str(req_file)
            ], check=True)
        else:
            print("No requirements.txt found in the cloned repo.")
    else:
        print("LDSC repository already exists")
    if ldsc_script.exists():
        print(f"Found ldsc.py at: {ldsc_script}")
    else:
        print("ERROR: ldsc.py still not found after cloning!")

   
    RESULTS_DIR = BASE_DIR / "results"
    RESULTS_DIR.mkdir(exist_ok=True)

    return (
        BASE_DIR,
        LDSC_DIR,
        LDSC_REPO_DIR,
        RESULTS_DIR,
        ldsc_script,
    )

@app.cell
def cell_copy_ldsc_script(LDSC_DIR, LDSC_REPO_DIR, subprocess):
    source_dir = LDSC_DIR / "source"
    source_dir.mkdir(exist_ok=True)
    source_script = source_dir / "ldsc.py"
    cloned_script = LDSC_REPO_DIR / "ldsc.py"
    
    if not source_script.exists() and cloned_script.exists():
        subprocess.run([
            "cp", str(cloned_script), str(source_script)
        ], check=True)
        print(f"Copied ldsc.py to {source_script}")
    
 
    source_ldscore = source_dir / "ldscore"
    cloned_ldscore = LDSC_REPO_DIR / "ldscore"
    
    if not source_ldscore.exists() and cloned_ldscore.exists():
        subprocess.run([
            "cp", "-r", str(cloned_ldscore), str(source_ldscore)
        ], check=True)
        print(f"Copied ldscore module to {source_ldscore}")
    if source_script.exists():
        subprocess.run(["chmod", "+x", str(source_script)], check=True)
        print("Made ldsc.py executable")
    
    return source_script,

@app.cell
def __(mo, os):
    config = {
        'ldsc_dir': '/mnt/hdd_1/rediet/hypothesis-generation-demo/ldsc_ph/ldsc',
        nn: '/mnt/hdd_1/rediet/hypothesis-generation-demo/ldsc_ph/results',
        
    
        'gwas_file': '21001_munged.gwas.imputed_v3.both_sexes.tsv',
        'baseline_ld': '1000G_Phase3_baselineLD_ldscores/baselineLD.',
        'weights_ld': '1000G_Phase3_weights_hm3_no_MHC/weights.hm3_noMHC.',
        'cts_file': 'Multi_tissue_gene_expr_gtex.ldcts',
        
     
        'output_prefix': 'current_Mock_BMI_Multi_tissue_gtex',
        'ldsc_script': 'source/ldsc.py'
    }
    
    mo.md(f"""
    ## Current Configuration:
    - **LDSC Directory**: `{config['ldsc_dir']}`
    - **Results Directory**: `{config['results_dir']}`
    - **GWAS File**: `{config['gwas_file']}`
    - **Output Prefix**: `{config['output_prefix']}`
    """)
    return config,

@app.cell
def __(config, os, subprocess):
    def build_ldsc_command(cfg):
        """Build the LDSC command with all parameters"""
        cmd = [
            'python2', 
            os.path.join(cfg['ldsc_dir'], cfg['ldsc_script']),
            '--h2-cts', cfg['gwas_file'],
            '--ref-ld-chr', cfg['baseline_ld'],
            '--out', os.path.join(cfg['results_dir'], cfg['output_prefix']),
            '--ref-ld-chr-cts', cfg['cts_file'],
            '--w-ld-chr', cfg['weights_ld']
        ]
        return cmd
    
    def run_ldsc_analysis(cfg):
        """Execute the LDSC analysis with real-time output"""
        original_dir = os.getcwd()
        os.chdir(cfg['ldsc_dir'])
        
        try:
            cmd = build_ldsc_command(cfg)
            
            print("Running LDSC command:")
            print(" ".join(cmd))
            print("\n" + "="*80 + "\n")
            print("Analysis in progress... This may take a few minutes.")
            print("Processing tissues:")
            
            import time
            start_time = time.time()
            
    
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=cfg['ldsc_dir'],
                bufsize=1,
                universal_newlines=True
            )
            
            output_lines = []
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    print(output.strip())
                    output_lines.append(output.strip())
            
          
            rc = process.poll()
            elapsed_time = time.time() - start_time
            
            print(f"\nAnalysis completed in {elapsed_time:.2f} seconds")
            
            if rc != 0:
                print(f"Command failed with return code: {rc}")
            else:
                print("Analysis completed successfully!")
                
           
            class MockResult:
                def __init__(self, returncode, stdout):
                    self.returncode = returncode
                    self.stdout = "\n".join(stdout)
                    self.stderr = ""
                    
            return MockResult(rc, output_lines)
            
        finally:
            
            os.chdir(original_dir)
    
    ldsc_cmd = build_ldsc_command(config)
    print("LDSC Command to be executed:")
    print(" ".join(ldsc_cmd))
    return build_ldsc_command, ldsc_cmd, run_ldsc_analysis

@app.cell
def __(config, os, run_ldsc_analysis):
    analysis_result = None

    required_files = [
        config['gwas_file'],
        config['cts_file']
    ]
    
    missing_files = []
    for required_file in required_files:
        if not os.path.exists(os.path.join(config['ldsc_dir'], required_file)):
            missing_files.append(required_file)
    
    if missing_files:
        print("Missing required files:")
        for missing_file in missing_files:
            print(f"  - {missing_file}")
    else:
        print("Starting LDSC analysis automatically...")
        analysis_result = run_ldsc_analysis(config)
    
    analysis_result
    return analysis_result, missing_files, required_files

@app.cell
def __(os, pd, config):
    RESULT_FILE = os.path.join(config["results_dir"], "current_Mock_BMI_Multi_tissue_gtex.cell_type_results.txt")
    OUTPUT_FILE = os.path.join(config["results_dir"], "top10_gtex_current_mock_bmi_significant_tissues.tsv")

    if not os.path.exists(RESULT_FILE):
        print(f"Error: File '{RESULT_FILE}' not found.")
        ldsc_output_file = None
    else:
        df = pd.read_csv(RESULT_FILE, sep="\t")
        df_filtered = df[df.iloc[:, 3] < 0.01]
        df_sorted = df_filtered.sort_values(by=df.columns[1], ascending=False)
        top10_df = df_sorted.head(10)
        top10_df.to_csv(OUTPUT_FILE, sep="\t", index=False)
        print(f"Top 10 significant tissues saved to '{OUTPUT_FILE}'")
        ldsc_output_file = OUTPUT_FILE

    return ldsc_output_file, RESULT_FILE, OUTPUT_FILE

@app.cell
def __():
    import requests
    import json
    from pronto import Ontology
    import warnings
    
    # Suppress SyntaxWarnings from pronto library
    warnings.filterwarnings("ignore", category=SyntaxWarning, module="pronto")
    
    return json, requests, Ontology

@app.cell
def __(json, os, requests):
    def download_file(url, filename):
        """Download file only if it doesn't exist locally"""
        if os.path.exists(filename):
            print(f"{filename} already exists, skipping download")
            return
        
        print(f"Downloading {filename}...")
        response = requests.get(url)
        response.raise_for_status()
        with open(filename, 'wb') as file_handle:
            file_handle.write(response.content)
        print(f"Downloaded {filename}")

    def download_json_file(url, filename):
        """Download JSON file only if it doesn't exist locally"""
        if os.path.exists(filename):
            print(f"{filename} already exists, loading from local file")
            with open(filename, 'r') as file_handle:
                data = json.load(file_handle)
            return data
        
        print(f"Downloading {filename}...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        with open(filename, 'w') as file_handle:
            json.dump(data, file_handle, indent=4)
        print(f"Downloaded {filename}")
        return data
    return download_file, download_json_file

@app.cell
def __():
    def create_cellxgene_mapping_from_tissue_descendants(tissue_descendants_data):
        """Create mapping from tissue descendants data - includes both parent tissues and their descendants"""
        cellxgene_uberon_map = {}
        
        for parent_uberon_id in tissue_descendants_data.keys():
            cellxgene_uberon_map[parent_uberon_id] = parent_uberon_id
        
        for parent_uberon_id, descendants in tissue_descendants_data.items():
            if isinstance(descendants, list):
                for descendant_id in descendants:
                    cellxgene_uberon_map[descendant_id] = parent_uberon_id  
        
        print(f"Created mapping for {len(cellxgene_uberon_map)} UBERON IDs (including descendants)")
        return cellxgene_uberon_map

    def get_tissue_name_from_ontology(uberon_id, ontology):
        print("ontology name from get tissue name from ontology:", ontology)
        """Get the human-readable tissue name from UBERON ID using ontology"""
        if not ontology:
            return None
        
        try:
            term = ontology[uberon_id]
            return term.name
        except KeyError:
            return None
        except Exception as e:
            print(f"Error getting tissue name for {uberon_id}: {e}")
            return None

    def map_gtex_to_cellxgene_tissue(gtex_tissue_name, gtex_uberon_map, cellxgene_uberon_map, ontology):
        """Map GTEx tissue to CellxGene tissue using UBERON ontology"""
        print(f"\n--- Mapping GTEx: '{gtex_tissue_name}' ---")

        gtex_uberon_id = gtex_uberon_map.get(gtex_tissue_name)
        if not gtex_uberon_id:
            return None, "no_direct_uberon_found", f"No direct UBERON ID found for GTEx tissue '{gtex_tissue_name}' in mapping."

        print(f"GTEx UBERON ID: {gtex_uberon_id}")
        print(f"Checking for {gtex_uberon_id} in tissue descendants...")
        

        if gtex_uberon_id in cellxgene_uberon_map:
            mapped_parent = cellxgene_uberon_map[gtex_uberon_id]
            if mapped_parent == gtex_uberon_id:
                print(f"Direct match found: {gtex_uberon_id} exists as parent tissue")
                tissue_name = get_tissue_name_from_ontology(gtex_uberon_id, ontology)
                notes = f"Direct UBERON ID match found: {gtex_uberon_id}"
                if tissue_name:
                    notes += f" ({tissue_name})"
                return gtex_uberon_id, "direct", notes
            else:
                print(f"Descendant match found: {gtex_uberon_id} is a descendant of {mapped_parent}")
                gtex_tissue_name_ont = get_tissue_name_from_ontology(gtex_uberon_id, ontology)
                parent_tissue_name = get_tissue_name_from_ontology(mapped_parent, ontology)
                notes = f"GTEx tissue '{gtex_uberon_id}'"
                if gtex_tissue_name_ont:
                    notes += f" ({gtex_tissue_name_ont})"
                notes += f" is a descendant of CellxGene tissue '{mapped_parent}'"
                if parent_tissue_name:
                    notes += f" ({parent_tissue_name})"
                return mapped_parent, "descendant", notes
        else:
            print(f"No direct match: {gtex_uberon_id} not found in tissue descendants")


        if ontology:
            try:
                gtex_term = ontology[gtex_uberon_id]
                print(f"GTEx UBERON Term Name: {gtex_term.name}")
                gtex_ancestor_ids = set()
                try:
                    for ancestor_term in gtex_term.superclasses(with_self=True):
                        gtex_ancestor_ids.add(str(ancestor_term.id))
                except AttributeError:
                    gtex_ancestor_ids.add(str(gtex_term.id))
                    print("Warning: Could not retrieve ancestors, using only the term itself")

                print(f"Found {len(gtex_ancestor_ids)} ancestor terms")

                best_match = None
                match_level = float('inf')
                best_notes = ""

                for cellxgene_uberon_id in cellxgene_uberon_map.keys():
                    if cellxgene_uberon_id in gtex_ancestor_ids:
                        try:
                            cellxgene_term = ontology[cellxgene_uberon_id]
                            distance = 0
                            
                            if cellxgene_uberon_id == gtex_uberon_id:
                                distance = 0
                            else:
                                distance = 1
                            
                            if distance < match_level:
                                best_match = cellxgene_uberon_id
                                match_level = distance
                                best_notes = f"Broader match found: GTEx UBERON ID '{gtex_uberon_id}' ({gtex_term.name}) is related to CellxGene UBERON ID '{cellxgene_uberon_id}' ({cellxgene_term.name}). Distance: {distance}"

                        except KeyError:
                            continue

                if best_match:
                    return best_match, "broader_match_found", best_notes
                else:
                    return None, "no_cellxgene_match", f"No suitable CellxGene Census tissue (direct or broader) found for GTEx UBERON ID '{gtex_uberon_id}'."

            except KeyError:
                return None, "no_uberon_in_ontology", f"UBERON ID '{gtex_uberon_id}' not found in the loaded ontology."
            except Exception as e:
                return None, "ontology_error", f"Error during ontology traversal: {e}"
        else:
            return None, "ontology_not_loaded", "UBERON ontology not loaded, cannot perform hierarchical mapping."
    return (
        create_cellxgene_mapping_from_tissue_descendants,
        get_tissue_name_from_ontology,
        map_gtex_to_cellxgene_tissue,
    )

@app.cell
def __(
    Ontology,
    create_cellxgene_mapping_from_tissue_descendants,
    download_file,
    download_json_file,
    get_tissue_name_from_ontology,
    json,
    map_gtex_to_cellxgene_tissue,
    os,
):
    print("Starting the main execution")
    uberon_url = "http://purl.obolibrary.org/obo/uberon.owl"
    uberon_filename = "uberon.owl"
    tissue_descendants_url = "https://raw.githubusercontent.com/chanzuckerberg/cellxgene-ontology-guide/latest/ontology-assets/tissue_descendants.json"
    tissue_descendants_filename = "tissue_descendants.json"

    download_file(uberon_url, uberon_filename)
    tissue_descendants_data = download_json_file(tissue_descendants_url, tissue_descendants_filename)


    gtex_uberon_mapping = {
        'Brain_Putamen_basal_ganglia': 'UBERON:0001874'
    }


    cellxgene_uberon_mapping = create_cellxgene_mapping_from_tissue_descendants(tissue_descendants_data)

    uberon_ontology = None
    if os.path.exists(uberon_filename):
        try:
            print("Loading UBERON ontology... This may take a moment.")
            uberon_ontology = Ontology(uberon_filename)
            print("UBERON ontology loaded successfully.")
        except Exception as e:
            print(f"Error loading UBERON ontology: {e}")
            print("Continuing without ontology - only direct matches will be found.")
    else:
        print(f"UBERON ontology file '{uberon_filename}' not found. Only direct matches will be found.")

    ontology_mapping_results = {}

    for gtex_tissue_to_map in ['Brain_Putamen_basal_ganglia']:
       
        gtex_uberon_id = gtex_uberon_mapping.get(gtex_tissue_to_map)
        gtex_ontology_name = get_tissue_name_from_ontology(gtex_uberon_id, uberon_ontology) if gtex_uberon_id else None


        mapped_parent_id, match_type, notes = map_gtex_to_cellxgene_tissue(
            gtex_tissue_to_map,
            gtex_uberon_mapping,
            cellxgene_uberon_mapping,
            uberon_ontology
        )
        parent_ontology_name = get_tissue_name_from_ontology(mapped_parent_id, uberon_ontology) if mapped_parent_id else None


        if mapped_parent_id and gtex_uberon_id and mapped_parent_id != gtex_uberon_id:
            descendant_id = gtex_uberon_id
            descendant_name = gtex_ontology_name
        else:
            descendant_id = None
            descendant_name = None

        ontology_mapping_results[gtex_tissue_to_map] = {
            "gtex_tissue_name": gtex_tissue_to_map,
            "gtex_uberon_id": gtex_uberon_id,
            "gtex_ontology_name": gtex_ontology_name,
            "cellxgene_parent_uberon_id": mapped_parent_id,
            "cellxgene_parent_ontology_name": parent_ontology_name,
            "cellxgene_descendant_uberon_id": descendant_id,
            "cellxgene_descendant_ontology_name": descendant_name,
            "match_type": match_type,
            "notes": notes
        }

        print(f"\nResult for '{gtex_tissue_to_map}':")
        print(json.dumps(ontology_mapping_results[gtex_tissue_to_map], indent=4))

    with open("all_gtex_cellxgene_detailed_results.json", "w") as output_file:
        json.dump(ontology_mapping_results, output_file, indent=4)

    print("Detailed results (with parent & descendant IDs) saved to gtex_cellxgene_detailed_results.json")
    
    return (
        cellxgene_uberon_mapping,
        gtex_uberon_mapping,
        ontology_mapping_results,
        tissue_descendants_data,
        uberon_ontology,
        uberon_filename,
        tissue_descendants_filename
    )

@app.cell
def __():
    # Import additional packages for CellxGene analysis
    import cellxgene_census
    import pickle
    from scipy.stats import pearsonr
    import gseapy as gp
    
    return cellxgene_census, pickle, pearsonr, gp

@app.cell
def __(cellxgene_census, np, pd, pearsonr):
    class CellxgeneMock:
        def get_coexpression_matrix(self, gene, tissue, cell_type, k=500):
            with cellxgene_census.open_soma() as census:
                adata = cellxgene_census.get_anndata(
                    census=census,
                    organism="Homo sapiens",
                    obs_value_filter = f"tissue == '{tissue}'",
                    obs_column_names=["assay", "cell_type", "tissue", "tissue_general", "suspension_type", "disease"]

                )

                if 'feature_id' in adata.var.columns:
                    print("feature id is found inside the data")
                    adata.var_names = adata.var['feature_id']
                else:
                    print("Gene names column 'feature_id' not found in var DataFrame")

                gene_expression_sum = np.array((adata.X > 0).sum(axis=0)).flatten()
                adata_filtered = adata[:, gene_expression_sum > 0]
                genes = adata_filtered.var['feature_id']
                df_expression = pd.DataFrame(adata_filtered.X.toarray(), columns=genes)

                if gene in df_expression.columns:
                    non_zero_samples = df_expression[df_expression[gene] > 0]
                    total_samples = df_expression.shape[0]
                    non_zero_sample_count = non_zero_samples.shape[0]
                    non_zero_percentage = (non_zero_sample_count / total_samples) * 100

                    print(f"Total samples: {total_samples}")
                    print(f"Samples with non-zero expression for '{gene}': {non_zero_sample_count} ({non_zero_percentage:.2f}%)")

                    correlations = {}
                    for gene_col in non_zero_samples.columns:
                        if gene_col != gene:
                            corr, p_value = pearsonr(non_zero_samples[gene], non_zero_samples[gene_col])
                            if p_value < 0.05:
                                correlations[gene_col] = corr

                    sorted_correlations = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
                    top_positive = sorted_correlations[:k]
                    top_negative = sorted_correlations[-k:]
                    
                    # Save to file with unique tissue name
                    output_filename = f"top_positive_{tissue.replace(' ', '_').replace('/', '_')}.txt"
                    with open(output_filename, "w") as output_file: 
                        output_file.writelines([f"{gene_id}\t{corr:.4f}\n" for gene_id, corr in top_positive])

                    return top_positive, top_negative, genes
                else:
                    print(f"Gene of interest '{gene}' not found in the dataset.")
                    return [], [], []

    return CellxgeneMock,

@app.cell
def __(CellxgeneMock, json, ontology_mapping_results):
    # CellxGene analysis configuration and execution
    gene_of_interest = 'ENSG00000140718'
    # gene_of_interest = 'ENSG00000177508'  # IRX3
    cell_type = 'preadipocyte'

    # Use the mapping results from the ontology mapping step
    cellxgene_analysis_mock = CellxgeneMock()
    cellxgene_coexp_results = {}
    
    for tissue_key, tissue_value in ontology_mapping_results.items():
        target_tissue = tissue_value["cellxgene_descendant_ontology_name"]
        if target_tissue:
            print(f"\nProcessing tissue: {target_tissue}")
            top_pos, top_neg, all_gene_ids = cellxgene_analysis_mock.get_coexpression_matrix(
                gene=gene_of_interest,
                tissue=target_tissue,
                cell_type=cell_type
            )
            
            cellxgene_coexp_results[target_tissue] = {
                'top_positive': top_pos,
                'top_negative': top_neg,
                'all_genes': all_gene_ids
            }
        else:
            print(f"No target tissue found for {tissue_key}")

    return cellxgene_coexp_results, gene_of_interest, cell_type, cellxgene_analysis_mock

@app.cell
def __(cellxgene_coexp_results, pickle, os):
    # Convert Ensembl IDs to HGNC symbols and prepare for pathway analysis
    hgnc_converted_results = {}
    
    try:
        ensembl_to_hgnc_map = pickle.load(open("../data/ensembl_to_hgnc.pkl", "rb"))
        
        # Process results for each tissue
        for tissue_name, coexp_results in cellxgene_coexp_results.items():
            top_positive_hgnc = [(ensembl_to_hgnc_map.get(gene_id, gene_id), corr) for gene_id, corr in coexp_results['top_positive']]
            top_negative_hgnc = [(ensembl_to_hgnc_map.get(gene_id, gene_id), corr) for gene_id, corr in coexp_results['top_negative']]
            all_genes_hgnc = [ensembl_to_hgnc_map.get(gene_id, gene_id) for gene_id in coexp_results['all_genes']]
            
            # Save positive correlations to file
            hgnc_output_filename = f"top_positive_hgnc_{tissue_name.replace(' ', '_').replace('/', '_')}.txt"
            with open(hgnc_output_filename, "w") as hgnc_file: 
                hgnc_file.writelines([f"{gene_symbol}\t{corr:.4f}\n" for gene_symbol, corr in top_positive_hgnc])
            
            hgnc_converted_results[tissue_name] = {
                'top_positive_hgnc': top_positive_hgnc,
                'top_negative_hgnc': top_negative_hgnc,
                'all_genes_hgnc': all_genes_hgnc
            }
        
        print("Successfully converted Ensembl IDs to HGNC symbols")
        conversion_successful = True
        
    except FileNotFoundError:
        print("Warning: ensembl_to_hgnc.pkl file not found. Using original Ensembl IDs.")
        hgnc_converted_results = cellxgene_coexp_results
        conversion_successful = False

    return hgnc_converted_results, conversion_successful

@app.cell
def __(gp, hgnc_converted_results):
    # Pathway enrichment analysis using GSEAPY
    pathway_library = "GO_Biological_Process_2023"
    organism = "Human"

    pathway_enrichment_results = {}
    for tissue_name, hgnc_results in hgnc_converted_results.items():
        try:
            print(f"\nPerforming pathway enrichment analysis for {tissue_name}...")
            
            gene_list = [gene_data[0] for gene_data in hgnc_results['top_positive_hgnc']]
            background = hgnc_results['all_genes_hgnc']
            
            if len(gene_list) > 0 and len(background) > 0:
                enrichment_res = gp.enrichr(gene_list=gene_list,
                                gene_sets=pathway_library,
                                background=background,
                                organism=organism,
                                outdir=None).results
                
                # Process results as in original code
                enrichment_res.drop("Gene_set", axis=1, inplace=True)
                enrichment_res.insert(1, "ID", enrichment_res["Term"].apply(
                    lambda x: x.split("(")[1].split(")")[0] if "(" in x and ")" in x else ""))
                enrichment_res["Term"] = enrichment_res["Term"].apply(lambda x: x.split("(")[0])
                enrichment_res = enrichment_res[enrichment_res["Adjusted P-value"] < 0.05]
                
                pathway_enrichment_results[tissue_name] = enrichment_res
                print(f"Found {len(enrichment_res)} significant pathways for {tissue_name}")
                print(enrichment_res.head())
            else:
                print(f"No genes found for analysis in {tissue_name}")
                
        except Exception as e:
            print(f"Error in pathway analysis for {tissue_name}: {e}")
            pathway_enrichment_results[tissue_name] = None

    return pathway_enrichment_results, organism


if __name__ == "__main__":
    app.run()