__author__ = "Abdulrahman S. Omar<xabush@signularitynet.io"
import os
import sys
import numpy as np
import pandas as pd
import tempfile
import subprocess
import shutil
import requests
#import cravat TODO: Uncomment this line
import sqlite3
from pathlib import Path
import time
import matplotlib.pyplot as plt
from sklearn.metrics import auc
import matplotlib.lines as mlines


def get_ucsc_track_data(track, chrom, start, end,
                         genome='hg38', max_retries=5):
    url = (
        f"https://api.genome.ucsc.edu/getData/track?"
        f"genome={genome};track={track};chrom={chrom};start={start};end={end}"
    )
    
    retry_count = 0
    wait_time = 1  # Initial wait time in seconds
    
    while retry_count <= max_retries:
        try:
            response = requests.get(url).json()
            if 'statusCode' not in response.keys() or response['statusCode'] != 500:
                return response
            
            print(f"API returned error 500, retrying ({retry_count+1}/{max_retries})...")
        except Exception as e:
            print(f"Request failed: {str(e)}, retrying ({retry_count+1}/{max_retries})...")
        
        if retry_count == max_retries:
            raise Exception(f"Failed to get data after {max_retries} retries")
        
        # Exponential backoff
        time.sleep(wait_time)
        wait_time *= 2  # Double the wait time for next retry
        retry_count += 1


def get_tfbs_data(chrom, start, end, tf):
    # print(f"Getting TFBS data for {chrom}:{start}-{end} for {tf}")
    track = "encRegTfbsClustered"
    data = get_ucsc_track_data(track, chrom,
                               start, end)
    results = []
    for tfbs in data[track]:
        chr = tfbs["chrom"]
        chr_start = tfbs["chromStart"]
        chr_end = tfbs["chromEnd"]
        tf_name = tfbs["name"].lower()
        if tf_name == tf:
            results.append({
                "id": f"{chr}_{chr_start}_{chr_end}_grch38",
                "chr": chr,
                "start": chr_start,
                "end": chr_end,
                "tf": tf_name
            })
        # print(f"chr: {chr}, start: {chr_start},",
        # f"end: {chr_end}",
        # f"tf: {tf_name}")
        #results.append({
        #    "id": f"{chr}_{start}_{end}_grch38",
        #    "chr": chr,
        #    "start": chr_start,
        #    "end": chr_end,
        #    "tf": tf_name
        #)

    return results


def get_motif_effect_data(chr, pos, rs_id, ref, alt):
    os.environ["MKL_SERVICE_FORCE_INTEL"] = "1"
    vcf_fmt = f"{chr}\t{pos}\t{rs_id}\t{ref.upper()}\t{alt.upper()}"
    # print(vcf_fmt)
    vcf_tmp = tempfile.NamedTemporaryFile().name
    with open(vcf_tmp, 'w') as t:
        t.write(vcf_fmt)

    temp_dir = tempfile.mkdtemp()

    command = f"getDiff --genome /mnt/hdd_2/abdu/motif_diff/hg38.fa --motif /mnt/hdd_2/abdu/motif_diff/HOCOMOCOv11_full_HUMAN_mono_meme_format.meme --nuc mono --vcf {vcf_tmp} --method probNorm --mode average --out {temp_dir}/res"
    try:
        res = subprocess.run(command, capture_output=True,
                             text=True, shell=True)

        if res.returncode != 0:
            print("Command failed!")
            print("Error:")
            print(res.stderr)  # Print the standard error
            raise RuntimeError(res.stderr)

        motif_diff_res = pd.read_csv(f"{temp_dir}/res_mono_probNorm_average.diff",
                                     sep="\t")
        motif_diff_res.columns = list(map(lambda x: x.split(
            "_")[0] if x != "Unnamed: 0" else "ID", motif_diff_res.columns))
        motif_diff_res_t = pd.melt(motif_diff_res, id_vars=["ID"],
                                   value_vars=motif_diff_res.columns,
                                   var_name="TF",
                                   value_name="Score")
        motif_diff_loss = motif_diff_res_t[motif_diff_res_t["Score"] < 0].sort_values(
            by="Score", ascending=True)
        motif_diff_gain = motif_diff_res_t[motif_diff_res_t["Score"] > 0].sort_values(
            by="Score", ascending=False)
        result = []
        for i, row in motif_diff_loss.iterrows():
            variant = row["ID"]
            tf = row["TF"].lower()
            score = row["Score"]
            result.append({"variant": variant, "tf": tf,
                           "score": score, "type": "loss"})

        for i, row in motif_diff_gain.iterrows():
            variant = row["ID"]
            tf = row["TF"].lower()
            score = row["Score"]
            result.append({"variant": variant, "tf": tf,
                           "score": score, "type": "gain"})

        return result
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def analyze_coding_effect(chr, pos, ref, alt):
    """
    Analyze if a variant is coding and get its functional impact scores.
    """

    # Create temporary OpenCRAVAT input file
    vcf_path = create_opencravat_input(chr, pos, ref.upper(), alt.upper())
    
    # Create temp directory
    temp_dir = tempfile.mkdtemp(prefix='cravat_analysis_')
    
    try:
        # Get input filename only
        input_name = Path(vcf_path).stem
        
        # Run minimal annotation with VEST and SIFT
        run_args = {
            'inputs': [vcf_path],
            'annotators': [
                'vest',    
                'sift',
                'dbsnp_common'
            ],
            'genome': 'hg38',
            'output_dir': temp_dir,
            'run_name': input_name,
            'reports': ['text'],
            'annotators_replace': [],
            'silent': True
        }
        
        result = cravat.run(**run_args)
        
        # Connect to results database
        db_path = os.path.join(temp_dir, f"{input_name}.sqlite")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Query including Ensembl IDs
        query = """
        SELECT v.base__chrom, v.base__pos, 
               v.base__ref_base, v.base__alt_base,
               v.base__coding, v.base__hugo,
               v.base__so,
               v.vest__score, v.vest__pval,
               v.sift__score, v.sift__prediction
        FROM variant v        """
        
        cursor.execute(query)
        variants = cursor.fetchall()
        
        results = []
        for var in variants:
            (chrom, pos, ref, alt, is_coding, hugo, so, 
             vest_score, vest_pval, 
             sift_score, sift_pred) = var
            
            result = {
                "variant": f"{chrom}_{pos}_{ref.lower()}_{alt.lower()}",
                "status": "noncoding",
                "effect": None
            }
            
            if is_coding == 'Y':
                result["status"] = "coding"
                result["effect"] = {
                    "hugo": hugo.lower(),
                    "consequence": so.lower(),
                    "vest_score": vest_score,
                    "vest_pval": vest_pval,
                    "sift_score": sift_score,
                    "sift_prediction": sift_pred,
                    "impact": interpret_impact(vest_score, sift_score)
                }
                
            results.append(result)
        
        conn.close()
        print(f"Arguments {chr}, {pos}, {ref}, {alt}")
        return results
        
    finally:
        # Clean up temp directory
        shutil.rmtree(temp_dir)


def interpret_impact(vest_score, sift_score):
    """
    Interpret combined VEST and SIFT scores
    """
    if vest_score is None and sift_score is None:
        return {"is_damaging": False, "confidence": "no_data"}
        
    impact = {
        'is_damaging': False,
        'confidence': 'low'
    }
    
    # Consider damaging if either score indicates damage
    if (vest_score is not None and vest_score > 0.5) or \
       (sift_score is not None and sift_score < 0.05):
        impact['is_damaging'] = True
        
        # High confidence if both agree
        if (vest_score is not None and vest_score > 0.8) and \
           (sift_score is not None and sift_score < 0.01):
            impact['confidence'] = 'high'
        else:
            impact['confidence'] = 'medium'
            
    return impact

def create_opencravat_input(chr, pos, ref, alt):
    """
    Create a temporary OpenCRAVAT input file from variant information.
    
    Args:
        chr (str): Chromosome (e.g., 'chr17')
        pos (str/int): Position
        ref (str): Reference allele
        alt (str): Alternate allele
        
    Returns:
        str: Path to temporary input file
    """
    import tempfile
    import os
    
    # Create temp directory with prefix for easy identification
    temp_dir = tempfile.mkdtemp(prefix='opencravat_')
    
    # Create input file path
    input_file = os.path.join(temp_dir, 'variant_input.txt')
    
    # Write variant in OpenCRAVAT format
    with open(input_file, 'w') as f:
        # Format: chrom pos strand ref alt sample
        line = f"{chr}\t{pos}\t+\t{ref}\t{alt}\ts0\n"
        f.write(line)
        
    return input_file

def get_janus_python_info():
    """Prints the Python version and executable path Janus is using."""
    print(f"Janus is using Python version: {sys.version}")
    print(f"Python executable path: {sys.executable}")
    return True # Return something to indicate success to Prolog


def plot_curves(all_folds_roc_data, all_folds_pr_data,
                    output_roc_filename, output_pr_filename):
    """
    Plots ROC and PR curves for multiple folds.
    Input data for ROC is expected as [FPR, TPR] pairs.
    Input data for PR is expected as [Recall, Precision] pairs.

    Args:
        all_folds_roc_data (list of list of list of float): 
            For ROC: [[[fpr1, tpr1], [fpr2, tpr2], ...], # Fold 1
                      [[fpr1, tpr1], [fpr2, tpr2], ...]]  # Fold 2
        all_folds_pr_data (list of list of list of float):
            For PR:  [[[rec1, prec1], [rec2, prec2], ...], # Fold 1
                      [[rec1, prec1], [rec2, prec2], ...]]  # Fold 2
        output_roc_filename (str): Filename for the ROC plot.
        output_pr_filename (str): Filename for the PR plot.
    """
    
    # --- ROC Curve Plotting ---
    plt.figure(figsize=(8, 6))
    fold_roc_aucs = []
    roc_handles = []

    for i, fold_data in enumerate(all_folds_roc_data):
        if not fold_data:
            print(f"Warning: Fold {i+1} has no ROC data.")
            fold_roc_aucs.append(np.nan) # Or 0, or skip
            continue
            
        # Assuming fold_data is already a list of [fpr, tpr] pairs
        fpr_values = [point[0] for point in fold_data]
        tpr_values = [point[1] for point in fold_data]
        
        # Ensure the curve starts at (0,0) and ends at (1,1) if not already present
        # and sort by FPR for correct plotting order and AUC calculation
        points = sorted(list(set(zip(fpr_values, tpr_values))))
        
        current_fpr = [p[0] for p in points]
        current_tpr = [p[1] for p in points]

        if not current_fpr or not current_tpr:
            print(f"Warning: Fold {i+1} has empty FPR/TPR values after processing.")
            fold_roc_aucs.append(np.nan)
            continue

        if current_fpr[0] != 0.0 or current_tpr[0] != 0.0:
             current_fpr.insert(0, 0.0)
             current_tpr.insert(0, 0.0)
        
        if current_fpr[-1] != 1.0 or current_tpr[-1] != 1.0:
            current_fpr.append(1.0)
            current_tpr.append(1.0)
            
        # Re-sort after adding points to ensure monotonicity for AUC calculation
        final_points = sorted(list(set(zip(current_fpr, current_tpr))), key=lambda p: p[0])
        plot_fpr = [p[0] for p in final_points]
        plot_tpr = [p[1] for p in final_points]

        if len(plot_fpr) < 2: # Need at least two points to draw a line or calculate AUC
            print(f"Warning: Fold {i+1} has insufficient ROC points after processing.")
            fold_roc_aucs.append(np.nan)
            continue    
            
        roc_auc_fold = auc(plot_fpr, plot_tpr)
        fold_roc_aucs.append(roc_auc_fold)
        
        line, = plt.plot(plot_fpr, plot_tpr, marker='.', linestyle='-', label=f'Fold {i+1} (AUC = {roc_auc_fold:.2f})')
        roc_handles.append(line)

    # Add mean ROC AUC to legend
    if fold_roc_aucs:
        mean_roc_auc = np.nanmean(fold_roc_aucs) # Use nanmean to ignore NaNs if any fold had no data
        mean_auc_line = mlines.Line2D([], [], color='black', linestyle='--', 
                                      label=f'Mean ROC AUC = {mean_roc_auc:.2f}')
        roc_handles.append(mean_auc_line)

    # Add random classifier line
    random_line, = plt.plot([0, 1], [0, 1], color='grey', linestyle='--', label='Random Classifier')
    roc_handles.append(random_line)
    
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate (FPR)')
    plt.ylabel('True Positive Rate (TPR)')
    plt.title('Receiver Operating Characteristic (ROC) Curves')
    plt.legend(handles=roc_handles, loc="lower right")
    plt.grid(True)
    
    try:
        plt.savefig(output_roc_filename, transparent=True)
        print(f"ROC plot saved to {output_roc_filename}")
    except Exception as e:
        print(f"Error saving ROC plot: {e}")
    plt.close()

    # --- PR Curve Plotting ---
    plt.figure(figsize=(8, 6))
    fold_pr_aucs = []
    pr_handles = []

    for i, fold_data in enumerate(all_folds_pr_data):
        if not fold_data:
            print(f"Warning: Fold {i+1} has no PR data.")
            fold_pr_aucs.append(np.nan)
            continue
            
        # Assuming fold_data is already a list of [recall, precision] pairs
        recall_values = [point[0] for point in fold_data]
        precision_values = [point[1] for point in fold_data]

        # Sort by recall for correct plotting order and AUC calculation
        # PR curves often start at (0, P_first) and end at (R_last, 0) or (1, P_at_R=1)
        # The points from Prolog's AUC library should be mostly ordered.
        # We need to ensure recall is non-decreasing.
        points = sorted(list(set(zip(recall_values, precision_values))), key=lambda p: p[0])
        
        plot_recall = [p[0] for p in points]
        plot_precision = [p[1] for p in points]

        if len(plot_recall) < 2: # Need at least two points
            print(f"Warning: Fold {i+1} has insufficient PR points after processing.")
            fold_pr_aucs.append(np.nan)
            continue

        # sklearn.metrics.auc expects x (recall) to be ordered.
        # The area under the PR curve is also known as Average Precision.
        pr_auc_fold = auc(plot_recall, plot_precision)
        fold_pr_aucs.append(pr_auc_fold)
        
        line, = plt.plot(plot_recall, plot_precision, marker='.', linestyle='-', label=f'Fold {i+1} (AUC = {pr_auc_fold:.2f})')
        pr_handles.append(line)

    # Add mean PR AUC to legend
    if fold_pr_aucs:
        mean_pr_auc = np.nanmean(fold_pr_aucs)
        mean_pr_line = mlines.Line2D([], [], color='black', linestyle='--',
                                     label=f'Mean PR AUC = {mean_pr_auc:.2f}')
        pr_handles.append(mean_pr_line)
        
    # Optional: Add a no-skill line for PR curve (depends on class balance)
    # For now, just plot the curves.
    
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title('Precision-Recall (PR) Curves')
    plt.legend(handles=pr_handles, loc="lower left") # Often PR AUC is better in lower left or upper right
    plt.grid(True)
    
    try:
        plt.savefig(output_pr_filename, transparent=True)
        print(f"PR plot saved to {output_pr_filename}")
    except Exception as e:
        print(f"Error saving PR plot: {e}")
    plt.close()