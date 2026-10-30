#!/usr/bin/env python3
"""
End-to-end experiment pipeline:
1. Run liftcover parameter learning → extract per-fold weights + confidences
2. Generate fold_params.metta with per-fold stv values
3. Run both MeTTa inference variants (NoisyOr + Revision)
4. Parse per-fold AUC scores
5. Build scores.txt and generate comparison plot
"""

import subprocess
import re
import os
import shutil
import pathlib

HYPGEN_DIR = "/home/abdu/code/hypothesis-generation-demo"
PETTA_DIR = "/home/abdu/code/PeTTa"
BIO2_DIR = os.path.join(PETTA_DIR, "bio2")

NOISY_OR_FILE = os.path.join(BIO2_DIR, "prolog_test_v2.metta")
REVISION_FILE = os.path.join(BIO2_DIR, "prolog_test_rev_v2.metta")
FOLD_PARAMS_FILE = os.path.join(BIO2_DIR, "fold_params.metta")
SCORES_FILE = os.path.join(BIO2_DIR, "scores.txt")
SCORES_ORIG = os.path.join(BIO2_DIR, "scores_orig.txt")
PLOT_SCRIPT = os.path.join(BIO2_DIR, "plot_scores.py")

# Map liftcover body predicates → MeTTa fold-stv identifiers
RULE_MAP = {
    "pgboost": "pgboost",
    "eqtl_association": "eqtl_association",
    "activity_by_contact": "activity_by_contact",
    "regulatory_effect": "regulatory_effect",
}


def run_liftcover():
    """Step 1: Run liftcover and parse per-fold weights + confidences."""
    print("=" * 60)
    print("STEP 1: Running liftcover parameter learning...")
    print("=" * 60)

    cmd = (
        'swipl -g "consult(pl/hypgen), init, '
        "run_param_learning('./data/chr16', 5, AUCROC, AUCPRC, "
        'M_AUCROC, S_AUCROC, M_AUCPR, S_AUCPR, 0.3, _)." -t halt'
    )
    result = subprocess.run(
        cmd, shell=True, cwd=HYPGEN_DIR,
        capture_output=True, text=True, timeout=600
    )
    output = result.stdout + result.stderr
    print(output)

    # Parse per-fold parameters from "--- Fold N ---" sections
    # Format: "  body(B,A): weight=0.123456 conf=0.789012"
    fold_params = {}  # {fold_num: {pred: (weight, conf)}}
    current_fold = None

    for line in output.splitlines():
        m = re.match(r'--- Fold (\d+) ---', line)
        if m:
            current_fold = int(m.group(1))
            fold_params[current_fold] = {}
            continue

        if current_fold is not None:
            m = re.match(r'\s+(\w+)\([^)]*\):\s+weight=([\d.]+)\s+conf=([\d.]+)', line)
            if m:
                pred = m.group(1)
                weight = float(m.group(2))
                conf = float(m.group(3))
                fold_params[current_fold][pred] = (weight, conf)

    # Parse AUC results
    auc_roc = None
    auc_pr = None
    for line in output.splitlines():
        m = re.match(r'AUCROC:\s+([\d.]+)\s*\+/-\s*([\d.]+)', line)
        if m:
            auc_roc = (float(m.group(1)), float(m.group(2)))
        m = re.match(r'AUCPR:\s+([\d.]+)\s*\+/-\s*([\d.]+)', line)
        if m:
            auc_pr = (float(m.group(1)), float(m.group(2)))

    # Parse per-fold AUCROC: "  Fold N: 0.123456"
    per_fold_aucroc = {}
    in_per_fold_auc = False
    for line in output.splitlines():
        if "=== Per-Fold AUCROC ===" in line:
            in_per_fold_auc = True
            continue
        if in_per_fold_auc:
            m = re.match(r'\s+Fold (\d+):\s+([\d.]+)', line)
            if m:
                per_fold_aucroc[int(m.group(1))] = float(m.group(2))
            elif line.strip() and not line.startswith(" "):
                in_per_fold_auc = False

    print("\n--- Parsed Per-Fold Parameters ---")
    for fold in sorted(fold_params):
        print(f"  Fold {fold}:")
        for pred, (w, c) in fold_params[fold].items():
            print(f"    {pred}: weight={w:.6f} conf={c:.6f}")

    if auc_roc:
        print(f"\nLiftcover AUC-ROC: {auc_roc[0]:.4f} +/- {auc_roc[1]:.4f}")
    if auc_pr:
        print(f"Liftcover AUC-PR:  {auc_pr[0]:.4f} +/- {auc_pr[1]:.4f}")

    if per_fold_aucroc:
        print("\n--- Per-Fold AUCROC (from current liftcover run) ---")
        for fold in sorted(per_fold_aucroc):
            print(f"  Fold {fold}: {per_fold_aucroc[fold]:.6f}")

    return fold_params, per_fold_aucroc


def generate_fold_params_metta(fold_params):
    """Step 2: Generate fold_params.metta with per-fold stv values."""
    print(f"\n{'=' * 60}")
    print("STEP 2: Generating fold_params.metta...")
    print("=" * 60)

    lines = ["; Auto-generated per-fold parameters from liftcover"]

    for fold in sorted(fold_params):
        lines.append(f"; Fold {fold}")
        for pred, metta_id in RULE_MAP.items():
            if pred in fold_params[fold]:
                w, c = fold_params[fold][pred]
                lines.append(f"(= (fold-stv {metta_id} {fold}) (stv {w} {c}))")
            else:
                print(f"  WARNING: {pred} not found in fold {fold}, skipping")

    content = "\n".join(lines) + "\n"
    with open(FOLD_PARAMS_FILE, "w") as f:
        f.write(content)

    print(f"  Wrote {FOLD_PARAMS_FILE}")
    print(f"\n  Contents:")
    print(content)


def run_metta(filepath, label):
    """Step 3: Run a MeTTa file and capture last 200 lines."""
    print(f"\n{'=' * 60}")
    print(f"STEP 3: Running {label} ({os.path.basename(filepath)})...")
    print("=" * 60)

    rel_path = os.path.relpath(filepath, PETTA_DIR)
    cmd = f"./run.sh ./{rel_path}"
    result = subprocess.run(
        cmd, shell=True, cwd=PETTA_DIR,
        capture_output=True, text=True, timeout=1800
    )
    output = result.stdout + result.stderr
    lines = output.splitlines()
    last_200 = lines[-200:] if len(lines) > 200 else lines
    tail = "\n".join(last_200)
    print(tail)

    return tail


def parse_metta_aucs(output):
    """Parse per-fold AUC values from MeTTa output.

    Looks for lines like: ("AUCs: " (0.77 0.72 0.76 0.78 0.85))
    Returns list of 3 lists (ST, Conf, Exp), each with 5 fold values.
    """
    auc_lists = []
    for line in output.splitlines():
        m = re.search(r'\("AUCs: "\s*\(([^)]+)\)\)', line)
        if m:
            vals = [float(x) for x in m.group(1).split()]
            auc_lists.append(vals)
    return auc_lists


def build_scores(revision_aucs, noisyor_aucs, per_fold_aucroc):
    """Step 4: Build scores.txt from parsed AUC values."""
    print(f"\n{'=' * 60}")
    print("STEP 4: Building scores.txt...")
    print("=" * 60)

    # Rename existing scores.txt
    if os.path.exists(SCORES_FILE):
        shutil.copy2(SCORES_FILE, SCORES_ORIG)
        print(f"  Backed up {SCORES_FILE} → {SCORES_ORIG}")

    # Method order in output: ST, Conf, Exp
    suffixes = ["ST", "Conf", "Exp"]

    rows = ["Method,Fold,AUC"]

    # Revision (from prolog_test_rev_v2.metta)
    for i, suffix in enumerate(suffixes):
        if i < len(revision_aucs):
            for fold, val in enumerate(revision_aucs[i]):
                rows.append(f"PLN-Revision-{suffix},{fold},{val}")

    # NoisyOr (from prolog_test_v2.metta)
    for i, suffix in enumerate(suffixes):
        if i < len(noisyor_aucs):
            for fold, val in enumerate(noisyor_aucs[i]):
                rows.append(f"PLN-NoisyOr-{suffix},{fold},{val}")

    # ProbLog baseline from current liftcover run
    for fold in sorted(per_fold_aucroc):
        rows.append(f"ProbLog,{fold},{per_fold_aucroc[fold]}")

    with open(SCORES_FILE, "w") as f:
        f.write("\n".join(rows) + "\n")

    print(f"  Wrote {len(rows) - 1} data rows to {SCORES_FILE}")
    print("\n  Contents:")
    for row in rows:
        print(f"    {row}")


def generate_plot():
    """Step 5: Run plot_scores.py."""
    print(f"\n{'=' * 60}")
    print("STEP 5: Generating plot...")
    print("=" * 60)

    result = subprocess.run(
        ["python3", PLOT_SCRIPT], capture_output=True, text=True
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)


def main():
    # Step 1: Learn parameters
    fold_params, per_fold_aucroc = run_liftcover()

    if not fold_params:
        print("ERROR: Failed to parse per-fold parameters from liftcover output")
        return

    # Step 2: Generate fold_params.metta
    generate_fold_params_metta(fold_params)

    # Step 3: Run MeTTa inference
    noisyor_output = run_metta(NOISY_OR_FILE, "NoisyOr")
    revision_output = run_metta(REVISION_FILE, "Revision")

    # Parse AUC scores
    noisyor_aucs = parse_metta_aucs(noisyor_output)
    revision_aucs = parse_metta_aucs(revision_output)

    print(f"\n  NoisyOr AUC lists found: {len(noisyor_aucs)}")
    for i, a in enumerate(noisyor_aucs):
        print(f"    [{i}]: {a}")
    print(f"  Revision AUC lists found: {len(revision_aucs)}")
    for i, a in enumerate(revision_aucs):
        print(f"    [{i}]: {a}")

    if len(noisyor_aucs) < 3 or len(revision_aucs) < 3:
        print("WARNING: Expected at least 3 AUC lists (ST, Conf, Exp) per variant")
        print("  Proceeding with what we have...")

    # Step 4: Build scores.txt
    build_scores(revision_aucs, noisyor_aucs, per_fold_aucroc)

    # Step 5: Plot
    generate_plot()

    print(f"\n{'=' * 60}")
    print("DONE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
