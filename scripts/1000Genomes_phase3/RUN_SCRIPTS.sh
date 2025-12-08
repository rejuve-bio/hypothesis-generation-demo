#!/bin/bash

set -euo pipefail

# Where to save all the data (needs ~900 GB free space)
OUTPUT_DIR="${OUTPUT_DIR:-/mnt/hdd_1/tesnim/hypothesis-generation_demo/data/1000Genomes_phase3}"

# Number of CPU cores to use 
CORES="${CORES:-8}"

# Harmonizer reference directory
HARMONIZER_REF_DIR="${HARMONIZER_REF_DIR:-/data/harmonizer_ref}"
HARMONIZER_CODE_REPO="${HARMONIZER_CODE_REPO:-/app/gwas-sumstats-harmoniser}"

# ============================================
# SCRIPT DIRECTORY
# ============================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure OUTPUT_DIR is set
OUTPUT_DIR="${OUTPUT_DIR:-/mnt/hdd_1/tesnim/hypothesis-generation_demo/data/1000Genomes_phase3}"

echo "============================================"
echo "1000 Genomes Phase 3 Pipeline"
echo "============================================"
echo "Output directory: $OUTPUT_DIR"
echo "CPU cores: $CORES"
echo "Script directory: $SCRIPT_DIR"
echo "============================================"
echo ""

# ============================================
# STEP 1: Download VCFs (SKIPPED - already downloaded)
# ============================================
echo "[1/4] Downloading VCF files from 1000 Genomes..."
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    bash 1_download_vcfs.sh "$OUTPUT_DIR"
    echo "✓ Download complete"
else
    echo "Skipped download step"
fi
echo "[1/4] Download step skipped - VCF files already exist"
echo ""

# ============================================
# STEP 2: Normalize VCFs (SKIPPED - already complete)
# ============================================
echo "[2/4] Normalizing VCF files..."
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    bash 2_normalise_vcfs.sh "$OUTPUT_DIR" "$CORES"
    echo "✓ Normalization complete"
else
    echo "Skipped normalization step"
fi
echo "[2/4] Normalization step skipped - VCF files already normalized"
echo ""

# ============================================
# STEP 3: Create Population IDs
# ============================================
echo "[3/4] Creating population ID files..."
bash 3_make_population_ids.sh "$OUTPUT_DIR"
echo "✓ Population IDs created"
echo ""

# ============================================
# STEP 4: Convert to PLINK Format
# ============================================
echo "[4/4] Converting to PLINK format..."
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    bash 4_convert_to_plink.sh "$OUTPUT_DIR" "$OUTPUT_DIR/vcf_norm" "$CORES"
    echo "✓ PLINK conversion complete"
else
    echo "Skipped PLINK conversion step"
fi
echo ""

# ============================================
# STEP 5: Harmonizer Setup (OPTIONAL)
# ============================================
echo "[5/5] Set up harmonizer reference data? (Skip if already done)"
read -p "Run harmonizer setup? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    bash 0_harmoniser_setup.sh \
        --ref "$HARMONIZER_REF_DIR" \
        --code-repo "$HARMONIZER_CODE_REPO"
    echo "✓ Harmonizer setup complete"
else
    echo "Skipped harmonizer setup"
fi
echo ""

# ============================================
# FINAL SETUP
# ============================================
echo "============================================"
echo "✓ Pipeline Complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "1. Set PLINK_DIR environment variable:"
echo "   export PLINK_DIR=\"$OUTPUT_DIR/plink_format_b38\""
echo ""
echo "2. Or add to your .env file:"
echo "   PLINK_DIR=$OUTPUT_DIR/plink_format_b38"
echo ""
echo "3. Verify files exist:"
echo "   ls -lh $OUTPUT_DIR/plink_format_b38/EUR/EUR.chr1.1KG.GRCh38.*"
echo ""

