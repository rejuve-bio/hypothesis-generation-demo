#!/usr/bin/env bash
set -euo pipefail

# 30x NYGC GRCh38 (genotype VCFs)
FTP_SITE=ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000G_2504_high_coverage/working/20220422_3202_phased_SNV_INDEL_SV/
OUT="$1"; [[ -z "${OUT:-}" ]] && { echo "Usage: $0 <output_dir>"; exit 1; }
VCF_DIR="$OUT/vcf"
mkdir -p "$VCF_DIR"

echo "Downloading panel file..."
# Corrected: Use -O to save the file with the specified name in VCF_DIR
echo "Downloading panel file (integrated_call_samples_v3.20130502.ALL.panel)"
wget -O "$VCF_DIR/integrated_call_samples_v3.20130502.ALL.panel" \
  "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/integrated_call_samples_v3.20130502.ALL.panel"

# Autosomes (adjust patterns to the actual file names in your chosen collection)
for CHR in $(seq 1 22); do
  SRC="$FTP_SITE/1kGP_high_coverage_Illumina.chr$CHR.filtered.SNV_INDEL_SV_phased_panel.vcf.gz"
  DEST="$VCF_DIR/${CHR}.1KG.GRCh38.vcf.gz"
  echo "Downloading $SRC"
  wget -O "$DEST" "$SRC"
  wget -O "${DEST}.tbi" "${SRC}.tbi"
  sleep 1
done

# X
# SRC="$FTP_SITE/1kGP_high_coverage_Illumina.chrX.filtered.SNV_INDEL_SV_phased_panel.v2.vcf.gz"
# DEST="$VCF_DIR/X.1KG.GRCh38.vcf.gz"
# wget -O "$DEST" "$SRC"
# wget -O "${DEST}.tbi" "${SRC}.tbi"

echo "Finished -> $VCF_DIR"