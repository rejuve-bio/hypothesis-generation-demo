#!/usr/bin/env bash
set -euo pipefail
OUT="$1"; [[ -z "${OUT:-}" ]] && { echo "Usage: $0 <out_dir>"; exit 1; }
VCF_DIR="$OUT/vcf"
PLINK_DIR="$OUT/plink_format_b38"
mkdir -p "$PLINK_DIR"

PANEL="$VCF_DIR/integrated_call_samples_v3.20130502.ALL.panel"
for POP in AFR AMR EAS EUR SAS; do
  d="$PLINK_DIR/$POP"; mkdir -p "$d"
  awk -v p="$POP" 'BEGIN{FS=OFS="\t"} NR>1 && $3==p {print "0",$1}' "$PANEL" > "$d/$POP.id"
done