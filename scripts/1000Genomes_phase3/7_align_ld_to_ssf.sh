#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: align_ld_to_ssf.sh --plink-dir DIR --ssf FILE --pop POP --out OUTDIR [--threads N]

Align per-pop, per-chromosome PLINK LD panel to a harmonized SSF, writing:
  - per-chromosome matched PLINK sets: OUTDIR/POP/matched/POP.matched.chr{CHR}.*
  - merged population-wide matched PLINK: OUTDIR/POP/POP.matched_to_ssf.{bed,bim,fam}
  - logs under OUTDIR/POP/logs/

Required:
  --plink-dir DIR   Root directory containing per-pop, per-chr PLINK bfiles (bed/bim/fam)
                    expected under DIR/POP/ (script will auto-detect file basenames).
  --ssf FILE        Harmonized SSF .tsv.gz from pipeline (with columns:
                    chromosome, base_pair_location, effect_allele, other_allele)
  --pop POP         One of: EUR, AFR, AMR, EAS, SAS
  --out OUTDIR      Output directory
Optional:
  --threads N       Parallelism for plink (default 4)

Notes:
  * Requires: awk, zcat, plink (1.9+), coreutils, grep, sort, realpath (optional)
  * Expects PLINK .bim IDs are CHR:POS:REF:ALT and SSF uses 1..22,X,(Y,MT) with no "chr" prefix
EOF
}

PLINK_DIR=""
SSF=""
POP=""
OUTDIR=""
THREADS=4

while [[ $# -gt 0 ]]; do
  case "$1" in
  --plink-dir)
    PLINK_DIR="${2:-}"
    shift 2
    ;;
  --ssf)
    SSF="${2:-}"
    shift 2
    ;;
  --pop)
    POP="${2:-}"
    shift 2
    ;;
  --out)
    OUTDIR="${2:-}"
    shift 2
    ;;
  --threads)
    THREADS="${2:-}"
    shift 2
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown arg: $1"
    usage
    exit 2
    ;;
  esac
done

# --- validation ----
[[ -n "$PLINK_DIR" && -d "$PLINK_DIR" ]] || {
  echo "ERROR: --plink-dir missing/invalid"
  exit 2
}
[[ -n "$SSF" && -r "$SSF" ]] || {
  echo "ERROR: --ssf missing/unreadable"
  exit 2
}
[[ -n "$OUTDIR" ]] || {
  echo "ERROR: --out is required"
  exit 2
}
mkdir -p "$OUTDIR"

shopt -s nocasematch
case "$POP" in
EUR | AFR | AMR | EAS | SAS) ;;
*)
  echo "ERROR: --pop must be one of EUR/AFR/AMR/EAS/SAS"
  exit 2
  ;;
esac
shopt -u nocasematch

abspath() {
  local p="$1"
  if command -v realpath >/dev/null 2>&1; then
    realpath -m -- "$p"
  else
    python3 - "$p" 2>/dev/null <<'PY' || printf '%s\n' "$p"
        import os, sys
        print(os.path.abspath(sys.argv[1]))
PY
  fi
}

# find_bfile_base() {
#   local pop="$1" chr="$2" root="$3"
#   local d="$root/$pop"
#   [[ -d "$d" ]] || return 1

#   # try a few patterns (adjust/extend if your naming differs)
#   local cand
#   for cand in \
#       "$d/$pop.${chr}.1KG.GRCh38" \
#       "$d/${pop}.${chr}" \
#       "$d/${chr}.${pop}.GRCh38" \
#       "$d/${pop}.${chr}" \
#       "$d/${pop}.${chr}"; do
#     [[ -f "${cand}.bed" && -f "${cand}.bim" && -f "${cand}.fam" ]] && { echo "$cand"; return 0; }
#   done

#   # fallback: any *chr{chr}* under $d that has bed/bim/fam
#   local bed
#   bed="$(find "$d" -maxdepth 2 -type f -name "*chr${chr}*.bed" | head -n1 || true)"
#   if [[ -n "$bed" ]]; then
#     local base="${bed%.bed}"
#     [[ -f "${base}.bim" && -f "${base}.fam" ]] && { echo "$base"; return 0; }
#   fi
#   return 1
# }

die() {
  echo "Error: $*" >&2
  exit 2
}

PLINK_DIR="$(abspath "$PLINK_DIR")"
SSF="$(abspath "$SSF")"
OUTDIR="$(abspath "$OUTDIR")"

POPDIR="$OUTDIR/$POP"
MATCHDIR="$POPDIR/matched"
LOGDIR="$POPDIR/logs"
TMPDIR="$POPDIR/tmp"
mkdir -p "$MATCHDIR" "$LOGDIR" "$TMPDIR"

KEEP_ALL="$TMPDIR/keep.ids.all.txt"

zcat "$SSF" |
  awk -F'\t' '
    NR==1{
        for(i=1;i<=NF;i++) h[$i]=i
        need="chromosome\tbase_pair_location\teffect_allele\tother_allele"
        if(!h["chromosome"]||!h["base_pair_location"]||!h["effect_allele"]||!h["other_allele"]){
            print "ERROR: SSF missing required columns: " need > "/dev/stderr"; exit 3
        }
        next
    }
    {
        chr=$h["chromosome"]; pos=$h["base_pair_location"];
        ea=toupper($h["effect_allele"]); oa=toupper($h["other_allele"]); 
        if(chr=="23") chr="X"; else if(chr=="24") chr="Y"; else if(chr=="26") chr="MT";
        if(chr!="X" && chr!="Y" && chr!="MT" && chr+0==0) next
        printf "%s:%s:%s:%s\n", chr,pos,oa,ea
    }
    ' | sort -u >"$KEEP_ALL"

CHROMS="$TMPDIR/chr.set"
awk -F':' '{print $1}' "$KEEP_ALL" | sort -u >"$TMPDIR/chr.set"

MERGELIST="$TMPDIR/merge.list"
: >"$MERGELIST"

while read -r CHR; do
  BFILE="${PLINK_DIR}/${POP}/${POP}.chr${CHR}.1KG.GRCh38"

  OUTBASE="$MATCHDIR/${POP}.matched.${CHR}"
  echo "Extracting POP=$POP chr=$CHR from $(basename "$BFILE")"

  KEEP_CHR="$TMPDIR/keep.$CHR.txt"
  awk -F':' -v C="$CHR" '$1==C {print $0}' "$KEEP_ALL" >"$KEEP_CHR"

  cut -f2 "${BFILE}.bim" | sort -u >"$TMPDIR/bim.$CHR.ids"
  sort -u "$KEEP_CHR" >"$TMPDIR/keep.$CHR.sorted"
  comm -12 "$TMPDIR/keep.$CHR.sorted" "$TMPDIR/bim.$CHR.ids" >"$TMPDIR/keep.$CHR.inpanel"

  A1_CHR="$TMPDIR/a1.$CHR.map"
  zcat "$SSF" | awk -F'\t' -v OFS='\t' -v C="$CHR" '
      NR==1{for(i=1;i<=NF;i++)h[$i]=i; next}
      {
        chr=$h["chromosome"]; pos=$h["base_pair_location"];
        ea=toupper($h["effect_allele"]); oa=toupper($h["other_allele"]);
        if(chr=="23")chr="X"; else if(chr=="24")chr="Y"; else if(chr=="26")chr="MT";
        id = chr ":" pos ":" oa ":" ea;
        if((chr)==C) print id, ea
      }' >"$A1_CHR"

  awk 'NR==FNR{seen[$1]=1; next} ($1 in seen)' "$TMPDIR/bim.$CHR.ids" "$A1_CHR" >"$TMPDIR/a1.$CHR.inpanel.map"

  plink2 \
    --bfile "$BFILE" \
    --extract "$TMPDIR/keep.$CHR.inpanel" \
    --keep-allele-order \
    --alt1-allele "$TMPDIR/a1.$CHR.inpanel.map" 1 2 \
    --make-bed \
    --threads "$THREADS" \
    --out "$OUTBASE" >/dev/null

  {
    echo -n "chr${CHR} input_bim: "
    wc -l <"${BFILE}.bim"
    echo -n "chr${CHR} keep_ids : "
    wc -l <"$KEEP_CHR"
    echo -n "chr${CHR} out_bim  : "
    wc -l <"${OUTBASE}.bim"
    echo
  } >>"$LOGDIR/counts.txt"

  # record for merge if non-empty
  if [[ -s "${OUTBASE}.bim" ]]; then
    echo "$OUTBASE" >>"$MERGELIST"
  else
    echo "WARN: no overlapping variants for $POP chr$CHR" | tee -a "$LOGDIR/empty_outputs.log"
  fi

done <"$CHROMS"

n=$(wc -l <"$MERGELIST")
BASE=$(head -n1 "$MERGELIST")
MERGE_OUT="$POPDIR/${POP}.final"

if [[ n > 1 ]]; then
  echo "Merging $((n - 1)) chromosomes -> $(basename "$MERGE_OUT")"
  plink2 \
    --bfile "$BASE" \
    --pmerge-list <(tail -n +2 "$MERGELIST") bfile \
    --make-bed \
    --threads "${THREADS:-1}" \
    --out "$MERGE_OUT" \
    >/dev/null
else
  for ext in bed bim fam; do
    cp "${BASE}.${ext}" "${MERGE_OUT}.${ext}"
  done

fi
head -n 5 "$KEEP_ALL" | while read -r id; do
  if ! grep -q -w "$id" "${MERGE_OUT}.bim"; then
    echo "WARN: merged PLINK missing example SSF ID: $id" | tee -a "$LOGDIR/sanity_missing_ids.log"
  fi
done

# Clean up
#rm -r $TMPDIR $MATCHDIR

echo "Done.
Per-chrom matched sets : $MATCHDIR/${POP}.final.${CHR}.{bed,bim,fam}
Merged matched panel   : ${MERGE_OUT}.{bed,bim,fam}
Logs                   : $LOGDIR/"
