#!/usr/bin/env bash
set -euo pipefail

# Sanity checks
command -v nextflow >/dev/null || { echo "ERROR: nextflow not found in PATH"; exit 1; }

# Default chromlist - updated down below based on the existing ones sumstats                            
CHROMLIST="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y,MT"
### --------------------------------

BASEDIR="${BASEDIR:-$(pwd -P)}"

# resolve_path() and to_gwas_ssf() live in ssf_remap.sh so the column-remap
# step can be reused standalone (e.g. by harmonize_sumstats_with_nextflow in
# src/tasks/analysis.py) without running the rest of this script.
# shellcheck source=./ssf_remap.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/ssf_remap.sh"

die() { echo "Error: $*" >&2; exit 2; }

usage() {
  cat << 'EOF'
Usage: harmonizer.sh [OPTIONS] -- INPUT ..

Options:
  -i, --input         Input GWAS Summary Stats data            (REQUIRED)
  -b, --build         Build of the summary data (GRCh38|GRCh37) (REQUIRED)
  -t, --threshold     Threshold for palindromic variants (default=0.99)
  -r, --ref           Reference data directory (REQUIRED; created by harmonizer_setup.sh)
  -c, --code-repo     Path to workflow scripts repo (default=current working directory)
  -h, --help          Show this help
EOF
}


: "${THRESHOLD:=0.99}"
: "${CODE_REPO:="$PWD"}"

SUMSTATS="${SUMSTATS-}"
BUILD="${BUILD-}"
REF_DIR="${REF_DIR-}"


while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)        SUMSTATS="${2:-}"; shift 2 ;;
    --input=*)      SUMSTATS="${1#*=}"; shift ;;
    --build)        BUILD="${2:-}"; shift 2 ;;
    --build=*)      BUILD="${1#*=}"; shift ;;
    --threshold)    THRESHOLD="${2:-}"; shift 2 ;;
    --threshold=*)  THRESHOLD="${1#*=}"; shift ;;
    --ref)          REF_DIR="${2:-}"; shift 2 ;;
    --ref=*)        REF_DIR="${1#*=}"; shift ;;
    --code-repo)    CODE_REPO="${2:-}"; shift 2 ;;
    --code-repo=*)  CODE_REPO="${1#*=}"; shift ;;
    -h|--help)      usage; exit 0 ;;
    --)             shift; break ;;
    -*)             break ;;  # let getopts handle the short options next
    *)              break ;;
  esac
done

OPTIND=1
while getopts ":i:b:t:r:c:h" opt; do
  case "$opt" in
    i) SUMSTATS="$OPTARG" ;;
    b) BUILD="$OPTARG" ;;
    t) THRESHOLD="$OPTARG" ;;
    r) REF_DIR="$OPTARG" ;;      
    c) CODE_REPO="$OPTARG" ;;   
    h) usage; exit 0 ;;
    :) echo "Option -$OPTARG requires an argument" >&2; usage; exit 2 ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage; exit 2 ;;
  esac
done
shift $((OPTIND-1))


if [[ -z ${SUMSTATS-} ]]; then
  die "--input/-i is required"
fi

if [[ ! -r "$SUMSTATS" ]]; then
  die "cannot read input file: $SUMSTATS"
fi

# --build (must be GRCh37 or GRCh38; case-insensitive)
if [[ -z ${BUILD-} ]]; then
  die "--build/-b is required (GRCh37 or GRCh38)"
fi
shopt -s nocasematch
case "$BUILD" in
  grch37|GRCh37|b37|hg19) BUILD="37" ;;
  grch38|GRCh38|b38|hg38) BUILD="38" ;;
  *) shopt -u nocasematch; die "--build must be GRCh37 or GRCh38 (got '$BUILD')" ;;
esac
shopt -u nocasematch

# Optional with numeric constraint: --threshold in (0,1)
# 1) ensure numeric format, 2) float compare
if ! [[ "$THRESHOLD" =~ ^([0-9]+(\.[0-9]+)?)$ ]]; then
  die "--threshold must be a number in (0,1); got '$THRESHOLD'"
fi

if ! awk "BEGIN{exit !($THRESHOLD > 0 && $THRESHOLD < 1)}"; then
  die "--threshold must be > 0 and < 1; got '$THRESHOLD'"
fi

if [[ -z ${REF_DIR-} ]]; then
  die "--ref/-r is required (path to reference data directory)"
fi
if [[ ! -d "$REF_DIR" ]]; then
  die "reference directory not found: $REF_DIR"
fi

if [[ ! -d "$CODE_REPO" ]]; then
  die "code repo directory not found: $CODE_REPO"
fi


# echo "==== [1/2] PREPARE REFERENCES ===="
# nextflow run . -profile standard \
#   --reference \
#   --ref "$REF_DIR" \
#   --chromlist "$CHROMLIST" \
#   -with-report   "$LOG_DIR/ref-report.html" \
#   -with-timeline "$LOG_DIR/ref-timeline.html" \
#   -with-trace    "$LOG_DIR/ref-trace.txt" \
#   |& tee -a "$LOG_DIR/ref.log"


# Create a meta yaml file based on the sumstats input
SUMSTATS_DIR=$(dirname -- "$SUMSTATS")
SUMSTATS_BASENAME=$(basename -- "$SUMSTATS")
DATA_FILE_NAME="${SUMSTATS_BASENAME%%.*}"

# Paths
LOG_DIR="$SUMSTATS_DIR/logs"
mkdir -p "$LOG_DIR"

# Add the code repo to the PATH so that Nextflow can find the workflow scripts
export PATH="$CODE_REPO:$PATH"

echo "==== VALIDATE INPUT SUMSTATS ===="
echo "=== Converting GWAS SUMSTATS to SSF Format (this will also remove X, Y, MT chrs if they are in the data) ===="

# to_gwas_ssf() is sourced from ssf_remap.sh (see top of file).

chromlist_from_ssf() {
  local ssf="$1"
  [[ -r "$ssf" ]] || die "cannot read SSF: $ssf"

  local reader="cat"
  case "$ssf" in *.gz|*.bgz) reader="bgzip -dc";; esac

  # Extract unique chromosomes from SSF, normalize to bare tokens for Nextflow (--chromlist)
  mapfile -t present < <(
    $reader "$ssf" | awk -F'\t' '
      NR==1 { for(i=1;i<=NF;i++) if($i=="chromosome") c=i; next }
      {
        chr=$c
        print chr
      }
    ' | awk 'NF' | sort -u
  )

  local order=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y MT)
  local have=()
  for ch in "${order[@]}"; do
    if printf '%s\n' "${present[@]}" | grep -qx "$ch"; then
      have+=("$ch")
    fi
  done

  if [[ ${#have[@]} -eq 0 ]]; then
    echo "WARN: No chromosomes detected in SSF; using full list" >&2
    echo "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y,MT"
    return 0
  fi

  (IFS=,; echo "${have[*]}")
}


# Detect if input is already in SSF format — skip reformatting (same logic as notebook)
_check_reader="cat"
case "$SUMSTATS" in *.gz|*.bgz) _check_reader="bgzip -dc";; esac
_header=$(
  set +o pipefail
  $_check_reader "$SUMSTATS" | head -1 | tr '[:upper:]' '[:lower:]'
)

if echo "$_header" | grep -q "base_pair_location" && echo "$_header" | grep -q "effect_allele"; then
  echo "Input is already in SSF format — skipping reformatting"
  OUT_GZ="$SUMSTATS"
  OUT_YAML="${SUMSTATS}-meta.yaml"
  if [[ ! -f "$OUT_YAML" ]]; then
    MD5="$(md5sum < "$SUMSTATS" | awk '{print $1}')"
    cat > "$OUT_YAML" <<YAML
# Study meta-data
date_metadata_last_modified: $(date +%F)

# Genotyping Information
genome_assembly: GRCh${BUILD}
coordinate_system: 1-based

# Summary Statistic information
data_file_name: $(basename "$SUMSTATS")
file_type: GWAS-SSF v0.1
data_file_md5sum: ${MD5}

# Harmonization status
is_harmonised: false
is_sorted: false
YAML
  fi
else
  OUT_GZ="$(to_gwas_ssf)"
fi

echo "SSF file: $OUT_GZ"
CHROMLIST="$(chromlist_from_ssf "$OUT_GZ")"
echo "Chromosomes detected: $CHROMLIST"

REF_DIR_ABS="$(resolve_path "$REF_DIR")"

cd $SUMSTATS_DIR

echo "==== HARMONISE SUMSTATS ===="

# Use custom config if it exists (for overriding Python environment)
CUSTOM_CONFIG="/app/config/harmonizer_custom.config"
CUSTOM_CONFIG_FLAG=""
if [[ -f "$CUSTOM_CONFIG" ]]; then
  CUSTOM_CONFIG_FLAG="-c $CUSTOM_CONFIG"
  echo "Using custom Nextflow config: $CUSTOM_CONFIG"
fi

nextflow run "$CODE_REPO" -profile standard \
  $CUSTOM_CONFIG_FLAG \
  -qs 4 \
  --harm \
  --ref "$REF_DIR_ABS" \
  --file "$OUT_GZ" \
  --chromlist "$CHROMLIST" \
  --to_build "38" \
  --threshold "$THRESHOLD"  \
  -with-report   "logs/harm-report-$(date +%s).html" \
  -with-timeline "logs/harm-timeline-$(date +%s).html" \
  -with-trace    "logs/harm-trace-$(date +%s).txt" \
  |& tee -a "$LOG_DIR/harm.log"

echo "All done."
echo "Logs:      $LOG_DIR/"

# Find the harmonized output file and output path for Python parsing
HARMONIZED_OUTPUT=$(find "$SUMSTATS_DIR" -type f \( -path "*/final/*.tsv.gz" -o -path "*/final/*harmonised*.tsv.gz" \) ! -name "*-meta.yaml" -print -quit 2>/dev/null)

if [[ -n "$HARMONIZED_OUTPUT" ]]; then
    echo "HARMONIZED_OUTPUT_PATH=$HARMONIZED_OUTPUT"
else
    echo "WARNING: Could not find harmonized output file" >&2
    echo "Outputs:   Look for a date-stamped folder created by Nextflow containing <basename>/final/"
fi