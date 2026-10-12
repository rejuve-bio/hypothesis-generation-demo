#!/usr/bin/env bash
set -euo pipefail

# Sanity checks
command -v nextflow >/dev/null || { echo "ERROR: nextflow not found in PATH"; exit 1; }
                               
CHROMLIST="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y,MT"
### --------------------------------



die() { echo "Error: $*" >&2; ext 2; }

usage() {
  cat << 'EOF'
Usage: harmonizer_setup.sh [OPTIONS] -- INPUT ..

Options:
  -r, --ref           Reference data directory (REQUIRED; where to download reference data)
  -c, --code-repo     Path to workflow scripts repo (default=current working directory)
  -h, --help          Show this help
EOF
}


: "${CODE_REPO:="$PWD"}"
REF_DIR="${REF_DIR-}"


while [[ $# -gt 0 ]]; do
  case "$1" in
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
while getopts ":r:c:h" opt; do
  case "$opt" in
    r) REF_DIR="$OPTARG" ;;      
    c) CODE_REPO="$OPTARG" ;;   
    h) usage; exit 0 ;;
    :) echo "Option -$OPTARG requires an argument" >&2; usage; exit 2 ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage; exit 2 ;;
  esac
done
shift $((OPTIND-1))

if [[ -z ${REF_DIR-} ]]; then
  die "--ref/-r is required (path to reference data directory)"
fi
if [[ ! -d "$REF_DIR" ]]; then
  die "reference directory not found: $REF_DIR"
fi

if [[ ! -d "$CODE_REPO" ]]; then
  die "code repo directory not found: $CODE_REPO"
fi

LOG_DIR="$REF_DIR/logs"
mkdir -p "$LOG_DIR"

# Add the code repo to the PATH so that Nextflow can find the workflow scripts
export PATH="$CODE_REPO:$PATH"

echo "==== PREPARE REFERENCES ===="
nextflow run "$CODE_REPO" -profile standard \
  --reference \
  --ref "$REF_DIR" \
  --chromlist "$CHROMLIST" \
  -with-report   "$LOG_DIR/ref-report.html" \
  -with-timeline "$LOG_DIR/ref-timeline.html" \
  -with-trace    "$LOG_DIR/ref-trace.txt" \
  |& tee -a "$LOG_DIR/ref.log"

echo "All done."
echo "Logs:      $LOG_DIR/"
echo "Outputs:   Look for a date-stamped folder created by Nextflow containing <basename>/final/"