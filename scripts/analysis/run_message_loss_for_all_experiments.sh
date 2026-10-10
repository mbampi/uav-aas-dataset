#!/usr/bin/env bash
# Runs metrics/message_loss_analyzer.py against every experiment folder under
# experiments/dissertation_exps/connector/, and appends the command + its
# output to the end of that folder's README (README.md or readme.md,
# whichever exists), in the same "command then verbatim output" style
# already used by the other entries in those READMEs.
#
# Safe to re-run: a folder whose README already has a "MESSAGE DROP /
# COALESCE ANALYSIS" block is skipped rather than appended to again.
set -euo pipefail
shopt -s nullglob

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXP_ROOT="$REPO_ROOT/experiments/dissertation_exps/connector"
ANALYZER="$REPO_ROOT/metrics/message_loss_analyzer.py"
# Relative to each run folder (experiments/dissertation_exps/connector/<run>/),
# used so the script's own "Reading file:"/Tier-0 output stays relative and
# short, matching the existing READMEs' style, instead of leaking an absolute
# local path into a checked-in-looking file.
MAPPING_REL="../../../../config/mappings/mavlink_ops.yaml"

for dir in "$EXP_ROOT"/*/; do
  name="$(basename "$dir")"

  latency_logs=("$dir"latency_*.log)
  if [ ${#latency_logs[@]} -eq 0 ]; then
    echo "skip $name: no latency_*.log found"
    continue
  fi
  latency_log="${latency_logs[0]}"

  dispatch_csvs=("$dir"dispatch_connector_*.csv)
  dispatch_csv=""
  if [ ${#dispatch_csvs[@]} -gt 0 ]; then
    dispatch_csv="${dispatch_csvs[0]}"
  fi

  readme_path=""
  for candidate in "$dir"README.md "$dir"readme.md "$dir"Readme.md; do
    if [ -f "$candidate" ]; then
      readme_path="$candidate"
      break
    fi
  done
  if [ -z "$readme_path" ]; then
    echo "skip $name: no README found"
    continue
  fi

  if grep -q "MESSAGE DROP / COALESCE ANALYSIS" "$readme_path"; then
    echo "skip $name: $readme_path already has a message-loss section"
    continue
  fi

  case "$name" in
    both_*) protocols=(mqtt opcua) ;;
    mqtt_*) protocols=(mqtt) ;;
    opcua_*) protocols=(opcua) ;;
    *)
      echo "skip $name: unrecognized folder prefix (expected mqtt_/opcua_/both_)"
      continue
      ;;
  esac

  latency_base="$(basename "$latency_log")"
  dispatch_base=""
  extra_args=()
  if [ -n "$dispatch_csv" ]; then
    dispatch_base="$(basename "$dispatch_csv")"
    extra_args=(--dispatch-csv "$dispatch_base" --mapping "$MAPPING_REL")
  else
    echo "note $name: no dispatch_connector_*.csv found, Tier 0 will be skipped"
  fi

  for proto in "${protocols[@]}"; do
    cmd_display="python3 message_loss_analyzer.py $latency_base --protocol $proto"
    if [ -n "$dispatch_base" ]; then
      cmd_display="$cmd_display --dispatch-csv $dispatch_base --mapping mavlink_ops.yaml"
    fi

    # Run with cwd set to the run folder so the script's own printed paths
    # (Reading file:, Tier 0's "from ...") come out relative and short.
    output="$(cd "$dir" && python3 "$ANALYZER" "$latency_base" --protocol "$proto" "${extra_args[@]}")"

    {
      echo ""
      echo "$cmd_display"
      echo "$output"
    } >> "$readme_path"
  done

  echo "updated $readme_path"
done
