#!/usr/bin/env bash

set -euo pipefail

context_dir="${CONTEXT_DIR:-/data/context}"
output_root="${OUTPUT_ROOT:-/data/output}"
seed_context_dir="/app/context/active"

mkdir -p "${context_dir}" "${output_root}/gemini_profiles" "${output_root}/material_matches" "${output_root}/databases"

if [ -d "${seed_context_dir}" ] && [ -z "$(find "${context_dir}" -mindepth 1 -maxdepth 1 2>/dev/null)" ]; then
  cp -R "${seed_context_dir}/." "${context_dir}/"
fi

cd /app
exec python scripts/pipeline_launcher.py "$@"
