#!/usr/bin/env bash
# Tail-sampling lab -- self-contained. Reproduces everything in ~6 minutes.
set -e
cd "$(dirname "$0")"
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. inference.proto
mkdir -p out
{
  echo "Tail-sampling lab -- sampling, exemplars, and the long stream"
  echo "python $(python3 -V 2>&1)  grpcio $(python3 -c 'import grpc;print(grpc.__version__)')"
  echo "opentelemetry-sdk $(python3 -c 'import opentelemetry.sdk.version as v;print(v.__version__)')"
  date -u
  python3 exp_a_visibility.py
  python3 exp_b_tailsampling.py
  python3 exp_c_exemplar_resolution.py
  python3 exp_d_a96_and_ledger.py
} 2>&1 | tee out/OUTPUT.txt
