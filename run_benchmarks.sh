#!/usr/bin/env sh

set -e

DATA_DIR=/data/ssdext4/fdegeus/escience25
RESULTS_DIR=./results/
BENCHMARK_FORMATS="root orc parquet"
N_RUNS=5

mkdir -p $RESULTS_DIR

function run() {
  PROG=$1
  INPUT_BASE=$2

  echo "***** $PROG *****"
  for fmt in $BENCHMARK_FORMATS; do
    INPUT_FILE=$DATA_DIR/$INPUT_BASE.$fmt

    if [ ! -f "$INPUT_FILE" ]; then
      echo "$INPUT_FILE does not exist, skipping"
      continue
    fi

    RESULTS_FILE=$RESULTS_DIR/${INPUT_BASE}_$fmt.csv
    echo -ne "running $INPUT_BASE benchmarks for $fmt..."
    cmd="./$PROG $INPUT_FILE"
    echo "init,analysis,main" > $RESULTS_FILE
    for i in $(seq 1 $N_RUNS); do
      ./clear_page_cache
      $cmd >> $RESULTS_FILE
    done
    echo -e " \tdone!"
  done
}

run lhcb B2HHH
run lhcb B2HHH_ntplcfg

run cms ttjet_signed
run cms ttjet_signed_ntplcfg
