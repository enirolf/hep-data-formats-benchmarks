#!/usr/bin/env sh

set -e

DATA_DIR=./data/
RESULTS_DIR=./results/
BENCHMARK_FORMATS="root orc parquet"
N_RUNS=5

mkdir -p $RESULTS_DIR

function run() {
  PROG=$1
  INPUT_BASE=$2

  echo "***** $PROG *****"
  for fmt in $BENCHMARK_FORMATS; do
    RESULTS_FILE=$RESULTS_DIR/${PROG}_$fmt.csv
    echo -ne "running $INPUT_BASE.$fmt benchmarks..."
    cmd="./$PROG $DATA_DIR/$INPUT_BASE.$fmt"
    echo "init,analysis,main" > $RESULTS_FILE
    for i in $(seq 1 $N_RUNS); do
      ./clear_page_cache
      $cmd >> $RESULTS_FILE
    done
    echo -e " \tdone!"
  done
}

run lhcb B2HHH
run cms DoubleMuon_signed
