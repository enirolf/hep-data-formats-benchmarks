#!/usr/bin/env sh

set -e

OUTPUT_DIR=./output
DATA_DIR=./data

############################# LHCB #############################

./lhcb $DATA_DIR/B2HHH.root $OUTPUT_DIR/lhcb_root.png
./lhcb $DATA_DIR/B2HHH.parquet $OUTPUT_DIR/lhcb_parquet.png
./lhcb $DATA_DIR/B2HHH.orc $OUTPUT_DIR/lhcb_orc.png

./lhcb $DATA_DIR/B2HHH_ntplcfg.parquet $OUTPUT_DIR/lhcb_ntplcfg_parquet.png
./lhcb $DATA_DIR/B2HHH_ntplcfg.orc $OUTPUT_DIR/lhcb_ntplcfg_orc.png

############################# CMS ##############################

./cms $DATA_DIR/ttjet_signed.root $OUTPUT_DIR/cms_root.png
./cms $DATA_DIR/ttjet_signed.parquet $OUTPUT_DIR/cms_parquet.png
./cms $DATA_DIR/ttjet_signed.orc $OUTPUT_DIR/cms_orc.png

./cms $DATA_DIR/ttjet_signed_ntplcfg.parquet $OUTPUT_DIR/cms_ntplcfg_parquet.png
./cms $DATA_DIR/ttjet_signed_ntplcfg.orc $OUTPUT_DIR/cms_ntplcfg_orc.png
