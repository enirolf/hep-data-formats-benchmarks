import ROOT
import argparse

parser = argparse.ArgumentParser(
    prog="convert",
    description="print all column names in a dataset (must be TTree or RNTuple format)",
)
parser.add_argument("dataset_name", help="name of the dataset")
parser.add_argument("input_path", help="path to the dataset")
parser.add_argument(
    "-o --output-path",
    dest="output_path",
    help="path to output the column names to (stdout by default)",
)

args = parser.parse_args()

rdf = ROOT.RDataFrame(args.dataset_name, args.input_path)
colnames = rdf.GetColumnNames()
if (args.output_path != None):
    with open(args.output_path, "w") as f:
        [f.write(f"{colname}\n") for colname in colnames]
else:
    [print(f"{colname}") for colname in colnames]
