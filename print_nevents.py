import ROOT
import argparse

parser = argparse.ArgumentParser(
    prog="print_nevents",
    description="print the number of events in a dataset (must be TTree or RNTuple format)",
)
parser.add_argument("dataset_name", help="name of the dataset")
parser.add_argument("input_path", help="path to the dataset")
parser.add_argument(
    "-o --output-path",
    dest="output_path",
    help="path to output the number of events to (stdout by default)",
)

args = parser.parse_args()

rdf = ROOT.RDataFrame(args.dataset_name, args.input_path)
n_events = rdf.Count()
if (args.output_path != None):
    with open(args.output_path, "w") as f:
        f.write(f"{n_events.GetValue()}\n")
else:
    print(f"{n_events.GetValue()}")
