import ROOT
import awkward as ak
from pyarrow import orc
import pyarrow.parquet as pq
import argparse

ROOT.gInterpreter.Declare("""
bool convert_to_signed(float x) {
    return x > 10;
}
""")


def convert(dataset_name, input_path, output_path):
    rdf = ROOT.RDataFrame(dataset_name, input_path)
    colnames = [str(colname) for colname in rdf.GetColumnNames()]
    rdf_converted = rdf

    for colname in colnames:
        new_typename = rdf.GetColumnType(colname).replace("std::uint_", "std::int_")
        rdf_converted = rdf_converted.Redefine(colname, f"({new_typename})({colname})")

    opts = ROOT.RDF.RSnapshotOptions()
    opts.fCompressionAlgorithm = ROOT.RCompressionSetting.EAlgorithm.kZSTD
    opts.fCompressionLevel = 1
    opts.fOutputFormat = ROOT.RDF.ESnapshotOutputFormat.kRNTuple
    opts.fOverwriteIfExists = True
    rdf_converted.Snapshot(dataset_name, output_path, "", opts)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="convert", description="convert unsigned columns to signed"
    )
    parser.add_argument("dataset_name", help="name of the dataset")
    parser.add_argument("input_path", help="path to the input RNTuple")
    parser.add_argument("output_path", help="path for the output file")

    args = parser.parse_args()

    convert(args.dataset_name, args.input_path, args.output_path)
