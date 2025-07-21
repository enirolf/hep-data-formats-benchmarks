import ROOT
import awkward as ak
from pyarrow import orc
import pyarrow.parquet as pq
import argparse


def read_as_ak(dataset_name, dataset_path):
    rdf = ROOT.RDataFrame(dataset_name, dataset_path)
    desc = rdf.Describe()
    desc.Print()

    colnames = [str(colname) for colname in rdf.GetColumnNames()]
    ak_array = ak.from_rdataframe(rdf, colnames)
    return ak_array


def convert2orc(ak_array, output_name):
    arrow_table = ak.to_arrow_table(ak_array, extensionarray=False)
    orc.write_table(
        arrow_table,
        output_name,
        compression="ZSTD",
        compression_block_size=1024 * 1024,
        stripe_size=1024 * 1024 * 128,
    )


def convert2parquet(ak_array, output_name):
    ak.to_parquet(
        ak_array,
        output_name,
        extensionarray=False,
        compression="ZSTD",
        compression_level=3,
        parquet_metadata_statistics=False,
        data_page_size=1024 * 1024,
    )


def convert2rntuple(dataset_name, input_path, output_path):
    rdf = ROOT.RDataFrame(dataset_name, input_path)
    opts = ROOT.RDF.RSnapshotOptions()
    opts.fCompressionAlgorithm = ROOT.RCompressionSetting.EAlgorithm.kZSTD
    opts.fCompressionLevel = 1
    opts.fOutputFormat = ROOT.RDF.ESnapshotOutputFormat.kRNTuple
    opts.fOverwriteIfExists = True
    rdf.Snapshot(dataset_name, output_path, "", opts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="convert", description="convert an RNTuple to ORC or Parquet"
    )
    parser.add_argument("dataset_name", help="name of the dataset")
    parser.add_argument("input_path", help="path to the input RNTuple")
    parser.add_argument("output_path", help="path for the output file")
    parser.add_argument(
        "-o, --output_mode",
        dest="output_mode",
        choices=["orc", "parquet", "rntuple"],
        default="orc",
        help="format to convert to",
    )

    args = parser.parse_args()

    if args.output_mode == "rntuple":
        convert2rntuple(args.dataset_name, args.input_path, args.output_path)
        exit(0)

    ak_array = read_as_ak(args.dataset_name, args.input_path)
    if args.output_mode == "orc":
        convert2orc(ak_array, args.output_path)
    if args.output_mode == "parquet":
        convert2parquet(ak_array, args.output_path)
