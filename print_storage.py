import ROOT
import argparse
import os

def get_filesize(benchmark, fmt, uncompressed=False):
    filename = f"data/{benchmark}.{fmt}"
    statinfo = os.stat(filename)
    return statinfo.st_size

def get_n_entries(dataset_name, benchmark):
    ntuple = ROOT.RNTupleReader.Open(dataset_name, f"data/{benchmark}.root")
    return ntuple.GetNEntries()

if __name__ == "__main__":
    print("benchmark,format,n_events,file_size")
    for dataset_name, benchmark in [("DecayTree", "B2HHH"), ("Events", "ttjet_signed")]:
        n_events = get_n_entries(dataset_name, benchmark)

        for fmt in ["root", "orc", "parquet"]:
            filesize = get_filesize(benchmark, fmt)
            print(f"{benchmark},{fmt},{n_events},{filesize}")

        for fmt in ["orc", "parquet"]:
            filesize = get_filesize(f"{benchmark}_ntplcfg", fmt)
            print(f"{benchmark}_ntplcfg,{fmt},{n_events},{filesize}")
        

    
