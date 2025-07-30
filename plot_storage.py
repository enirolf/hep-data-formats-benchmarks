from bz2 import compress
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import argparse
import pandas as pd
import os
import ROOT
import duckdb
import subprocess
import json

plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "text.usetex": True,
        "axes.autolimit_mode": "round_numbers"
    }
)

FORMATS = ["orc", "parquet", "root"]
DATA_SET_NAMES = {
    "cms": "Events",
    "lhcb": "DecayTree",
}
DATA_SET_FILE_NAMES = {
    # "cms": "DoubleMuon_signed",
    "cms": "ttjet_trimmed",
    "lhcb": "B2HHH",
}
N_EVENTS = {
    # "cms": 39668813,
    "cms": 1620657,
    "lhcb": 8556118,
}


def get_event_size_rntuple(benchmark, uncompressed=False):
    path = (
        f"data/{DATA_SET_FILE_NAMES[benchmark]}_uncompressed.root"
        if uncompressed
        else f"data/{DATA_SET_FILE_NAMES[benchmark]}.root"
    )
    inspector = ROOT.Experimental.RNTupleInspector.Create(
        DATA_SET_NAMES[benchmark], path
    )
    size = inspector.GetCompressedSize()
    return size / N_EVENTS[benchmark]


def get_event_size_parquet(benchmark, uncompressed=False):
    path = (
        f"data/{DATA_SET_FILE_NAMES[benchmark]}_uncompressed.parquet"
        if uncompressed
        else f"data/{DATA_SET_FILE_NAMES[benchmark]}.parquet"
    )
    output = duckdb.sql(
        f"SELECT sum(total_compressed_size) FROM parquet_metadata('{path}')"
    )
    size = output.fetchall()[0][0]
    return size / N_EVENTS[benchmark]


def get_event_size_orc(benchmark, uncompressed=False):
    path = (
        f"data/{DATA_SET_FILE_NAMES[benchmark]}_uncompressed.orc"
        if uncompressed
        else f"data/{DATA_SET_FILE_NAMES[benchmark]}.orc"
    )
    output = subprocess.run(
        ["orc-metadata", path],
        capture_output=True,
    ).stdout
    metadata = json.loads(output)
    size = 0
    for stripe in metadata["stripes"]:
        size += stripe["data"]
    return size / N_EVENTS[benchmark]


def get_filesize(benchmark, fmt, uncompressed=False):
    filename = (
        f"data/{DATA_SET_FILE_NAMES[benchmark]}_uncompressed.{fmt}"
        if uncompressed
        else f"data/{DATA_SET_FILE_NAMES[benchmark]}.{fmt}"
    )
    statinfo = os.stat(filename)
    return statinfo.st_size


def plot_filesize(benchmark):
    filesizes = [get_filesize(benchmark, fmt) / 1e9 for fmt in FORMATS]
    fig, ax = plt.subplots(figsize=(3, 2.5))

    ax.bar(
        ["ORC", "Parquet", "RNTuple"],
        filesizes,
        color=["tab:red", "tab:blue", "tab:orange"],
    )

    ax.set_xlabel("Data format")
    ax.set_ylabel("File size (GB)")
    # ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_ylim(0, 1.25 if benchmark == "lhcb" else 4)

    fig.savefig(f"output/filesize_{benchmark}.png", bbox_inches="tight")
    fig.savefig(f"output/filesize_{benchmark}.pdf", bbox_inches="tight")


def plot_event_size(benchmark):
    # event_sizes = [
    #     get_event_size_orc(benchmark),
    #     get_event_size_parquet(benchmark),
    #     get_event_size_rntuple(benchmark)
    # ]
    event_sizes = [get_filesize(benchmark, f) / N_EVENTS[benchmark] for f in ["orc", "parquet", "root"]]
    fig, ax = plt.subplots(figsize=(3, 2.5))

    bar_plot = ax.bar(
        ["ORC", "Parquet", "RNTuple"],
        event_sizes,
        color=["tab:red", "tab:blue", "tab:orange"],
    )

    ax.bar_label(
        bar_plot,
        labels=[
            f"{event_sizes[0] / event_sizes[-1]:.2f}x",
            f"{event_sizes[1] / event_sizes[-1]:.2f}x",
            "",
        ],
        padding=-20,
        color="white",
        size=14,
    )

    ax.set_xlabel("Data format", fontdict={"size": 14})
    ax.set_ylabel("Average event size (B)", fontdict={"size": 14})
    ax.tick_params(axis='both', which='major', labelsize=12)

    # ax.set_ylim(0, 160 if benchmark=="lhcb" else 30)

    fig.savefig(f"output/event_size_{benchmark}.png", bbox_inches="tight")
    fig.savefig(f"output/event_size_{benchmark}.pdf", bbox_inches="tight")

def plot_compression_factor(benchmark):
    compression_factors = [
        get_filesize(benchmark, "orc", True) / get_filesize(benchmark, "orc"),
        get_filesize(benchmark, "parquet", True) / get_filesize(benchmark, "parquet"),
        get_filesize(benchmark, "root", True) / get_filesize(benchmark, "root")
    ]

    for c in compression_factors:
        print(c)

    fig, ax = plt.subplots(figsize=(3, 2.5))

    ax.bar(
        ["ORC", "Parquet", "RNTuple"],
        compression_factors,
        color=["tab:red", "tab:blue", "tab:orange"],
    )

    ax.set_xlabel("Data format")
    ax.set_ylabel("Compression factor")

    fig.savefig(f"output/compression_factor_{benchmark}.png", bbox_inches="tight")
    fig.savefig(f"output/compression_factor_{benchmark}.pdf", bbox_inches="tight")

if __name__ == "__main__":
    plot_filesize("cms")
    plot_event_size("cms")
    # plot_compression_factor("cms")

    plot_filesize("lhcb")
    plot_event_size("lhcb")
    # plot_compression_factor("lhcb")
