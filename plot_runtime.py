import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import argparse
import pandas as pd

plt.rcParams.update({
    "font.family": "sans-serif",
    "text.usetex": True,
    "axes.autolimit_mode": "round_numbers"
})

FORMATS = ["root", "orc", "parquet"]
N_EVENTS = {
    # "cms": 39668813,
    "cms": 1620657,
    "lhcb": 8556118,
}

def plot_runtime(benchmark, results_dir):
    results = [f"{results_dir}/{benchmark}_{fmt}.csv" for fmt in FORMATS]
    dfs = [pd.read_csv(r) for r in results]
    df = pd.concat(dfs, keys=FORMATS, names=["format", "run"])
    df["analysis"] /= 1e6
    df["init"] /= 1e6
    df["main"] /= 1e6
    df["analysis_throughput"] = N_EVENTS[benchmark] / df.analysis
    print(df)
    df_by_fmt = df.groupby("format")
    print(df_by_fmt.mean())
    print(df_by_fmt.std())
    fig, ax = plt.subplots(figsize=(3, 2.5))

    bar_plot = ax.bar(
        ["ORC", "Parquet", "RNTuple"],
        df_by_fmt.mean().analysis_throughput,
        yerr=df_by_fmt.std().analysis_throughput,
        color=["tab:red", "tab:blue", "tab:orange"],
        error_kw={
            "elinewidth": 0.7,
            "capsize": 1,
        },
    )

    ax.bar_label(
        bar_plot,
        labels=[
            f"{df_by_fmt.mean().analysis_throughput.orc / df_by_fmt.mean().analysis_throughput.root:.2f}x",
            f"{df_by_fmt.mean().analysis_throughput.parquet / df_by_fmt.mean().analysis_throughput.root:.2f}x",
            "",
        ],
        padding=-18,
        color="white",
        size=12,
    )


    ax.set_xlabel("Data format", fontdict={"size": 12})
    ax.set_ylabel("Event throughput ($N_{events}/ s$)", fontdict={"size": 12})
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
    ax.set_yscale("log")
    ax.tick_params(axis='both', which='major', labelsize=12)

    # ax.set_ylim(
    #     0, 6e6 if benchmark == "lhcb" else 2.5e8
    # )

    fig.savefig(f"output/runtime_{benchmark}.png", bbox_inches="tight")
    fig.savefig(f"output/runtime_{benchmark}.pdf", bbox_inches="tight")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
          prog="plot_runtime", description="plot the benchmark runtimes"
      )
    parser.add_argument(
        "results_dir", default="./results/", help="path to the results directory"
    )
    args = parser.parse_args()

    plot_runtime("cms", args.results_dir)
    plot_runtime("lhcb", args.results_dir)
