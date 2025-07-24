import matplotlib.pyplot as plt
import argparse
import pandas as pd

plt.rcParams.update({
    "font.family": "sans-serif",
    "text.usetex": True,
})

FORMATS = ["root", "orc", "parquet"]
N_EVENTS = {
    "cms": 39668813,
    "lhcb": 8556118,
}

def plot_runtime(benchmark):
    results = [f"results/{benchmark}_{fmt}.csv" for fmt in FORMATS]
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
    fig, ax = plt.subplots(figsize=(3, 4))

    ax.bar(
        ["ORC", "Parquet", "RNTuple"],
        df_by_fmt.mean().analysis_throughput,
        yerr=df_by_fmt.std().analysis_throughput,
        color=["tab:red", "tab:blue", "tab:orange"],
        error_kw={
            "elinewidth": 0.7,
            "capsize": 1,
        }
    )

    ax.set_xlabel("Data format")
    ax.set_ylabel("Event throughput ($N_{events}/ s$)")

    ax.set_ylim(0, 3.5e6)

    fig.savefig(f"output/runtime_{benchmark}.png", bbox_inches="tight")
    fig.savefig(f"output/runtime_{benchmark}.pdf", bbox_inches="tight")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
          prog="plot_runtime", description="plot the benchmark runtimes"
      )
    parser.add_argument(
        "benchmark", choices=["cms", "lhcb"], help="name of the benchmark"
    )
    args = parser.parse_args()

    plot_runtime(args.benchmark)
