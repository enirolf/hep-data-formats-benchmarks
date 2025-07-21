# HEP data format comparison benchmark

Benchmark suite to compare the [ROOT](https://root.cern) data format against [Apache ORC](https://orc.apache.org) and [Apache Parquet](https://parquet.apache.org) for high-energy physics (HEP) data analysis.

> [!WARNING]
> The documentation below is not complete yet


## Environment setup

### Apache Arrow and Parquet

```sh
sudo dnf install libarrow-devel parquet-libs-devel
```

### ROOT

TODO add tag to patch

Build ROOT from source according to the [instructions](https://root.cern/install/build_from_source/) with the `arrow` option enabled (`-Darrow=on` in the `cmake` call).

### Python environment

```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> [!IMPORTANT]
> Make sure that the `pyarrow` version in `requirements.txt` matches that of the `libarrow-devel` package!

## Obtaining the input data

TODO

## Converting the data formats

```
usage: convert [-h] [-o, --output_mode {orc,parquet,rntuple}] input_path output_path
```

## Building the benchmarks

```sh
mkdir build && cd build
cmake ..
make
```

## Running the benchmarks

```
./{cms|lhcb} INPUT_PATH [HISTO_PATH]
```
