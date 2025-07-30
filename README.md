# Evaluation: Efficient Updates for Worst-Case Optimal Join Triple Stores

## Paper Data

All measurements, tables and figures used for the paper are in the [`data.zip`](./data.zip) archive.

The technical report for the algorithm is provided as [`hypertrie_update_technical_report.pdf`](./hypertrie_update_technical_report.pdf). It also contains a table that compares offline bulk loading speed and storage efficiency of the tested systems.

The proof of time complexity of Hypertrie updates is provided as [`hypertrie_update_proofs.pdf`](./hypertrie_update_proofs.pdf).

Due to file size limitations, some files cannot be hosted anonymously. Therefore, all links in this repository have been anonymized to maintain double-blind review standards. Upon acceptance, we will provide the original links.
## Requirements

Following executables need to be installed:

- `docker`
- `bash`
- `curl`
- `wget`
- `zstd`
- (`python 3.13`)

Add `ulimit -n 64000` to your .bashrc. This increases the number of open files allowed by the system.
Log out and in again to apply the changes.

## Initializing the Environment

We recommend to install python 3.13 through pyenv. To do so, run the following commands:

```bash
curl https://pyenv.run | bash
export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
```

Then, install python 3.13:

```bash
pyenv install 3.13 # make sure this is successful. If not, install the required dependencies from your package manager, run again and confirm [Y] if asked to continue because it already exists. 
```

Afterward, check out the repository and set up the environment:

```bash
git clone <this-repo>
cd <this-repo>
pyenv local 3.13
pip install -r requirements.txt
```

## Run the Experiments

Navigate to the repository root, ensure that the correct venv or pyenv is activated and run the following:

```bash
python run_benchmarks.py --tripletstore tentris-baseline tentris-insdel graphdb virtuoso oxigraph --dataset dbpedia wikidata
```

This will run the following steps:

1. download or build the triple stores to [`benchmark_workspace/triplestores`](./benchmark_workspace/triplestores)
2. download the benchmarks to [`benchmark_workspace/benchmarks`](./benchmark_workspace/benchmarks)
3. download the benchmarking tool to [`benchmark_workspace/mini-iguana`](./benchmark_workspace/mini-iguana)
4. Runs the benchmark for each triple store and dataset:
    1. load the dataset
    2. run the warmup queries
    3. run the update changesets

   Results are stored to [`benchmark_workspace/results`](./benchmark_workspace/results)

## Generate Tables and Figures

Navigate to the repository root, ensure that the correct venv or pyenv is activated.

Make sure the requirements are installed:

```bash
pip install -r requirements_eval.txt
```

Run the following command to generate the tables and figures:

```bash
python generate_tables.py
python generate_figures.py
```

Tables are stored to [`tables`](./tables) and figures to [`figures`](./figures).