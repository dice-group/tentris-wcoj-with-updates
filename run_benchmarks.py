import os
from datetime import datetime
from pathlib import Path
from subprocess import Popen
import argparse

from core import util, global_defs
from core.experiment import Experiment
from core.triplestores import Tentris, Oxigraph, QLever, Fuseki, DatabaseVersion, Virtuoso, GraphDB
from core.datasets import DBpedia2015, SWDF, Wikidata, BSBM
from core.util import bash

import logging


logging.basicConfig(filename=f'hypertrie_insdel_bench_{datetime.now().isoformat()}.log', encoding='utf-8',
                    level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def download_triplestores(triplestore_dir: Path):
    from io import BytesIO
    from urllib.request import urlopen
    from zipfile import ZipFile
    zipurl = 'https://files.dice-research.org/datasets/hypertrie_update/binaries.zip'
    with urlopen(zipurl) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall()
    triplestore_dir.mkdir(parents=True, exist_ok=True)
    bash(f"mv insdel-paper-binaries/* {triplestore_dir.absolute()}")
    bash(f"rm -rf insdel-paper-binaries")

    fuseki_version = Fuseki("", Path()).version
    # Fuseki
    util.download_file(url=f"https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-{fuseki_version}.tar.gz",
                       checksum=0x650d4576927cb4e330c372c46f76dde3fbd683b59cc692bac501b545097402c58c2254b25502e4225ba6f7d4c79ee12c5e43b78124115dbbe6371c487aa8604f,
                       checksum_type="sha512",
                       dest=triplestore_dir.joinpath(f"apache-jena-fuseki-{fuseki_version}.tar.gz"))
    triplestore_dir.joinpath("fuseki").mkdir(exist_ok=True)
    bash(f"tar -xf {triplestore_dir}/apache-jena-fuseki-{fuseki_version}.tar.gz -C {triplestore_dir}/fuseki")
    triplestore_dir.joinpath(f"apache-jena-fuseki-{fuseki_version}.tar.gz").unlink(missing_ok=True)
    util.download_file(url=f"https://dlcdn.apache.org/jena/binaries/apache-jena-{fuseki_version}.tar.gz",
                       checksum=0x9a66044573ca269c0a9a1191fe7a8e1925f2d77e2e0bdadddd68616a1bafed1d9819c0b2988dd03614ec778a3882ccb091d825976fc0837406916f9f001b701c,
                       checksum_type="sha512",
                       dest=triplestore_dir.joinpath(f"apache-jena-{fuseki_version}.tar.gz"))
    bash(f"tar -xf {triplestore_dir}/apache-jena-{fuseki_version}.tar.gz -C {triplestore_dir}/fuseki")
    triplestore_dir.joinpath(f"apache-jena-{fuseki_version}.tar.gz").unlink(missing_ok=True)

    # Virtuoso
    bash(
        f"curl -L https://github.com/openlink/virtuoso-opensource/releases/download/v7.2.12/virtuoso-opensource.x86_64-generic_glibc25-linux-gnu.tar.gz"
        f" | tar -xz -C {triplestore_dir}")
    triplestore_dir.joinpath("virtuoso-opensource").rename(triplestore_dir.joinpath("virtuoso"))

    # GraphDB
    zipurl = "https://download.ontotext.com/owlim/5fc32688-dc98-11ee-ac5d-42843b1b6b38/graphdb-10.6.2-dist.zip"  # TODO: see for how long this link will work
    with urlopen(zipurl) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall()
    bash(f"mv graphdb-10.6.2/ {triplestore_dir.absolute()}")
    bash(f"mv {triplestore_dir.absolute().joinpath('graphdb-10.6.2')} "
         f"{triplestore_dir.absolute().joinpath('graphdb')}")


if __name__ == "__main__":
    from getpass import getpass

    parser = argparse.ArgumentParser(description='Hypertrie Insertion Deletion Benchmark')
    parser.add_argument('--download-triplestores', action='store_true', default=True, help='Download triplestores')
    parser.add_argument('--download-datasets', action='store_true', default=True, help='Download datasets')
    parser.add_argument('--reuse-existing-database', action='store_true', default=False,
                        help='Reuse existing database if present')
    parser.add_argument('--triplestore-ram', type=int, default=8, help='RAM limit for triplestores in GB')
    keep_database_group = parser.add_mutually_exclusive_group()
    keep_database_group.add_argument('--keep-database', default=False, action='store_true',
                                     help='Keep the database after experiments')
    keep_database_group.add_argument('--no-keep-database', dest='keep_database', action='store_false',
                                     help='Do not keep the database after experiments')
    warmup_group = parser.add_mutually_exclusive_group()
    warmup_group.add_argument('--run-warmup-queries', dest='run_warmup_queries', action='store_true', default=True,
                              help='Run warmup queries')
    warmup_group.add_argument('--no-run-warmup-queries', dest='run_warmup_queries', action='store_false',
                              help='Do not run warmup queries')
    update_group = parser.add_mutually_exclusive_group()
    update_group.add_argument('--run-update-queries', dest='run_update_queries', default=True, action='store_true',
                              help='Run update queries')
    update_group.add_argument('--no-run-update-queries', dest='run_update_queries', action='store_false',
                              help='Do not run update queries')

    allowed_triplestores = global_defs.triplestores
    allowed_datasets = global_defs.datasets

    parser.add_argument('--triplestores', choices=allowed_triplestores, nargs='+',
                        default=['tentris-insdel', 'fuseki', 'graphdb', 'virtuoso'], help='List of triplestores to use')
    parser.add_argument('--datasets', choices=allowed_datasets, nargs='+', default=['dbpedia', 'wikidata'],
                        help='List of datasets to use')
    args = parser.parse_args()
    ram_limit_g = args.triplestore_ram

    # ask for user password. We will need it for certain commands
    pw = getpass(f"Please enter password for user '{os.getlogin()}': ")
    logging.info(f"User password entered")
    # TODO: add other triplestores
    # TODO: test everything
    assert os.environ.get('BEAM_PASSWORD') is not None
    logging.info(f"BEAM_PASSWORD environment variable set")

    working_dir = Path.cwd().joinpath("benchmark_workspace")
    logging.info(f"Working directory: {working_dir}")
    triplestore_dir: Path = working_dir.joinpath("triplestores")
    logging.info(f"Triplestore directory: {triplestore_dir}")
    benchmarks_dir: Path = working_dir.joinpath("benchmarks")
    logging.info(f"Benchmarks directory: {benchmarks_dir}")
    results_dir: Path = working_dir.joinpath("results")
    logging.info(f"Results directory: {results_dir}")

    # run tentris-os-optimizations.sh
    optimizations_path = Path.cwd().joinpath('tentris-os-optimizations.sh')
    optimizations_path.chmod(0o755)
    # Run the command and capture the output
    print(bash(f'echo "{pw}" | sudo -S bash "{optimizations_path.absolute()}"'))
    logging.info(f"Ran tentris-os-optimizations.sh")
    if not triplestore_dir.exists():
        if args.download_triplestores:
            download_triplestores(triplestore_dir)
            logging.info(f"Downloaded triplestores")
    # download mini-iguana and make it executable
    if not working_dir.joinpath('mini-iguana').exists():
        bash(
            f"curl -L 'https://github.com/dice-group/mini-iguana/releases/latest/download/mini-iguana' > '{working_dir.joinpath('mini-iguana').absolute()}'")
        working_dir.joinpath('mini-iguana').absolute().chmod(0o755)
        logging.info(f"Downloaded mini-iguana")

    datasets = list()
    if 'swdf' in args.datasets:
        datasets += [SWDF(benchmarks_dir)]
    if 'dbpedia' in args.datasets:
        datasets += [DBpedia2015(benchmarks_dir)]
    if 'wikidata' in args.datasets:
        datasets += [Wikidata(benchmarks_dir)]
    if 'bsbm' in args.datasets:
        datasets += [BSBM(benchmarks_dir)]

    logging.info(f"Datasets: {[ds.name for ds in datasets]}")
    triplestores = list()
    if 'tentris-baseline' in args.triplestores:
        triplestores += [Tentris(name="tentris-baseline", base_dir=working_dir)]
    if 'tentris-insdel' in args.triplestores:
        triplestores += [Tentris(name="tentris-insdel", base_dir=working_dir)]
    if 'oxigraph' in args.triplestores:
        triplestores += [Oxigraph(name="oxigraph", base_dir=working_dir)]
    if 'fuseki' in args.triplestores:
        triplestores += [Fuseki(name="fuseki", base_dir=working_dir)]
    if 'graphdb' in args.triplestores:
        triplestores += [GraphDB(name="graphdb", base_dir=working_dir)]
    if 'virtuoso' in args.triplestores:
        triplestores += [Virtuoso(name="virtuoso", base_dir=working_dir)]
    if 'qlever' in args.triplestores:
        triplestores += [QLever(name="qlever", base_dir=working_dir)]

    logging.info(f"Triplestores: {[ts.name for ts in triplestores]}")

    if args.download_datasets:
        for dataset in datasets:
            if args.download_datasets and not dataset.dataset_path.exists():
                logging.info(f"Downloading {dataset.name}")
                dataset.download()
        logging.info(f"Downloaded datasets")

    experiments = {
        "swdf": [
            "changesets10"  # only for debugging
        ],
        "dbpedia": [
            "changesets"
        ],
        "wikidata": [
            "changesets10",
            "changesets100",
            "changesets1000",
            "changesets10000",
            "changesets100000",
            "changesets1000000",
        ],
        "bsbm": [
            "changesets"
        ]
    }
    logging.info(f"Experiments: {experiments}")

    # load datasets into triplestores
    for triplestore in triplestores:
        for dataset in datasets:

            # Warmup and run experiments
            print(bash(
                f'echo "{pw}" | sudo -S sh -c "/usr/bin/sync; /usr/bin/echo 3 > /proc/sys/vm/drop_caches && /usr/bin/echo \\"caches dropped\\""'))

            db_version = DatabaseVersion.for_dataset(dataset)
            if not args.reuse_existing_database:
                if triplestore.dataset_db_dir(dataset).exists():
                    logging.info(f"Deleting {dataset.name} from {triplestore.name}")
                    triplestore.delete_database(dataset)
                    logging.info(f"Deleted {dataset.name} from {triplestore.name}")
                logging.info(f"Loading {dataset.name} into {triplestore.name}")
                db_version = triplestore.load(dataset)
                logging.info(f"Loaded {dataset.name} into {triplestore.name}")
            if not triplestore.dataset_db_dir(dataset).exists():
                raise RuntimeError(f"Database {dataset.name} not found for {triplestore.name}")
            # TODO: re-enable backup and restore
            # backup database with beam
            # logging.info(f"Backing up {dataset.name} in {triplestore.name}")
            # triplestore.backup(db_version)
            # logging.info(f"Backed up {dataset.name} in {triplestore.name}")
            # for restoring from a backup
            # triplestore.restore(DatabaseVersion(datetime.fromisoformat("2024-04-11T13:25:57.094210"))

            if triplestore.name == "tentris-baseline":
                # triplestore tentris-baseline does not support updates
                if not args.keep_database:
                    logging.info(f"Deleting {dataset.name} from {triplestore.name}")
                    triplestore.delete_database(dataset)
                    logging.info(f"Deleted {dataset.name} from {triplestore.name}")
                continue

            if args.run_warmup_queries or args.run_update_queries:
                logging.info(f"Starting {triplestore.name}")
                handle: Popen[bytes] = triplestore.start(db_version)
                logging.info(f"Started {triplestore.name}")
                triplestore_running = lambda: handle.poll() is None

                assert triplestore_running()
                logging.info(f"Waiting for {triplestore.name} to initialize")
                util.wait_until_available(triplestore.sparql_endpoint, timeout=20 * 60)  # up to 20 minutes

                if args.run_warmup_queries:
                    logging.info(f"Running warmup for {dataset.name} in {triplestore.name}")
                    Experiment("warmup", working_dir, triplestore, db_version).run_warmup()
                    logging.info(f"Ran warmup for {dataset.name} in {triplestore.name}")

                if args.run_update_queries:
                    for experiment_name in experiments[dataset.name]:
                        experiment = Experiment(experiment_name, working_dir, triplestore, db_version)
                        assert triplestore_running()
                        logging.info(f"Running {experiment_name} for {dataset.name} in {triplestore.name}")
                        experiment.run_experiment("_000000000000" if triplestore.name == "qlever" else None)
                        logging.info(f"Ran {experiment_name} for {dataset.name} in {triplestore.name}")
                assert triplestore_running()

                logging.info(f"Stopping {triplestore.name}")
                triplestore.stop(handle, dataset)
                logging.info(f"Stopped {triplestore.name}")

            if not args.keep_database:
                # free up space by deleting database
                logging.info(f"Deleting {dataset.name} from {triplestore.name}")
                triplestore.delete_database(dataset)
                logging.info(f"Deleted {dataset.name} from {triplestore.name}")
