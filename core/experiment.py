import subprocess
from pathlib import Path

from core.triplestores import Triplestore, DatabaseVersion


class Experiment:

    def __init__(self, name, base_dir: Path, triplestore: Triplestore, db_version: DatabaseVersion):
        self.name = name
        self.mini_iguana_exec = base_dir.joinpath("mini-iguana")
        self.mini_iguana_exec.chmod(0o755)
        self.triplestore = triplestore
        self.db_version = db_version
        self.update_queries_file = self.db_version.dataset.path.joinpath(self.name).joinpath("update_queries.txt")

        self.results_dir = (triplestore.results_dir
                            .joinpath(f"{db_version.dataset.name}")
                            .joinpath(f"{self.triplestore.name}")
                            .joinpath(f"{self.name}")
                            .joinpath(db_version.timestamp.isoformat()))
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def run_warmup(self):
        # Run the experiment
        mini_iguana_log = self.results_dir.joinpath("warmup.log")

        with open(mini_iguana_log, "wb") as log:
            result = subprocess.run([f"{self.mini_iguana_exec.absolute()}",
                                     "--timeout-secs", f"{6 * 60}",
                                     f"{self.triplestore.sparql_endpoint}",
                                     f"{self.db_version.dataset.warmup_queries.absolute()}",
                                     "warmup"],
                                    stderr=log)
        assert mini_iguana_log.exists()
        assert result.returncode == 0

    def run_experiment(self, qlever_access_token=None):
        # Run the experiment
        csv_result_file = self.results_dir.joinpath("results.csv")
        mini_iguana_log = self.results_dir.joinpath("results.log")

        with open(csv_result_file, "wb") as out, \
                open(mini_iguana_log, "wb") as log:
            cmd = [f"{self.mini_iguana_exec.absolute()}",
                   "--timeout-secs", f"{15 * 60}"]
            if qlever_access_token:
                cmd.extend(["--qlever-access-token", qlever_access_token])
            cmd.extend([
                f"{self.triplestore.update_endpoint}",
                f"{self.update_queries_file.absolute()}",
                "update"])

            result = subprocess.run(cmd,
                                    stdout=out, stderr=log)
        assert csv_result_file.exists()
        assert mini_iguana_log.exists()
        assert result.returncode == 0
