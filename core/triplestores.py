import datetime
import shutil
import subprocess
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from subprocess import Popen

from core.datasets import Dataset
from core.global_defs import ram_limit_g
from core.util import bash, wait_until_available


@dataclass
class DatabaseVersion:
    timestamp: datetime
    dataset: Dataset

    @staticmethod
    def for_dataset(dataset: Dataset):
        return DatabaseVersion(datetime.now(), dataset)


class Triplestore:

    def __init__(self, name, base_dir: Path) -> None:
        self.name: str = name
        self.sparql_endpoint: str = None
        self.update_endpoint: str = None
        self.installation_dir: Path = base_dir.joinpath(f"triplestores/{self.name}")
        self.database_dir: Path = base_dir.joinpath(f"databases/{self.name}")
        self.database_dir.mkdir(parents=True, exist_ok=True)

        self.results_dir: Path = base_dir.joinpath("results")

    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        pass

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        pass

    def load(self, dataset: Dataset) -> DatabaseVersion:
        import time
        elapsed = time.perf_counter_ns()
        db_version = self._load_impl(dataset)
        elapsed = time.perf_counter_ns() - elapsed

        try:
            # write elapsed time and memory footprint to file
            import json
            (self.loading_results_dir(db_version)
            .joinpath("loading_stats.json")
            .write_text(json.dumps({
                "ns": elapsed,
                "bytes": bash(f"du -s '{self.dataset_db_dir(dataset).absolute()}'").split("\t")[0]
            })))
        finally:
            return db_version

    def stop(self, handle: Popen[bytes], dataset: Dataset = None):
        handle.terminate()  # TODO: SIGINT maybe, because of tentris?
        for i in range(30):  # wait up to 30 seconds
            time.sleep(1)
            if handle.poll() is not None:
                break
        handle.kill()

    def backup(self, db_version: DatabaseVersion) -> str:
        cwd = Path(os.getcwd())
        os.chdir(self.database_dir.absolute())
        temp_name = F"{self.name}_{db_version.dataset.name}_{db_version.timestamp.isoformat()}"
        os.rename(db_version.dataset.name, temp_name)
        try:
            beam_key = bash(f"beam up --result-only {temp_name}")
            self.loading_results_dir(db_version).joinpath("beam_key").write_text(beam_key, 'utf8')
            return beam_key
        finally:
            os.rename(temp_name, db_version.dataset.name)
            os.chdir(cwd)

    def restore(self, db_version: DatabaseVersion):
        cwd = Path(os.getcwd())
        os.chdir(self.database_dir.absolute())
        temp_name = F"{self.name}_{db_version.dataset.name}_{db_version.timestamp.isoformat()}"
        try:
            beam_key = self.loading_results_dir(db_version).joinpath("beam_key").read_text('utf8')
            bash(f"beam down {beam_key}")
            os.rename(temp_name, db_version.dataset.name)
        finally:
            os.chdir(cwd)

    def delete_database(self, dataset: Dataset) -> None:
        if self.dataset_db_dir(dataset).exists():
            shutil.rmtree(self.dataset_db_dir(dataset))

    def dataset_db_dir(self, dataset: Dataset) -> Path:
        return self.database_dir.joinpath(dataset.name)

    def loading_results_dir(self, db_version: DatabaseVersion) -> Path:
        return self.results_dir.joinpath(
            f"{db_version.dataset.name}/{self.name}/loading/{db_version.timestamp.isoformat()}")


class Tentris(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.installation_dir.joinpath("tentris_loader").chmod(0o755)
        self.installation_dir.joinpath("tentris_server").chmod(0o755)
        self.sparql_endpoint: str = "http://localhost:9080/sparql"
        self.update_endpoint: str = "http://localhost:9080/sparql"

    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)  # intentionally throw if exists

        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.loading_results_dir(db_version)

        subprocess.run([f"{self.installation_dir.absolute()}/tentris_loader",
                        "--file", f"{dataset.dataset_path}",
                        "--storage", self.dataset_db_dir(dataset),
                        "--logfiledir",
                        f"{log_dir.absolute()}",
                        "--loglevel", "trace"])
        return db_version

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        """
        Start the server in the background and return the handle
        :param db_version:  The database version to start
        :return:          The handle to the process
        """
        return subprocess.Popen([f"{self.installation_dir.absolute()}/tentris_server",
                                 "-j", f"{1}",
                                 "--storage", self.dataset_db_dir(db_version.dataset),
                                 "--logfiledir",
                                 f"{self.loading_results_dir(db_version)}/",
                                 "--loglevel", "info"])


class Oxigraph(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executable_path: Path = self.installation_dir.joinpath("oxigraph_server_v0.4.7_x86_64_linux_gnu")
        self.executable_path.chmod(0o755)
        self.sparql_endpoint: str = "http://localhost:7878/"
        self.update_endpoint: str = "http://localhost:7878/update"

    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)  # intentionally throw if exists

        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.loading_results_dir(db_version)
        log_dir.mkdir(parents=True, exist_ok=True)

        with open(log_dir.joinpath("loading.log"), "w") as f:
            subprocess.run([f"{self.executable_path}",
                            "load",
                            "--file", f"{dataset.dataset_path}",
                            "--location", db_dir,
                            "--lenient"], stdout=f, stderr=subprocess.STDOUT)
        return db_version

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        return subprocess.Popen([f"{self.executable_path}",
                                 "serve",
                                 "--location", str(self.dataset_db_dir(db_version.dataset))])  # TODO: log


class QLever(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.build()
        self.sparql_endpoint: str = "http://0.0.0.0:8888/sparql"
        self.update_endpoint: str = "http://0.0.0.0:8888/update"

    def build(self):
        from os.path import isdir
        if not isdir(f'{self.installation_dir.absolute()}'):
            try:
                bash(
                        f"git init  {self.installation_dir.absolute()} && cd  {self.installation_dir.absolute()} && git remote add origin https://github.com/ad-freiburg/qlever.git && git fetch --depth 1 origin f2562fe2ca1da7e1704aa2a5b0473be08e5f296c && git checkout FETCH_HEAD && cd -")
                bash(f"docker build -t qlever {self.installation_dir.absolute()}")
            except Exception as e:
                shutil.rmtree(f"{self.installation_dir.absolute()}")
                sys.exit(1)

    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)

        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.loading_results_dir(db_version)
        log_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy("Qleverfile", f"{db_dir.absolute()}")
        uid = str(os.getuid())
        gid = str(os.getgid())
        subprocess.run(["docker", "run", "-it", "--rm",
                        "-e", f"UID={uid}", "-e", f"GID={gid}",
                        "-v", f"{db_dir.absolute()}:/data/database",
                        "--mount", f"type=bind,source={log_dir.absolute()},destination=/data/logs",
                        "-w" "/data",
                        "--name", f"qlever-{dataset.name}-index",
                        "qlever",
                        "-c",
                        f'cd database && qlever index --name dbpedia | tee /data/logs/{dataset.name}-index-log.txt',
                        ], check=True)
        return db_version

    def stop(self, handle: Popen[bytes], dataset: Dataset):
        bash(f"docker container kill qlever-{dataset.name}-start")
        time.sleep(5)
        handle.kill()

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        dataset = db_version.dataset
        db_dir = self.dataset_db_dir(dataset)
        log_dir = self.loading_results_dir(db_version)

        # return a subprocess handle that stops the container and removes it
        uid = str(os.getuid())
        gid = str(os.getgid())
        return subprocess.Popen(["docker", "run", "-it", "--rm",
                                 "-e", f"UID={uid}", "-e", f"GID={gid}",
                                 "-v", f"{db_dir.absolute()}:/data/database",
                                 "-p", "8888:8888",
                                 "--mount", f"type=bind,source={log_dir.absolute()},destination=/data/logs",
                                 "-w" "/data",
                                 "--name", f"qlever-{dataset.name}-start",
                                 "qlever",
                                 "-c",
                                 f'cd database && qlever start --name dbpedia --timeout {15 * 60}s --run-in-foreground | tee /data/logs/{dataset.name}-start-log.txt']
                                )


class Fuseki(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = "5.2.0"
        self.jena_dir = self.installation_dir.joinpath(f"apache-jena-{self.version}")
        self.fuseki_dir = self.installation_dir.joinpath(f"apache-jena-fuseki-{self.version}")
        self.sparql_endpoint = "http://localhost:3030/ds/sparql"
        self.update_endpoint = "http://localhost:3030/ds/update"

    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)  # intentionally throw if exists
        # for fuseki the database path must not exist
        db_dir.rmdir()

        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.loading_results_dir(db_version)
        log_dir.mkdir(parents=True, exist_ok=True)

        env_opts = os.environ.copy()
        env_opts['JAVA_OPTS'] = f'-Xms1g -Xmx{ram_limit_g}g'

        with open(log_dir.joinpath("loading.log"), "w") as f:
            r = subprocess.run([f"{self.jena_dir}/bin/tdb2.tdbloader",
                                "--loc", f"{db_dir}",
                                f"{dataset.dataset_path}", ],
                               stdout=f, stderr=subprocess.STDOUT,
                               env=env_opts)
            assert r.returncode == 0
        return db_version

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        env_opts = os.environ.copy()
        env_opts['JAVA_OPTS'] = f'-Xms1g -Xmx{ram_limit_g}g'

        return subprocess.Popen(["java", "-jar", "fuseki-server.jar",
                                 f"--loc={self.dataset_db_dir(db_version.dataset).absolute()}",
                                 "--update",
                                 "/ds"],
                                cwd=self.fuseki_dir,
                                env=env_opts)  # TODO: log


class Virtuoso(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sparql_endpoint = "http://localhost:8890/sparql"
        self.update_endpoint = "http://localhost:8890/sparql"

    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)
        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.loading_results_dir(db_version)
        log_dir.mkdir(parents=True, exist_ok=True)
        db_dir.joinpath("database").mkdir(parents=True, exist_ok=True)

        config_path = dataset.path.joinpath("virtuoso.ini")
        from string import Template
        template_path = self.installation_dir.parent.parent.parent.joinpath("virtuoso_template.ini")
        config_template = Template(template_path.read_text("utf-8"))
        substitutions = {
            "installation_dir": str(self.installation_dir.absolute()),
            "database_dir": str(db_dir.absolute()),
            "benchmarks_dir": str(dataset.path.absolute()),
            "thread_count": os.cpu_count(),
            "max_dirty_buffers": ram_limit_g * 62500,
            "number_of_buffers": ram_limit_g * 85000,
            "serve_log": str(log_dir.joinpath("serve.log")),  # should be fine
        }
        config_path.write_text(config_template.substitute(substitutions), "utf-8")

        p = subprocess.Popen(
            [f"{self.installation_dir.joinpath('bin').joinpath('virtuoso-t')}", "-c", f"{config_path}", "-w",
             "+foreground"])
        wait_until_available(self.sparql_endpoint)

        command = \
            f"""ld_dir ('{dataset.path.absolute()}', '*.nt', 'http://example.com');
rdf_loader_run();
GRANT SPARQL_UPDATE TO "SPARQL";
DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);
INSERT INTO DB.DBA.SYS_SPARQL_HOST (SH_HOST, SH_GRAPH_URI) VALUES ('localhost:8890', 'http://example.com');
checkpoint;
shutdown;"""

        subprocess.run([f"{self.installation_dir.joinpath('bin').joinpath('isql')}"], input=command, text=True)
        wait = p.wait(20 * 60)  # max 20 min
        if wait != 0:
            p.kill()
            raise RuntimeError("Virtuoso checkpoint and shutdown failed")

        return db_version

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        config_path = db_version.dataset.path.joinpath("virtuoso.ini")
        assert config_path.is_file()
        return subprocess.Popen([f"{self.installation_dir.joinpath('bin').joinpath('virtuoso-t')}",
                                 "-c", f"{config_path}",
                                 "-f",
                                 "+foreground"])


class GraphDB(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.sparql_endpoint = "http://localhost:7200/repositories/"
        bash(f"chmod 755 -R {self.installation_dir.absolute()}")

    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)
        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.loading_results_dir(db_version)
        log_dir.mkdir(parents=True, exist_ok=True)

        config_path = dataset.path.joinpath("graphdb.ttl")
        if not config_path.exists():
            from string import Template
            template_path = self.installation_dir.parent.parent.parent.joinpath("graphdb_template.ttl")
            config_template = Template(template_path.read_text("utf-8"))
            substitutions = {"dataset_name": dataset.name}
            config_path.write_text(config_template.substitute(substitutions), "utf-8")
        else:
            assert config_path.is_file()

        with open(log_dir.joinpath("loading.log"), "w") as f:

            env_opts = os.environ.copy()
            env_opts['GDB_HEAP_SIZE'] = f'{ram_limit_g}g'

            subprocess.run([f"{self.installation_dir.joinpath('bin').joinpath('importrdf')}",
                            f"-Dgraphdb.home.data={self.dataset_db_dir(dataset).joinpath('data')}",
                            # TODO: check if these values make sense
                            f"-Dgraphdb.home.logs={log_dir.joinpath('logs')}",
                            f"-Dgraphdb.dist={self.installation_dir}",
                            "preload",
                            "-c", f"{config_path}",
                            "-f",
                            f"{dataset.dataset_path}"],
                           stdout=f, stderr=subprocess.STDOUT,
                           env=env_opts)

        return db_version

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        self.sparql_endpoint = f"http://localhost:7200/repositories/{db_version.dataset.name}"
        self.update_endpoint = f"http://localhost:7200/repositories/{db_version.dataset.name}/statements"

        env_opts = os.environ.copy()
        env_opts['GDB_HEAP_SIZE'] = f'{ram_limit_g}g'

        return subprocess.Popen([f"{self.installation_dir.joinpath('bin').joinpath('graphdb')}",
                                 f"-Dgraphdb.home.data={self.dataset_db_dir(db_version.dataset).joinpath('data')}",
                                 f"-Dgraphdb.home.logs={self.loading_results_dir(db_version).joinpath('logs')}",
                                 f"-Dgraphdb.dist={self.installation_dir}",
                                 "-s"],
                                env=env_opts)
