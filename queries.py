from pathlib import Path

from core import util


class UpdateQuery:
    def __init__(self, name: str, base_dir: Path):
        self.name: str = name
        self.base_dir: Path = base_dir
        self.query_dir: Path = base_dir.joinpath(name)
        self.query_file: Path = self.query_dir.joinpath("update_queries.txt")

    def download(self):
        pass

    def delete(self):
        self.query_file.unlink()


class DBpediaChangesetQueries(UpdateQuery):
    def __init__(self, base_directory: Path):
        super().__init__("changesets", base_directory)

    def download(self):
        util.download_and_extract(
            "https://files.dice-research.org/datasets/hypertrie_update/dbpedia/dbpedia-2015-10-changelog-queries.zip",
            dest=self.query_file,
            checksum=0xeef169af710b93810c1bd3c6dc6023224b42dbc4bad58afe795e353a374fc9c934f54494bcb52490f9b68b75331a7188525b655221a4b1a66ea4070ae6b88565,
            compression_algorithm=util.CompressionAlgorithm.ZIP)


class WikidataChangesetQueries(UpdateQuery):
    def __init__(self, base_directory: Path, batch_size: int):
        super().__init__(f"changesets{batch_size}", base_directory)
        self.batch_size = batch_size

    def download(self):
        # TODO: hashsum
        file_name = f"wikidata-test-queries-{self.batch_size}.txt"
        util.download_file(
            f"https://files.dice-research.org/datasets/hypertrie_update/wikidata/wikidata-test-queries-{self.batch_size}.txt.tar.zst",
            dest=Path(f"{self.query_dir}/{file_name}.tar.zst"))
        util.bash(f"tar -xf {self.query_dir}/{file_name}.tar.zst -C {self.query_dir}")
        util.bash(f"mv {self.query_dir}/{file_name} {self.query_file}")
        self.query_dir.joinpath(f"{file_name}.tar.zst").unlink(missing_ok=True)

    @staticmethod
    def available_batch_sizes() -> list[int]:
        return [10, 100, 1000, 10000, 100000, 1000000]

class BSBMChangesetQueries(UpdateQuery):
    def __init__(self, base_directory: Path):
        super().__init__(f"changesets", base_directory)

    def download(self):
        query_file_url = "https://files.dice-research.org/datasets/hypertrie_update/bsbm/update_queries_bsbm100m.txt.zst"
        from core.util import bash
        bash(f"curl -L '{query_file_url}' | zstd -d > '{self.query_dir.absolute()}'")
        # TODO: hashsum
        assert self.query_dir.exists()

