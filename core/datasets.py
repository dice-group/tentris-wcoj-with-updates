from pathlib import Path
import zipfile

from queries import WikidataChangesetQueries, DBpediaChangesetQueries, BSBMChangesetQueries
from core.util import bash, hash_file


class Dataset:
    def __init__(self, name, benchmarks_dir: Path):
        self.name = name
        self.path = benchmarks_dir.joinpath(name)
        self.path.mkdir(parents=True, exist_ok=True)
        self.dataset_path: Path = self.path.joinpath("dataset.nt")
        self.warmup_queries: Path = self.path.joinpath("warmup_queries.txt")

    def download(self) -> None:
        pass


class SWDF(Dataset):
    def __init__(self, directory: Path):
        super().__init__("swdf", directory)

    def download(self):
        # warmup queries
        warmup_queriesurl = "https://raw.githubusercontent.com/dice-group/iswc2020_tentris/master/queries/SWDF-Queries.txt"
        bash(f"curl -L '{warmup_queriesurl}' > '{self.warmup_queries.absolute()}'")
        assert self.warmup_queries.exists()
        warmup_queriesurl_sha1 = 'e8c4d295d29f36f11b0b77a1ea83e13ff7333488'
        assert warmup_queriesurl_sha1 == hash_file(self.warmup_queries, "sha1")

        # use bash to download and decompress the dataset
        dataset_url = "https://files.dice-research.org/datasets/ISWC2020_Tentris/swdf.zip"
        from urllib.request import urlopen
        from io import BytesIO
        with urlopen(dataset_url) as zipresp:
            with zipfile.ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall()
        bash(f"mv swdf.nt {self.dataset_path.absolute()}")
        assert self.dataset_path.exists()

        # use as DBpedia changeset as dummy changeset
        WikidataChangesetQueries(self.path, 10).download()


class DBpedia2015(Dataset):
    def __init__(self, directory: Path):
        super().__init__("dbpedia", directory)

    def download(self):
        # warmup queries
        warmup_queriesurl = "https://files.dice-research.org/projects/tentris_compression/feasible-DBpedia-bgp-v2.txt"
        bash(f"curl -L '{warmup_queriesurl}' > '{self.warmup_queries.absolute()}'")
        assert self.warmup_queries.exists()
        warmup_queries_sha1 = '10c397a57f4a7d3844194c214cfb2c26ab132d01'
        assert warmup_queries_sha1 == hash_file(self.warmup_queries, "sha1")

        # use bash to download and decompress the dataset
        dataset_url = "https://files.dice-research.org/datasets/ISWC2020_Tentris/dbpedia_2015-10_en_wo-comments_c.nt.zst"
        bash(f"curl -L '{dataset_url}' | zstd -d > '{self.dataset_path.absolute()}'")
        assert self.dataset_path.exists()

        DBpediaChangesetQueries(self.path).download()


class Wikidata(Dataset):
    def __init__(self, directory: Path):
        super().__init__("wikidata", directory)

    def download(self: Dataset):
        # dataset
        # warmup queries
        warmup_queries_url = "https://files.dice-research.org/projects/tentris_compression/feasible-exmp-wikidata500-bgp-v4.txt"
        bash(f"curl -L '{warmup_queries_url}' > '{self.warmup_queries.absolute()}'")
        assert self.warmup_queries.exists()
        warmup_queriesurl_sha1 = 'd881ea12c315669ff3ef1f8073ca553e3f9b2715'
        assert warmup_queriesurl_sha1 == hash_file(self.warmup_queries, "sha1")

        # use bash to download and decompress the dataset
        dataset_url = "https://files.dice-research.org/datasets/hypertrie_update/wikidata/wikidata-2020-11-11-truthy-BETA-without-preparation.nt.zst"
        bash(f"curl -L '{dataset_url}' | zstd -d > '{self.dataset_path.absolute()}'")
        assert self.dataset_path.exists()

        for batch_size in WikidataChangesetQueries.available_batch_sizes():
            WikidataChangesetQueries(self.path, batch_size).download()

class BSBM(Dataset):
    def __init__(self, directory: Path):
        super().__init__("bsbm", directory)

    def download(self):
        # warmup queries
        # empty file, there is no warmup queries for BSBM
        with open(self.warmup_queries.absolute(), 'w'):
            pass
        assert self.warmup_queries.exists()

        # use bash to download and decompress the dataset
        dataset_url = "https://files.dice-research.org/datasets/hypertrie_update/bsbm/data100m/dataset.nt.zst"
        bash(f"curl -L '{dataset_url}' | zstd -d > '{self.dataset_path.absolute()}'")
        assert self.dataset_path.exists()

        BSBMChangesetQueries(self.path).download()