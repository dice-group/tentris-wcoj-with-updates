import sys
from enum import Enum
from pathlib import Path
from subprocess import CompletedProcess

import requests
from tqdm import tqdm
import hashlib

import subprocess


def download_file(url: str, dest: Path, checksum: int = None, checksum_type: str = "sha1"):
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url} to {dest}...")

    response = requests.get(url, stream=True)
    if response.status_code != 200:
        print(f"Failed to download {url}, status code {response.status_code}")
        raise requests.exceptions.HTTPError

    if not checksum is None:
        if checksum_type == "sha1":
            hasher = hashlib.sha1()
        if checksum_type == "sha512":
            hasher = hashlib.sha512()

    total_length = int(response.headers.get("content-length"))
    with tqdm(total=total_length, unit='B', unit_scale=True) as pbar:
        with open(dest, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    pbar.update(len(chunk))
                    file.write(chunk)
                    file.flush()
                    if checksum is not None:
                        hasher.update(chunk)

    if checksum is not None:
        if checksum != int(hasher.hexdigest(), 16):
            print(f"Checksum mismatch.")
            raise RuntimeError


def bash(cmd) -> str:
    import subprocess
    result: CompletedProcess[str] = subprocess.run(cmd, shell=True,
                                                   executable="/bin/bash",
                                                   capture_output=True,
                                                   text=True)
    print(result.stderr, file=sys.stderr)
    status: int = result.returncode
    if result.returncode != 0:
        print(result.stdout)
        raise RuntimeError(f"Command '{cmd}' failed with status {status}")
    assert type(result.stdout) is str
    return result.stdout


def hash_file(file: Path, hash_type: str = "sha1") -> str:
    if hash_type == "sha1":
        hasher = hashlib.sha1()
    if hash_type == "sha512":
        hasher = hashlib.sha512()

    with open(file, 'rb', buffering=0) as f:
        return hashlib.file_digest(f, hash_type).hexdigest()


class CompressionAlgorithm(Enum):
    ZSTD = 1
    ZIP = 2
    BZIP2 = 3


def extract_file(source: Path, dest: Path, algorithm: CompressionAlgorithm, keep_source=True, overwrite=False) -> None:
    if not source.is_file():
        raise FileNotFoundError

    if dest.exists():
        if overwrite:
            dest.unlink()
        else:
            raise FileExistsError

    if algorithm == CompressionAlgorithm.ZSTD:
        with open(source, "rb") as compressed_file:
            with open(dest, "wb") as uncompressed_file:
                import zstandard as zstd
                dctx = zstd.ZstdDecompressor()
                for chunk in dctx.read_to_iter(compressed_file):
                    uncompressed_file.write(chunk)

    if algorithm == CompressionAlgorithm.ZIP:
        import zipfile
        import os
        with zipfile.ZipFile(source, "r") as z:
            filename = z.namelist()[0]
            z.extract(filename)
            os.rename(filename, dest)

    if algorithm == CompressionAlgorithm.BZIP2:
        with open(dest, "wb") as f:
            subprocess.run(["pbzip2", "-dvc", source], stdout=f)

    if not keep_source:
        source.unlink(missing_ok=True)


def wait_until_available(url: str, timeout: int = sys.maxsize):
    import time
    import requests
    tried = 0
    # wait for triplestore to start
    while tried < timeout:
        try:
            requests.get(url, timeout=5)
        except Exception as e:
            tried += 5
            time.sleep(5)
            continue
        return
    raise TimeoutError(f"Timed out waiting for {url}.")


def download_and_extract(url: str, dest: Path, checksum: int, compression_algorithm: CompressionAlgorithm,
                         overwrite: bool = False, checksum_type: str = "sha512") -> None:
    if dest.exists() and not overwrite:
        print("File already exists, skipping.")
        return
    temp_file = dest.parent.joinpath("temp")
    download_file(url, dest=temp_file, checksum=checksum, checksum_type=checksum_type)
    extract_file(temp_file, dest, compression_algorithm, keep_source=False)
