import gzip
import zipfile
from pathlib import Path
from typing import Iterator, Tuple


def iter_zip_member_bytes(zip_path: Path) -> Iterator[Tuple[str, bytes]]:
    """Yield (member filename, member bytes) for non-directory entries in a zip archive."""
    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue

            member_name = Path(member.filename).name
            if not member_name:
                continue

            yield member_name, archive.read(member.filename)


def write_gzip_file(content: bytes, target_path: Path) -> Path:
    """Write bytes to a gzip file, creating parent directories when needed."""
    target_path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(target_path, "wb") as gz_file:
        gz_file.write(content)

    return target_path
