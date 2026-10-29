from __future__ import annotations

import bz2
import gzip
import io
import json
import lzma
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterator, TextIO

from .model import SourceIssue


@dataclass(slots=True)
class EventRecord:
    sequence: int
    source: str
    line_number: int
    event: dict[str, Any]


class EventReader:
    """Stream JSON events from plain, compressed, zipped, or rolling logs."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.issues: list[SourceIssue] = []
        self.complete = not self.path.name.endswith(".inprogress")

    def __iter__(self) -> Iterator[EventRecord]:
        sequence = 0
        for source_name, stream in self._text_streams():
            try:
                for line_number, line in enumerate(stream, 1):
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        self.complete = False
                        self.issues.append(SourceIssue(source_name, line_number, str(exc)))
                        continue
                    if not isinstance(event, dict):
                        self.issues.append(SourceIssue(source_name, line_number, "Event is not a JSON object"))
                        continue
                    sequence += 1
                    yield EventRecord(sequence, source_name, line_number, event)
            finally:
                stream.close()

    def _source_files(self) -> list[Path]:
        if self.path.is_file():
            return [self.path]
        if not self.path.is_dir():
            raise FileNotFoundError(self.path)
        files = [
            item
            for item in self.path.rglob("*")
            if item.is_file()
            and not item.name.startswith((".", "appstatus"))
            and not item.name.endswith(".crc")
        ]
        event_parts = [item for item in files if item.name.startswith("events_")]
        return sorted(event_parts or files, key=lambda item: item.as_posix())

    def _text_streams(self) -> Iterator[tuple[str, TextIO]]:
        for path in self._source_files():
            if zipfile.is_zipfile(path):
                archive = zipfile.ZipFile(path)
                names = sorted(info.filename for info in archive.infolist() if not info.is_dir())
                for name in names:
                    raw = archive.open(name)
                    yield f"{path}!{name}", io.TextIOWrapper(raw, encoding="utf-8")
                archive.close()
                continue
            yield str(path), self._open_text(path)

    @staticmethod
    def _open_text(path: Path) -> TextIO:
        with path.open("rb") as probe:
            magic = probe.read(10)
        suffix = path.suffix.lower()
        if magic.startswith(b"\x1f\x8b") or suffix == ".gz":
            return gzip.open(path, "rt", encoding="utf-8")
        if magic.startswith(b"BZh") or suffix == ".bz2":
            return bz2.open(path, "rt", encoding="utf-8")
        if magic.startswith(b"\xfd7zXZ") or suffix in {".xz", ".lzma"}:
            return lzma.open(path, "rt", encoding="utf-8")
        if magic.startswith(b"\x04\x22\x4d\x18") or suffix == ".lz4":
            try:
                import lz4.frame  # type: ignore
            except ImportError as exc:
                raise RuntimeError("LZ4 log requires the 'lz4' optional dependency") from exc
            return io.TextIOWrapper(lz4.frame.open(path, "rb"), encoding="utf-8")
        if magic.startswith(b"\x28\xb5\x2f\xfd") or suffix in {".zst", ".zstd"}:
            try:
                import zstandard  # type: ignore
            except ImportError as exc:
                raise RuntimeError("Zstandard log requires the 'zstandard' optional dependency") from exc
            raw: BinaryIO = path.open("rb")
            return io.TextIOWrapper(zstandard.ZstdDecompressor().stream_reader(raw), encoding="utf-8")
        if magic.startswith(b"\xff\x06\x00\x00sNaPpY") or suffix in {".snappy", ".sz"}:
            try:
                import snappy  # type: ignore
            except ImportError as exc:
                raise RuntimeError("Snappy log requires the 'python-snappy' optional dependency") from exc
            data = snappy.StreamDecompressor().decompress(path.read_bytes())
            return io.TextIOWrapper(io.BytesIO(data), encoding="utf-8")
        return path.open("rt", encoding="utf-8")
