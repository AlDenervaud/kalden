"""Utilities for reading, validating, and rewriting MIKE dfs0 time series.

This module wraps the current public ``mikeio`` dfs0 workflow:

* ``mikeio.open()`` for lightweight file/header access
* ``mikeio.read()`` for loading a dfs0 file into a ``Dataset``
* ``Dataset.to_dataframe(..., round_time=False)`` and ``mikeio.from_pandas(...)``
  when rebuilding a file with explicit timestamps and preserved item metadata
* ``Dataset.to_dfs()`` for writing

The implementation intentionally avoids the old script-style pattern of hard-coded
paths and top-level execution.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
import os
from os import PathLike as OsPathLike
from pathlib import Path
import tempfile

import mikeio

PathLike = str | OsPathLike[str]

__all__ = ["Dfs0"]


class Dfs0:
    """Convenience wrapper for common dfs0 file operations.

    Parameters
    ----------
    path : str | os.PathLike | None, optional
        Path to a dfs0 file. Methods that operate on a single file use this
        stored path when no explicit ``path`` argument is supplied.
    """

    def __init__(self, path: PathLike | None = None) -> None:
        self.path = None if path is None else self._as_path(path)

    @staticmethod
    def _as_path(path: PathLike) -> Path:
        """Return a normalized ``Path`` instance."""
        return Path(path).expanduser()

    @classmethod
    def _resolve_source(cls, path: PathLike | None, default: Path | None) -> Path:
        """Resolve and validate the source dfs0 path."""
        source = default if path is None else cls._as_path(path)
        if source is None:
            raise ValueError("A dfs0 file path must be provided.")
        if source.suffix.lower() != ".dfs0":
            raise ValueError(f"Expected a .dfs0 file, got: {source}")
        if not source.is_file():
            raise FileNotFoundError(f"dfs0 file not found: {source}")
        return source

    @staticmethod
    def _resolve_destination(
        source: Path,
        destination: PathLike | None,
        overwrite: bool,
    ) -> Path:
        """Resolve the output path and guard against accidental overwrites."""
        target = source if destination is None else Path(destination).expanduser()
        if target.suffix.lower() != ".dfs0":
            raise ValueError(f"Expected a .dfs0 file, got: {target}")
        if target.exists() and target != source and not overwrite:
            raise FileExistsError(f"Output file already exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    @staticmethod
    def _validate_item_count(dataset, items: Sequence[object] | None) -> Sequence[object]:
        """Ensure replacement items match the number of data items."""
        if items is None:
            return list(dataset.items)
        if len(items) != dataset.n_items:
            raise ValueError(
                "Replacement items must match the number of dataset items "
                f"({dataset.n_items})."
            )
        return list(items)

    @classmethod
    def iter_files(cls, root: PathLike, *, recursive: bool = True) -> list[Path]:
        """Return all dfs0 files under ``root`` in deterministic order.

        Parameters
        ----------
        root : str | os.PathLike
            Either a single dfs0 file or a directory containing dfs0 files.
        recursive : bool, default True
            If ``True``, search subdirectories recursively.
        """
        root_path = cls._as_path(root)
        if root_path.is_file():
            if root_path.suffix.lower() != ".dfs0":
                raise ValueError(f"Expected a .dfs0 file, got: {root_path}")
            return [root_path]
        if not root_path.is_dir():
            raise FileNotFoundError(f"Directory not found: {root_path}")

        pattern = "**/*.dfs0" if recursive else "*.dfs0"
        return sorted(path for path in root_path.glob(pattern) if path.is_file())

    @staticmethod
    def _is_excluded(path: Path, exclude_substrings: Iterable[str]) -> bool:
        """Return ``True`` when a path should be skipped."""
        path_text = str(path).lower()
        return any(token in path_text for token in exclude_substrings)

    def open(self, path: PathLike | None = None):
        """Open a dfs0 file with ``mikeio.open`` and return the header object."""
        source = self._resolve_source(path, self.path)
        return mikeio.open(source)

    def read(
        self,
        path: PathLike | None = None,
        *,
        items=None,
        time=None,
        keepdims: bool = False,
    ):
        """Read a dfs0 file into a ``mikeio.Dataset``."""
        source = self._resolve_source(path, self.path)
        return mikeio.read(source, items=items, time=time, keepdims=keepdims)

    def to_dataframe(
        self,
        path: PathLike | None = None,
        *,
        unit_in_name: bool = False,
        round_time: str | bool = "ms",
    ):
        """Read a dfs0 file and return a pandas ``DataFrame``."""
        dataset = self.read(path)
        return dataset.to_dataframe(unit_in_name=unit_in_name, round_time=round_time)

    def duplicate_timestamps(self, path: PathLike | None = None):
        """Return duplicated timestamps from a dfs0 file.

        The result is a ``pandas.DatetimeIndex`` containing every duplicated
        timestamp occurrence (``keep=False``).
        """
        dataset = self.read(path)
        return dataset.time[dataset.time.duplicated(keep=False)]

    def validate_timestamps(
        self,
        path: PathLike | None = None,
        *,
        require_sorted: bool = True,
        require_unique: bool = True,
    ):
        """Validate timestamp ordering and uniqueness.

        Returns
        -------
        mikeio.Dataset
            The loaded dataset, so validation and downstream work can share the
            same read operation.
        """
        source = self._resolve_source(path, self.path)
        dataset = self.read(source)

        if dataset.n_timesteps == 0:
            raise ValueError(f"dfs0 file has no time steps: {source}")
        if require_sorted and not dataset.time.is_monotonic_increasing:
            raise ValueError(f"Timestamps are not sorted in ascending order: {source}")
        if require_unique and dataset.time.has_duplicates:
            duplicates = dataset.time[dataset.time.duplicated(keep=False)]
            raise ValueError(
                f"Duplicate timestamps found in {source}: {duplicates.unique().tolist()}"
            )

        return dataset

    def rewrite(
        self,
        destination: PathLike | None = None,
        *,
        overwrite: bool = False,
        items: Sequence[object] | None = None,
        title: str | None = None,
        validate_timestamps: bool = True,
        **kwargs,
    ) -> Path:
        """Rewrite a dfs0 file using current public ``mikeio`` APIs.

        The file is read to a ``Dataset``, converted to a ``DataFrame`` without
        timestamp rounding, recreated with ``mikeio.from_pandas(...)`` to keep
        item metadata, and written back with ``Dataset.to_dfs()``.

        Parameters
        ----------
        destination : str | os.PathLike | None, optional
            Output path. If omitted, the source file is overwritten.
        overwrite : bool, default False
            Allow overwriting an existing output file when ``destination`` is a
            different path than the source.
        items : sequence, optional
            Replacement ``mikeio.ItemInfo`` sequence. If omitted, the original
            item metadata is preserved.
        title : str | None, optional
            Optional dfs title. Defaults to the source filename stem.
        validate_timestamps : bool, default True
            Validate timestamp ordering and uniqueness before writing.
        **kwargs
            Additional keyword arguments forwarded to ``Dataset.to_dfs()``.
        """
        source = self._resolve_source(None, self.path)
        target = self._resolve_destination(source, destination, overwrite)
        dataset = (
            self.validate_timestamps(source) if validate_timestamps else self.read(source)
        )

        output_items = self._validate_item_count(dataset, items)
        dataframe = dataset.to_dataframe(unit_in_name=False, round_time=False)
        rebuilt = mikeio.from_pandas(dataframe, items=output_items)
        rebuilt.to_dfs(target, title=source.stem if title is None else title, **kwargs)
        return target

    def convert_to_nonequidistant(
        self,
        destination: PathLike | None = None,
        *,
        overwrite: bool = False,
        items: Sequence[object] | None = None,
        title: str | None = None,
        validate_timestamps: bool = True,
        require_non_equidistant: bool = True,
        **kwargs,
    ) -> Path:
        """Rewrite a dfs0 file and verify that the output is non-equidistant.

        This method uses the same public ``mikeio`` dataset/DataFrame workflow as
        :meth:`rewrite`. After writing, it re-reads the output and can assert that
        the resulting time axis is non-equidistant.

        Parameters
        ----------
        require_non_equidistant : bool, default True
            If ``True``, raise an error when the rewritten file still has an
            equidistant time axis.
        """
        source = self._resolve_source(None, self.path)
        target = self._resolve_destination(source, destination, overwrite)
        target_for_write = target
        temp_target: Path | None = None

        if require_non_equidistant and target == source:
            handle, temp_name = tempfile.mkstemp(
                prefix=f"{source.stem}_",
                suffix=".dfs0",
                dir=source.parent,
            )
            os.close(handle)
            Path(temp_name).unlink(missing_ok=True)
            temp_target = Path(temp_name)

        if temp_target is not None:
            target_for_write = temp_target

        try:
            rewritten_target = self.rewrite(
                destination=target_for_write,
                overwrite=True,
                items=items,
                title=title,
                validate_timestamps=validate_timestamps,
                **kwargs,
            )

            if require_non_equidistant:
                written = mikeio.read(rewritten_target)
                if written.is_equidistant:
                    raise RuntimeError(
                        "The rewritten file is still equidistant. The current public "
                        "mikeio write path preserved the regular time axis."
                    )

            if temp_target is not None:
                temp_target.replace(source)
                return source

            return rewritten_target
        finally:
            if temp_target is not None and temp_target.exists():
                temp_target.unlink(missing_ok=True)

    @classmethod
    def batch_convert_to_nonequidistant(
        cls,
        root: PathLike,
        *,
        recursive: bool = True,
        overwrite: bool = False,
        exclude_substrings: Iterable[str] | None = None,
        require_non_equidistant: bool = True,
        **kwargs,
    ) -> list[Path]:
        """Convert every eligible dfs0 file below ``root``.

        Parameters
        ----------
        root : str | os.PathLike
            A dfs0 file or a directory containing dfs0 files.
        recursive : bool, default True
            Search subdirectories when ``root`` is a directory.
        overwrite : bool, default False
            Allow overwriting destination files when ``destination`` is supplied
            via ``kwargs`` for individual calls.
        exclude_substrings : iterable of str, optional
            Case-insensitive substrings used to skip matching paths.
        require_non_equidistant : bool, default True
            Propagate the non-equidistant verification check.
        **kwargs
            Additional keyword arguments forwarded to
            :meth:`convert_to_nonequidistant`.
        """
        exclusions = tuple(token.lower() for token in (exclude_substrings or ()))
        converted: list[Path] = []

        for path in cls.iter_files(root, recursive=recursive):
            if exclusions and cls._is_excluded(path, exclusions):
                continue
            reader = cls(path)
            converted.append(
                reader.convert_to_nonequidistant(
                    overwrite=overwrite,
                    require_non_equidistant=require_non_equidistant,
                    **kwargs,
                )
            )

        return converted

    @classmethod
    def scan_duplicate_timestamps(
        cls,
        root: PathLike,
        *,
        recursive: bool = True,
        exclude_substrings: Iterable[str] | None = None,
    ) -> dict[Path, list]:
        """Scan one file or a directory tree for duplicate timestamps."""
        exclusions = tuple(token.lower() for token in (exclude_substrings or ()))
        findings: dict[Path, list] = {}

        for path in cls.iter_files(root, recursive=recursive):
            if exclusions and cls._is_excluded(path, exclusions):
                continue
            duplicates = cls(path).duplicate_timestamps()
            if len(duplicates) > 0:
                findings[path] = duplicates.tolist()

        return findings
