"""
Utility functions for generic disk writing / reading operations.

This module provides helper methods used across projects.

Author: ALDE / DEAO
Created: 2026-01-15
"""

import os
import chardet
import shutil
from pathlib import Path


def hello(name: str) -> str:
    return f"Hello, {name}!"

# -------------------------  DIRECTORY  ------------------------------------
def ensure_dir_exists(dir_path: str | os.PathLike) -> None:
    """
    Ensure that the directory path exists.
    If the directory (or any of its parents) does not exist, it is created.

    Parameters
        dir_path : str | os.PathLike
            Path to a directory should be ensured to exist.
    """
    dir_path = Path(dir_path)
    if not dir_path.is_dir():
        dir_path.mkdir(parents=True, exist_ok=True)

def ensure_file_dir_exists(file_path: str | os.PathLike) -> None:
    """
    Ensure that the parent directory of the given file path exists.
    If the directory (or any of its parents) does not exist, it is created.

    Parameters
        file_path : str | os.PathLike
            Path to a file whose parent directory should be ensured to exist.
    """
    dir_path = Path(file_path).parent
    ensure_dir_exists(dir_path)

def is_dir_empty(dir_path: str) -> bool:
    """Return True if the directory is empty, False otherwise."""
    return len(os.listdir(dir_path)) == 0

# def empty_dir(folder_path):
#     folder = Path(folder_path)

#     if not folder.exists():
#         raise FileNotFoundError(f"Folder does not exist: {folder}")

#     if not folder.is_dir():
#         raise NotADirectoryError(f"Not a folder: {folder}")

#     for item in folder.iterdir():
#         if item.is_dir():
#             shutil.rmtree(item)
#         else:
#             item.unlink()

def empty_dir(dir_path: str | os.PathLike, missing_ok: bool = False) -> None:
    """
    Remove all contents of a directory without deleting the directory itself.

    Parameters
    ----------
    dir_path : str | os.PathLike
        Path to the directory to empty.
    missing_ok : bool, default=False
        If True, do nothing when the directory does not exist.
        If False, raise FileNotFoundError.

    Raises
    ------
    FileNotFoundError
        If the directory does not exist and `missing_ok` is False.
    NotADirectoryError
        If `dir_path` exists but is not a directory.
    """
    path = Path(dir_path)

    if not path.exists():
        if missing_ok:
            return
        raise FileNotFoundError(f"Directory does not exist: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")

    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()

# -------------------------  FILE  ------------------------------------
def file_exists(file_path: str | os.PathLike) -> bool:
    """
    Check whether a file exists at the given path.

    Parameters
        file_path : str | os.PathLike
            Path to the file to check.

    Returns
        bool
            True if the file exists and is a regular file, otherwise False.
    """
    return Path(file_path).is_file()


def detect_file_encoding(file_path):
    """Détecte l'encodage d'un fichier"""
    with open(file_path, 'rb') as f:
        raw_data = f.read(1024 * 100)  # Premier 100KB suffisent
        result = chardet.detect(raw_data)
    return result['encoding']

