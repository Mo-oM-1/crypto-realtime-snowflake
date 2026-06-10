"""Rend le module du consumer importable quel que soit le cwd (pytest)."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
