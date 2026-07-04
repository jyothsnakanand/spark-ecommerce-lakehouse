"""
conftest.py — shared pytest fixtures.

A session-scoped SparkSession so all tests share one JVM (starting Spark per
test would be painfully slow).
"""

import sys, os
import pytest

sys.path.append(os.path.dirname(__file__))          # _spark.py is local to this folder
from _spark import get_spark

# tests read the built tables relative to the repo root (two levels up)
os.chdir(os.path.join(os.path.dirname(__file__), "..", ".."))


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests")
    yield s
    s.stop()
