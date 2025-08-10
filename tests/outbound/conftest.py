# tests/outbound/conftest.py
import pytest
import sys
import types


@pytest.fixture(scope='function', autouse=True)
def patch_air_util(monkeypatch):
    # Create a dummy 'air.util' module with a fake 'create_dbutils' function
    fake_air_util = types.ModuleType("air.util")
    fake_air_util.create_dbutils = lambda: None
    # Use monkeypatch to temporarily replace the module
    monkeypatch.setitem(sys.modules, "air.util", fake_air_util)
