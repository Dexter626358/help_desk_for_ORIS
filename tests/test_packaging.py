"""Проверка, что wheel содержит шаблоны/схемы и entrypoint резолвится."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_wheel_contains_package_data(tmp_path):
    dist = tmp_path / "dist"
    subprocess.run(
        [sys.executable, "-m", "pip", "wheel", ".", "-w", str(dist), "--no-deps"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(dist.glob("ipsas-*.whl"))
    assert wheels, "wheel не собран"
    with zipfile.ZipFile(wheels[0], "r") as zf:
        names = zf.namelist()
    assert any(
        n.endswith("ipsas/web/templates/dashboard.html")
        or "templates/dashboard.html" in n
        for n in names
    )
    assert any("journal3.xsd" in n for n in names)
    assert any(n.endswith("ipsas/web/wsgi.py") for n in names)


def test_devserver_entrypoint_importable():
    spec = importlib.util.find_spec("ipsas.web.devserver")
    assert spec is not None
    from ipsas.web import wsgi as wsgi_mod

    assert hasattr(wsgi_mod, "app")
