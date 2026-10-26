"""Database creation and reference-data loading."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path


def _sql(name: str) -> str:
    return files("factorysim").joinpath("sql", name).read_text(encoding="utf-8")


def create_database(
    path: str | Path = ":memory:", *, overwrite: bool = False
) -> sqlite3.Connection:
    """Create a fresh database with the schema and seed data applied."""
    if str(path) != ":memory:":
        path = Path(path)
        if path.exists():
            if not overwrite:
                raise FileExistsError(f"{path} already exists (pass overwrite=True to replace it)")
            path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_sql("schema.sql"))
    conn.executescript(_sql("seed.sql"))
    validate_reference_data(conn)
    return conn


@dataclass(frozen=True)
class Material:
    material_id: int
    name: str
    unit_cost: float
    opening_stock: int
    lead_time_days: int
    reorder_point: int
    order_quantity: int


@dataclass(frozen=True)
class Product:
    product_id: int
    sku: str
    family: str
    grade: str
    unit_price: float
    base_yield: float


@dataclass(frozen=True)
class Station:
    station_id: int
    name: str
    material_id: int
    daily_input_units: int
    family: str


@dataclass(frozen=True)
class ReferenceData:
    materials: dict[int, Material]
    products: dict[int, Product]
    stations: list[Station]

    def family_products(self, family: str) -> list[Product]:
        return [p for p in self.products.values() if p.family == family]

    def grade_product(self, family: str, grade: str) -> Product:
        matches = [p for p in self.family_products(family) if p.grade == grade]
        if len(matches) != 1:
            raise ValueError(
                f"expected one {grade!r} product in family {family!r}, found {len(matches)}"
            )
        return matches[0]


def load_reference_data(conn: sqlite3.Connection) -> ReferenceData:
    materials = {
        row[0]: Material(*row)
        for row in conn.execute(
            """SELECT material_id, material_name, unit_cost, current_stock,
                      lead_time_days, reorder_point, order_quantity
               FROM raw_materials ORDER BY material_id"""
        )
    }
    products = {
        row[0]: Product(*row)
        for row in conn.execute(
            """SELECT product_id, sku, product_family, quality_grade, unit_price, base_yield
               FROM products ORDER BY product_id"""
        )
    }
    stations = [
        Station(*row)
        for row in conn.execute(
            """SELECT station_id, station_name, input_material_id, daily_input_units, product_family
               FROM production_stations ORDER BY station_id"""
        )
    ]
    return ReferenceData(materials, products, stations)


def validate_reference_data(conn: sqlite3.Connection) -> None:
    """Fail fast on reference data the simulator cannot interpret."""
    ref = load_reference_data(conn)
    for family in {s.family for s in ref.stations}:
        products = ref.family_products(family)
        if not products:
            raise ValueError(f"stations produce family {family!r} but it has no products")
        total = sum(p.base_yield for p in products)
        if abs(total - 1.0) > 1e-9:
            raise ValueError(f"base_yield for family {family!r} sums to {total}, expected 1")
        ref.grade_product(family, "Scrap")
