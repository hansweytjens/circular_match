#!/usr/bin/env python3

import argparse
import sqlite3
from pathlib import Path

from output_paths import DEFAULT_DB_PATH


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS companies (
    company_id INTEGER PRIMARY KEY,
    company_name TEXT NOT NULL,
    street TEXT,
    postal_code TEXT,
    city TEXT,
    country TEXT,
    website TEXT,
    latitude REAL,
    longitude REAL,
    source_file TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_name, website)
);

CREATE TABLE IF NOT EXISTS products_and_services (
    product_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('product', 'service')),
    volume_estimate TEXT CHECK (volume_estimate IN ('large', 'medium', 'low')),
    product_order INTEGER,
    FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE CASCADE,
    UNIQUE(company_id, name)
);

CREATE TABLE IF NOT EXISTS material_outputs (
    material_output_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    material TEXT NOT NULL,
    output_kind TEXT NOT NULL,
    name TEXT NOT NULL,
    form TEXT NOT NULL,
    treatment TEXT NOT NULL,
    grade TEXT NOT NULL,
    condition TEXT NOT NULL,
    size_dimensions TEXT,
    volume_estimate TEXT CHECK (volume_estimate IN ('large', 'medium', 'low')),
    notes TEXT,
    output_type TEXT,
    FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE CASCADE,
    UNIQUE(company_id, rank)
);

CREATE TABLE IF NOT EXISTS material_output_products (
    material_output_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    PRIMARY KEY (material_output_id, product_id),
    FOREIGN KEY (material_output_id) REFERENCES material_outputs(material_output_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products_and_services(product_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_products_company_id
ON products_and_services(company_id);

CREATE INDEX IF NOT EXISTS idx_material_outputs_company_id
ON material_outputs(company_id);

CREATE INDEX IF NOT EXISTS idx_material_outputs_material
ON material_outputs(material);

CREATE INDEX IF NOT EXISTS idx_material_output_products_product_id
ON material_output_products(product_id);

CREATE TRIGGER IF NOT EXISTS companies_set_updated_at
AFTER UPDATE ON companies
FOR EACH ROW
BEGIN
    UPDATE companies
    SET updated_at = CURRENT_TIMESTAMP
    WHERE company_id = NEW.company_id;
END;
"""


def create_database(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as connection:
        connection.executescript(SCHEMA_SQL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create the SQLite database schema for company material flows."
    )
    parser.add_argument(
        "db_path",
        nargs="?",
        default=str(DEFAULT_DB_PATH),
        help=f"SQLite database file to create (default: {DEFAULT_DB_PATH})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    create_database(db_path)
    print(f"Created or updated schema at {db_path}")


if __name__ == "__main__":
    main()
