#!/usr/bin/env python3

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from create_company_material_flows_db import DEFAULT_DB_PATH, create_database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import company material flow JSON files into a SQLite database."
    )
    parser.add_argument(
        "json_dir",
        help="Directory containing pipeline JSON outputs.",
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help=f"SQLite database file to import into (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--glob",
        default="*.json",
        help="Filename glob used to select input files (default: *.json)",
    )
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path} does not contain a top-level JSON object")
    return data


def require_object(data: dict[str, Any], key: str, source: Path) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{source}: expected '{key}' to be an object")
    return value


def require_array(data: dict[str, Any], key: str, source: Path) -> list[dict[str, Any]]:
    value = data.get(key)
    if not isinstance(value, list):
        raise ValueError(f"{source}: expected '{key}' to be an array")
    if not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{source}: expected '{key}' items to be objects")
    return value


def upsert_company(
    connection: sqlite3.Connection,
    company_profile: dict[str, Any],
    source_file: Path,
) -> int:
    coordinates = company_profile.get("coordinates") or {}
    payload = (
        company_profile["company_name"],
        company_profile.get("street"),
        company_profile.get("postal_code"),
        company_profile.get("city"),
        company_profile.get("country"),
        company_profile.get("website"),
        coordinates.get("latitude"),
        coordinates.get("longitude"),
        source_file.name,
    )
    connection.execute(
        """
        INSERT INTO companies (
            company_name,
            street,
            postal_code,
            city,
            country,
            website,
            latitude,
            longitude,
            source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(company_name, website) DO UPDATE SET
            street = excluded.street,
            postal_code = excluded.postal_code,
            city = excluded.city,
            country = excluded.country,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            source_file = excluded.source_file
        """,
        payload,
    )
    row = connection.execute(
        """
        SELECT company_id
        FROM companies
        WHERE company_name = ? AND website = ?
        """,
        (company_profile["company_name"], company_profile.get("website")),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to resolve company_id for {source_file}")
    return int(row[0])


def replace_company_children(connection: sqlite3.Connection, company_id: int) -> None:
    connection.execute(
        "DELETE FROM products_and_services WHERE company_id = ?",
        (company_id,),
    )
    connection.execute(
        "DELETE FROM material_outputs WHERE company_id = ?",
        (company_id,),
    )


def insert_products(
    connection: sqlite3.Connection,
    company_id: int,
    products: list[dict[str, Any]],
) -> dict[str, int]:
    product_ids: dict[str, int] = {}
    for index, product in enumerate(products, start=1):
        cursor = connection.execute(
            """
            INSERT INTO products_and_services (
                company_id,
                name,
                category,
                volume_estimate,
                product_order
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                company_id,
                product["name"],
                product["category"],
                product.get("volume_estimate"),
                index,
            ),
        )
        product_ids[product["name"]] = int(cursor.lastrowid)
    return product_ids


def insert_material_outputs(
    connection: sqlite3.Connection,
    company_id: int,
    material_outputs: list[dict[str, Any]],
    product_ids: dict[str, int],
    source_file: Path,
) -> None:
    for item in material_outputs:
        cursor = connection.execute(
            """
            INSERT INTO material_outputs (
                company_id,
                rank,
                material,
                output_kind,
                name,
                form,
                treatment,
                grade,
                condition,
                size_dimensions,
                volume_estimate,
                notes,
                output_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                company_id,
                item["rank"],
                item["material"],
                item["output_kind"],
                item["name"],
                item["form"],
                item["treatment"],
                item["grade"],
                item["condition"],
                item.get("size_dimensions"),
                item.get("volume_estimate"),
                item.get("notes"),
                item.get("output_type"),
            ),
        )
        material_output_id = int(cursor.lastrowid)
        for product_name in item.get("produced_by_products", []):
            product_id = product_ids.get(product_name)
            if product_id is None:
                raise ValueError(
                    f"{source_file}: unknown product reference '{product_name}' "
                    f"for material output rank {item['rank']}"
                )
            connection.execute(
                """
                INSERT INTO material_output_products (
                    material_output_id,
                    product_id
                ) VALUES (?, ?)
                """,
                (material_output_id, product_id),
            )


def import_file(connection: sqlite3.Connection, path: Path) -> None:
    data = read_json(path)
    company_profile = require_object(data, "company_profile", path)
    products = require_array(data, "products_and_services", path)
    material_outputs = require_array(data, "material_outputs", path)

    company_id = upsert_company(connection, company_profile, path)
    replace_company_children(connection, company_id)
    product_ids = insert_products(connection, company_id, products)
    insert_material_outputs(connection, company_id, material_outputs, product_ids, path)


def main() -> None:
    args = parse_args()
    json_dir = Path(args.json_dir).expanduser().resolve()
    db_path = Path(args.db).expanduser().resolve()

    if not json_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {json_dir}")

    create_database(db_path)
    json_files = sorted(json_dir.glob(args.glob))
    if not json_files:
        raise SystemExit(f"No files matching '{args.glob}' found in {json_dir}")

    imported_count = 0
    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        for json_file in json_files:
            import_file(connection, json_file)
            imported_count += 1
        connection.commit()

    print(f"Imported {imported_count} files into {db_path}")


if __name__ == "__main__":
    main()
