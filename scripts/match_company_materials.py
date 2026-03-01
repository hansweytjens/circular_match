#!/usr/bin/env python3

import argparse
import csv
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from create_company_jsons import (
    DEFAULT_API_VERSION,
    DEFAULT_MODEL,
    call_gemini,
    load_api_key,
    normalize_model_name,
    parse_json_response,
    read_json_file,
    read_text_file,
    resolve_local_schema_refs,
    resolve_model_name,
)
from output_paths import DEFAULT_DB_PATH, MATERIAL_MATCHES_DIR


DEFAULT_CONTEXT_DIR = Path("/workspace/context/active")
DEFAULT_OUTPUT_DIR = MATERIAL_MATCHES_DIR


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Match company products/services to waste and by-product streams "
            "already stored in the company material flows SQLite database."
        )
    )
    parser.add_argument("--n", type=int, default=1, help="Number of CSV rows to process.")
    parser.add_argument(
        "--manufacturers",
        action="store_true",
        help="Only process companies with NACE-BEL Code 'C'.",
    )
    parser.add_argument(
        "--context-dir",
        type=Path,
        default=DEFAULT_CONTEXT_DIR,
        help="Directory containing the CSV and prompt/schema files.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Gemini model name (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--api-version",
        default=DEFAULT_API_VERSION,
        help=f"Gemini API version (default: {DEFAULT_API_VERSION})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for output JSON files.",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=150,
        help="Maximum number of candidate source outputs passed to the model per company.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call Gemini; write prompts and metadata only.",
    )
    return parser.parse_args()


def read_companies(csv_path: Path, n: int, manufacturers: bool) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            nace_code = (row.get("NACE-BEL Code") or "").strip()
            if manufacturers and nace_code != "C":
                continue
            rows.append(row)
            if len(rows) >= n:
                break
    return rows


def normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().casefold().split())


def fetch_db_company(
    connection: sqlite3.Connection,
    company_name: str,
    website: str,
) -> dict[str, Any] | None:
    normalized_name = normalize_text(company_name)
    normalized_website = normalize_text(website)
    rows = connection.execute(
        """
        SELECT
            company_id,
            company_name,
            website,
            street,
            postal_code,
            city,
            country,
            latitude,
            longitude
        FROM companies
        """
    ).fetchall()

    exact_both: dict[str, Any] | None = None
    exact_name: dict[str, Any] | None = None
    exact_website: dict[str, Any] | None = None

    for row in rows:
        candidate = {
            "company_id": row[0],
            "company_name": row[1],
            "website": row[2] or "",
            "street": row[3],
            "postal_code": row[4],
            "city": row[5],
            "country": row[6],
            "latitude": row[7],
            "longitude": row[8],
        }
        row_name = normalize_text(candidate["company_name"])
        row_website = normalize_text(candidate["website"])
        if normalized_name and normalized_website and row_name == normalized_name and row_website == normalized_website:
            exact_both = candidate
            break
        if normalized_name and row_name == normalized_name and exact_name is None:
            exact_name = candidate
        if normalized_website and row_website == normalized_website and exact_website is None:
            exact_website = candidate

    return exact_both or exact_name or exact_website


def fetch_products(connection: sqlite3.Connection, company_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT name, category, volume_estimate, product_order
        FROM products_and_services
        WHERE company_id = ?
        ORDER BY product_order, name
        """,
        (company_id,),
    ).fetchall()
    return [
        {
            "name": row[0],
            "category": row[1],
            "volume_estimate": row[2],
            "product_order": row[3],
        }
        for row in rows
    ]


def fetch_candidate_outputs(
    connection: sqlite3.Connection,
    target_company_id: int,
    limit: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            c.company_name,
            COALESCE(c.website, ''),
            m.material,
            m.output_kind,
            m.name,
            m.form,
            m.treatment,
            m.grade,
            m.condition,
            COALESCE(m.size_dimensions, ''),
            COALESCE(m.volume_estimate, ''),
            COALESCE(m.notes, ''),
            COALESCE(m.output_type, ''),
            GROUP_CONCAT(DISTINCT p.name)
        FROM material_outputs m
        JOIN companies c ON c.company_id = m.company_id
        LEFT JOIN material_output_products mop ON mop.material_output_id = m.material_output_id
        LEFT JOIN products_and_services p ON p.product_id = mop.product_id
        WHERE m.company_id != ?
        GROUP BY
            c.company_name,
            c.website,
            m.material_output_id,
            m.material,
            m.output_kind,
            m.name,
            m.form,
            m.treatment,
            m.grade,
            m.condition,
            m.size_dimensions,
            m.volume_estimate,
            m.notes,
            m.output_type
        ORDER BY
            CASE COALESCE(m.volume_estimate, '')
                WHEN 'large' THEN 1
                WHEN 'medium' THEN 2
                WHEN 'low' THEN 3
                ELSE 4
            END,
            m.rank,
            c.company_name,
            m.name
        LIMIT ?
        """,
        (target_company_id, limit),
    ).fetchall()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        produced_by = [item.strip() for item in (row[13] or "").split(",") if item and item.strip()]
        candidates.append(
            {
                "source_company": row[0],
                "source_website": row[1],
                "candidate_material": row[2],
                "output_kind": row[3],
                "candidate_name": row[4],
                "candidate_form": row[5],
                "treatment": row[6],
                "grade": row[7],
                "condition": row[8],
                "size_dimensions": row[9],
                "volume_estimate": row[10],
                "notes": row[11],
                "output_type": row[12],
                "produced_by_products": produced_by,
            }
        )
    return candidates


def build_prompt(
    prompt_template: str,
    csv_company: dict[str, str],
    db_company: dict[str, Any],
    products: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> str:
    payload = {
        "target_company": {
            "csv_company_name": csv_company.get("Company Name", ""),
            "csv_website": csv_company.get("Website URL", ""),
            "nace_bel_code": csv_company.get("NACE-BEL Code", ""),
            "standard_sector": csv_company.get("Standard Sector", ""),
            "persona": csv_company.get("Persona", ""),
            "persona_rationale": csv_company.get("Persona Rationale", ""),
            "address": csv_company.get("Address", ""),
            "database_company": db_company,
            "products_and_services": products,
        },
        "candidate_material_outputs": candidates,
    }
    return (
        f"{prompt_template}\n\n"
        "# TARGET COMPANY AND CANDIDATE SUPPLY POOL\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
    )


def safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value.strip())
    return cleaned.strip("_") or "company"


def run_matching(args: argparse.Namespace) -> Path:
    if args.n < 1:
        raise ValueError("--n must be >= 1")
    if args.candidate_limit < 1:
        raise ValueError("--candidate-limit must be >= 1")

    context_dir = args.context_dir.expanduser().resolve()
    csv_path = context_dir / "Database Aalst - Sheet1.csv"
    prompt_path = context_dir / "material_match_prompt.txt"
    schema_path = context_dir / "material_match_output.schema.json"
    db_path = args.db.expanduser().resolve()

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt not found: {prompt_path}")
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    csv_companies = read_companies(csv_path, args.n, args.manufacturers)
    if not csv_companies:
        raise RuntimeError("No company rows found in CSV for the requested filters.")

    prompt_template = read_text_file(prompt_path)
    schema = resolve_local_schema_refs(read_json_file(schema_path), schema_path.parent)

    client = None
    effective_model = args.model
    if not args.dry_run:
        try:
            from google import genai
        except ImportError as exc:
            raise RuntimeError(
                "Missing dependency 'google-genai'. Run: pip install -r requirements.txt"
            ) from exc
        api_key = load_api_key()
        try:
            client = genai.Client(
                api_key=api_key,
                http_options={"api_version": args.api_version},
            )
        except TypeError:
            client = genai.Client(api_key=api_key)
        effective_model = resolve_model_name(client, args.model)

    report: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "model": effective_model,
        "source_csv": str(csv_path),
        "source_db": str(db_path),
        "filters": {
            "n": args.n,
            "manufacturers": args.manufacturers,
            "candidate_limit": args.candidate_limit,
        },
        "companies": [],
    }

    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        for index, csv_company in enumerate(csv_companies, start=1):
            db_company = fetch_db_company(
                connection,
                csv_company.get("Company Name", ""),
                csv_company.get("Website URL", ""),
            )
            company_record: dict[str, Any] = {
                "index": index,
                "csv_company_name": csv_company.get("Company Name", ""),
                "csv_website": csv_company.get("Website URL", ""),
                "matched_in_database": db_company is not None,
            }
            if db_company is None:
                company_record["error"] = "Company not found in database by name+website matching."
                company_record["matches"] = []
                report["companies"].append(company_record)
                print(f"Skipped {index}/{len(csv_companies)}: {company_record['csv_company_name']} (not in DB)")
                continue

            products = fetch_products(connection, int(db_company["company_id"]))
            candidates = fetch_candidate_outputs(
                connection,
                int(db_company["company_id"]),
                args.candidate_limit,
            )
            prompt = build_prompt(prompt_template, csv_company, db_company, products, candidates)

            company_record["database_company"] = db_company
            company_record["products_and_services"] = products
            company_record["candidate_pool_size"] = len(candidates)

            if args.dry_run:
                company_record["matches"] = []
                company_record["prompt"] = prompt
            else:
                response_text = call_gemini(
                    client=client,
                    model=effective_model,
                    prompt=prompt,
                    schema=schema,
                )
                parsed = parse_json_response(response_text)
                company_record["raw_response"] = response_text
                if isinstance(parsed, dict) and isinstance(parsed.get("matches"), list):
                    company_record["matches"] = parsed["matches"]
                else:
                    company_record["matches"] = []
                    company_record["parse_error"] = "Model response was not valid JSON matching the expected schema."

            report["companies"].append(company_record)
            print(f"Processed {index}/{len(csv_companies)}: {company_record['csv_company_name']}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    suffix = "manufacturers" if args.manufacturers else "all"
    output_path = args.output_dir / f"material_matches_n{args.n}_{suffix}_{timestamp}.json"
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def main() -> None:
    args = parse_args()
    output_path = run_matching(args)
    print(f"Done. Output written to: {output_path}")


if __name__ == "__main__":
    main()
