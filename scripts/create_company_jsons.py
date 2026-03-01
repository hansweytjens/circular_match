#!/usr/bin/env python3

import argparse
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from output_paths import GEMINI_PROFILES_DIR


DEFAULT_MODEL = "gemini-3-flash-preview"
DEFAULT_API_VERSION = "v1beta"
MODEL_ALIASES = {
    "gemini-3-flash": "gemini-3-flash-preview",
    "gemini-3-pro": "gemini-3-pro-preview",
    "gemini-3.1-pro": "gemini-3.1-pro-preview",
    "gemini-3-pro-image": "gemini-3-pro-image-preview",
}
PREFERRED_MODELS = [
    "gemini-3-flash-preview",
    "gemini-3-pro-preview",
    "gemini-3.1-pro-preview",
    "gemini-3-pro-image-preview",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-1.5-flash",
]
DEFAULT_CONTEXT_DIR = Path("/workspace/context/active")
DEFAULT_OUTPUT_DIR = GEMINI_PROFILES_DIR


def load_dotenv_file(dotenv_path: Path = Path(".env")) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def load_api_key() -> str:
    load_dotenv_file()
    candidate_keys = [
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "GOOGLE_GENAI_API_KEY",
        "GEMINI API KEY",
    ]
    api_key = None
    for key in candidate_keys:
        value = os.getenv(key)
        if value:
            api_key = value
            break
    if not api_key:
        raise RuntimeError(
            "Missing Gemini API key. Set one of: GEMINI_API_KEY, GOOGLE_API_KEY, "
            "GOOGLE_GENAI_API_KEY, or GEMINI API KEY in .env"
        )
    return api_key


def read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace").strip()


def read_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_local_schema_refs(schema: Any, base_dir: Path) -> Any:
    if isinstance(schema, dict):
        if "$ref" in schema and isinstance(schema["$ref"], str):
            ref = schema["$ref"]
            if not ref.startswith("#"):
                ref_path = (base_dir / ref).resolve()
                ref_schema = read_json_file(ref_path)
                return resolve_local_schema_refs(ref_schema, ref_path.parent)
        return {
            key: resolve_local_schema_refs(value, base_dir)
            for key, value in schema.items()
        }
    if isinstance(schema, list):
        return [resolve_local_schema_refs(item, base_dir) for item in schema]
    return schema


def load_context_files(context_dir: Path) -> Dict[str, str]:
    context_files: Dict[str, str] = {}
    for path in sorted(context_dir.glob("*.txt")):
        context_files[path.name] = read_text_file(path)
    return context_files


def render_specification_attributes(spec_desc: Dict[str, Any]) -> str:
    lines: List[str] = [
        "For each material flow, capture relevant specifications:",
        "",
        "## Treatment (especially for wood, metals, textiles)",
    ]
    for key, description in spec_desc.get("treatment", {}).items():
        lines.append(f"- {key}: {description}")

    lines.extend(["", "## Grade"])
    for key, description in spec_desc.get("grade", {}).items():
        lines.append(f"- {key}: {description}")

    lines.extend(["", "## Condition"])
    for key, description in spec_desc.get("condition", {}).items():
        lines.append(f"- {key}: {description}")

    size_desc = spec_desc.get("size_dimensions", {}).get(
        "description",
        "Free text describing size range or dimensions.",
    )
    lines.extend(
        [
            "",
            "## Size/Dimensions (when relevant)",
            f"- {size_desc}",
        ]
    )
    return "\n".join(lines)


def read_companies(
    csv_path: Path,
    n: int,
    manufacturers: bool = False,
) -> List[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            nace_code = (row.get("NACE-BEL Code") or "").strip()
            if manufacturers and nace_code != "C":
                continue
            rows.append(row)
            if len(rows) >= n:
                break
    return rows


def build_prompt(
    company: Dict[str, str],
    context_files: Dict[str, str],
    context_dir: Path,
) -> str:
    master_prompt = context_files.get("Master Prompt.txt", "")
    spec_desc_path = context_dir / "material_stream_properties_descriptions.json"
    if spec_desc_path.exists():
        spec_desc = read_json_file(spec_desc_path)
        spec_section = render_specification_attributes(spec_desc)
        master_prompt = master_prompt.replace(
            "{{SPECIFICATION_ATTRIBUTES}}",
            spec_section,
        )

    excluded_context_files = {"Master Prompt.txt"}
    other_context = []
    for name, content in context_files.items():
        if name in excluded_context_files:
            continue
        other_context.append(f"### {name}\n{content}")

    company_json = json.dumps(company, ensure_ascii=False, indent=2)

    return (
        f"{master_prompt}\n\n"
        "# REFERENCE CONTEXT FILES\n"
        + "\n\n".join(other_context)
        + "\n\n# COMPANY TO ANALYZE\n"
        + company_json
        + "\n\nReturn only the final JSON output for this company."
    )


def call_gemini(client: Any, model: str, prompt: str, schema: Any) -> str:
    config = {
        "response_mime_type": "application/json",
        "response_json_schema": schema,
    }
    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=config,
        )
    except TypeError:
        # Some SDK versions use response_schema instead of response_json_schema.
        config["response_schema"] = config.pop("response_json_schema")
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=config,
        )

    if getattr(response, "text", None):
        return response.text

    if hasattr(response, "to_json"):
        return response.to_json()
    return str(response)


def normalize_model_name(name: str) -> str:
    base = name.removeprefix("models/")
    return MODEL_ALIASES.get(base, base)


def model_supports_generation(model_obj: Any) -> bool:
    keys = (
        "supported_actions",
        "supported_generation_methods",
        "supported_methods",
    )
    for key in keys:
        methods = getattr(model_obj, key, None)
        if not methods:
            continue
        joined = " ".join(str(m).lower() for m in methods)
        if "generatecontent" in joined or "generate_content" in joined:
            return True
    return False


def list_generate_models(client: Any) -> List[str]:
    names: List[str] = []
    try:
        models_iter = client.models.list()
    except Exception:
        return names

    for model_obj in models_iter:
        name = getattr(model_obj, "name", None)
        if not name:
            continue
        if model_supports_generation(model_obj):
            names.append(normalize_model_name(name))
            continue
        # Some SDK/API combinations don't expose capability metadata reliably.
        names.append(normalize_model_name(name))
    return sorted(set(names))


def resolve_model_name(client: Any, requested_model: str) -> str:
    requested = normalize_model_name(requested_model)
    available_models = list_generate_models(client)
    if not available_models:
        return requested
    if requested in available_models:
        return requested
    for candidate in PREFERRED_MODELS:
        if candidate in available_models:
            print(
                f"Requested model '{requested_model}' is unavailable. "
                f"Using '{candidate}' instead."
            )
            return candidate
    fallback = available_models[0]
    print(
        f"Requested model '{requested_model}' is unavailable. "
        f"Using '{fallback}' from listed available models."
    )
    return fallback


def parse_json_response(response_text: str) -> Any | None:
    text = response_text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def normalize_produced_by_products(payload: Any) -> Any:
    if isinstance(payload, dict):
        normalized: Dict[str, Any] = {}
        for key, value in payload.items():
            if key == "produced_by_products" and isinstance(value, list):
                names: List[str] = []
                for item in value:
                    if isinstance(item, str):
                        names.append(item)
                    elif isinstance(item, dict):
                        name = item.get("name") or item.get("product_id")
                        if isinstance(name, str) and name:
                            names.append(name)
                normalized[key] = names
            else:
                normalized[key] = normalize_produced_by_products(value)
        return normalized

    if isinstance(payload, list):
        return [normalize_produced_by_products(item) for item in payload]

    return payload


def run_pipeline(
    context_dir: Path,
    model: str,
    api_version: str,
    n: int,
    output_dir: Path,
    dry_run: bool,
    schema_path: Path | None,
    manufacturers: bool,
) -> Path:
    context_files = load_context_files(context_dir)
    csv_path = context_dir / "Database Aalst - Sheet1.csv"
    default_schema_path = context_dir / "company_material_flows.schema.json"
    if not default_schema_path.exists():
        default_schema_path = context_dir / "schema.json"
    resolved_schema_path = schema_path or default_schema_path

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    if "Master Prompt.txt" not in context_files:
        raise FileNotFoundError("Missing Master Prompt.txt in context directory")
    if not resolved_schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {resolved_schema_path}")

    companies = read_companies(csv_path, n, manufacturers=manufacturers)
    if not companies:
        raise RuntimeError("No company rows found in CSV")
    schema = resolve_local_schema_refs(
        read_json_file(resolved_schema_path),
        resolved_schema_path.parent,
    )

    api_key = load_api_key()
    client = None
    effective_model = model
    if not dry_run:
        try:
            from google import genai
        except ImportError as exc:
            raise RuntimeError(
                "Missing dependency 'google-genai'. Run: pip install -r requirements.txt"
            ) from exc
        try:
            client = genai.Client(
                api_key=api_key,
                http_options={"api_version": api_version},
            )
        except TypeError:
            client = genai.Client(api_key=api_key)
        effective_model = resolve_model_name(client, model)

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"gemini_profiles_n{n}_{timestamp}"
    wrote_output = False

    for idx, company in enumerate(companies, start=1):
        prompt = build_prompt(company, context_files, context_dir=context_dir)
        if dry_run:
            response_text = "DRY_RUN: Gemini call skipped."
        else:
            response_text = call_gemini(
                client=client,
                model=effective_model,
                prompt=prompt,
                schema=schema,
            )
        response_json = parse_json_response(response_text)
        if response_json is not None:
            if not wrote_output:
                run_dir.mkdir(parents=True, exist_ok=True)
                wrote_output = True
            response_json = normalize_produced_by_products(response_json)
            company_name = company.get("Company Name", f"company_{idx}").strip()
            safe_name = "".join(
                c if c.isalnum() or c in ("-", "_") else "_" for c in company_name
            )
            company_path = run_dir / f"{idx:03d}_{safe_name}.json"
            company_path.write_text(
                json.dumps(response_json, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        print(
            f"Processed {idx}/{len(companies)}: "
            f"{company.get('Company Name', 'Unknown')}"
        )

    if not wrote_output:
        raise RuntimeError("No valid JSON outputs were generated.")

    return run_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Gemini profiling pipeline for the first n companies in Database Aalst CSV."
    )
    parser.add_argument("--n", type=int, default=1, help="Number of rows to process (default: 1)")
    parser.add_argument(
        "--context-dir",
        type=Path,
        default=DEFAULT_CONTEXT_DIR,
        help="Directory containing prompt/reference files and CSV",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Gemini model name (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--api-version",
        default=DEFAULT_API_VERSION,
        help=f"Gemini API version for SDK calls (default: {DEFAULT_API_VERSION})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for generated company profile folders",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call Gemini; write built prompts and placeholder responses",
    )
    parser.add_argument(
        "--schema-path",
        type=Path,
        default=None,
        help="Path to JSON schema file (default: <context-dir>/schema.json)",
    )
    parser.add_argument(
        "--manufacturers",
        action="store_true",
        help="Only process companies with NACE-BEL Code 'C'",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.n < 1:
        raise ValueError("--n must be >= 1")

    output_path = run_pipeline(
        context_dir=args.context_dir,
        model=args.model,
        api_version=args.api_version,
        n=args.n,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
        schema_path=args.schema_path,
        manufacturers=args.manufacturers,
    )
    print(f"Done. Output written to: {output_path}")


if __name__ == "__main__":
    main()
