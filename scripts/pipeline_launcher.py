#!/usr/bin/env python3

import argparse
import subprocess
import sys
from pathlib import Path

from output_paths import DEFAULT_DB_PATH, GEMINI_PROFILES_DIR


STEP_DEFINITIONS = {
    1: "Generate company profiles with Gemini",
    2: "Create or update the SQLite database schema",
    3: "Import generated profile JSON files into the database",
    4: "Match company material outputs to candidate buyers",
}

MANUFACTURER_EXPLANATION = (
    "Manufacturer filter (`--manufacturers`): when enabled, only companies with "
    "NACE-BEL Code `C` are processed. In the source CSV, that means manufacturing "
    "companies only. When disabled, all companies are considered."
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Interactive launcher for the full material-flow pipeline."
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Fail instead of prompting when no interactive terminal is available.",
    )
    return parser.parse_args()


def print_header() -> None:
    print("Material Flow Pipeline")
    print("======================")
    print()
    print("Available steps:")
    for index, label in STEP_DEFINITIONS.items():
        print(f"{index}. {label}")
    print()


def prompt_choice(prompt: str, allowed: set[str]) -> str:
    while True:
        value = input(prompt).strip()
        if value in allowed:
            return value
        print(f"Please enter one of: {', '.join(sorted(allowed))}")


def prompt_yes_no(prompt: str, default: bool) -> bool:
    suffix = "[Y/n]" if default else "[y/N]"
    while True:
        value = input(f"{prompt} {suffix}: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Please answer yes or no.")


def prompt_positive_int(prompt: str, default: int | None = None) -> int:
    while True:
        suffix = f" [{default}]" if default is not None else ""
        raw_value = input(f"{prompt}{suffix}: ").strip()
        if not raw_value and default is not None:
            return default
        try:
            value = int(raw_value)
        except ValueError:
            print("Please enter a whole number greater than or equal to 1.")
            continue
        if value < 1:
            print("Please enter a whole number greater than or equal to 1.")
            continue
        return value


def prompt_path(prompt: str, default: Path | None = None, must_exist: bool = True) -> Path:
    while True:
        suffix = f" [{default}]" if default is not None else ""
        raw_value = input(f"{prompt}{suffix}: ").strip()
        chosen = Path(raw_value).expanduser() if raw_value else default
        if chosen is None:
            print("A path is required.")
            continue
        resolved = chosen.resolve()
        if must_exist and not resolved.exists():
            print(f"Path does not exist: {resolved}")
            continue
        return resolved


def find_latest_profiles_dir() -> Path | None:
    if not GEMINI_PROFILES_DIR.exists():
        return None
    candidates = sorted(
        path for path in GEMINI_PROFILES_DIR.iterdir()
        if path.is_dir() and path.name.startswith("gemini_profiles_n")
    )
    if not candidates:
        return None
    return candidates[-1]


def explain_common_inputs() -> tuple[int, bool]:
    print()
    print("Common inputs for steps 1 and 4:")
    print("- `--n` controls how many rows are read from the source CSV.")
    print("  If manufacturer filtering is enabled, it means the first N manufacturing rows.")
    print(f"- {MANUFACTURER_EXPLANATION}")
    print()
    n = prompt_positive_int("How many companies should be processed", default=5)
    manufacturers_only = prompt_yes_no(
        "Limit processing to manufacturers only",
        default=True,
    )
    return n, manufacturers_only


def build_command(step: int, n: int | None, manufacturers_only: bool | None, json_dir: Path | None) -> list[str]:
    python_executable = sys.executable
    if step == 1:
        if n is None or manufacturers_only is None:
            raise ValueError("Step 1 requires n and manufacturer filter settings.")
        command = [
            python_executable,
            "scripts/create_company_jsons.py",
            "--n",
            str(n),
        ]
        if manufacturers_only:
            command.append("--manufacturers")
        return command
    if step == 2:
        return [python_executable, "scripts/create_company_material_flows_db.py"]
    if step == 3:
        if json_dir is None:
            raise ValueError("Step 3 requires an input JSON directory.")
        return [
            python_executable,
            "scripts/import_company_material_flows_json.py",
            str(json_dir),
        ]
    if step == 4:
        if n is None or manufacturers_only is None:
            raise ValueError("Step 4 requires n and manufacturer filter settings.")
        command = [
            python_executable,
            "scripts/match_company_materials.py",
            "--n",
            str(n),
        ]
        if manufacturers_only:
            command.append("--manufacturers")
        return command
    raise ValueError(f"Unknown step: {step}")


def run_step(step: int, n: int | None, manufacturers_only: bool | None, json_dir: Path | None) -> Path | None:
    label = STEP_DEFINITIONS[step]
    command = build_command(step, n, manufacturers_only, json_dir)
    print()
    print(f"Running step {step}: {label}")
    print("Command:", " ".join(command))
    subprocess.run(command, check=True)
    if step == 1:
        latest_dir = find_latest_profiles_dir()
        if latest_dir is not None:
            print(f"Latest generated profile directory: {latest_dir}")
        return latest_dir
    return json_dir


def choose_run_mode() -> str:
    print("Choose how to run the pipeline:")
    print("1. Full pipeline")
    print("2. Single step only")
    print("3. Start from a chosen step and continue to the end")
    print("4. Exit")
    return prompt_choice("Selection [1-4]: ", {"1", "2", "3", "4"})


def choose_step(prompt_text: str) -> int:
    print()
    for index, label in STEP_DEFINITIONS.items():
        print(f"{index}. {label}")
    print()
    return int(prompt_choice(prompt_text, {"1", "2", "3", "4"}))


def collect_inputs_for_steps(steps: list[int]) -> tuple[int | None, bool | None, Path | None]:
    n: int | None = None
    manufacturers_only: bool | None = None
    json_dir: Path | None = None

    if 1 in steps or 4 in steps:
        n, manufacturers_only = explain_common_inputs()

    if 3 in steps and 1 not in steps:
        latest_profiles_dir = find_latest_profiles_dir()
        print()
        print("Step 3 needs a directory containing the generated company profile JSON files.")
        if latest_profiles_dir is not None:
            print(f"Latest detected profile directory: {latest_profiles_dir}")
        json_dir = prompt_path(
            "Directory to import",
            default=latest_profiles_dir,
            must_exist=True,
        )

    return n, manufacturers_only, json_dir


def run_sequence(steps: list[int]) -> None:
    n, manufacturers_only, json_dir = collect_inputs_for_steps(steps)
    for step in steps:
        effective_json_dir = json_dir
        if step == 3 and effective_json_dir is None:
            effective_json_dir = find_latest_profiles_dir()
            if effective_json_dir is None:
                raise RuntimeError(
                    "No generated profile directory was found. Run step 1 first or provide a directory."
                )
        result_path = run_step(step, n, manufacturers_only, effective_json_dir)
        if step == 1 and result_path is not None:
            json_dir = result_path


def ensure_interactive_terminal(non_interactive: bool) -> None:
    if sys.stdin.isatty() and sys.stdout.isatty():
        return
    message = (
        "This launcher is interactive. Start the container with a TTY, for example "
        "`docker run -it <image>`."
    )
    raise SystemExit(message if non_interactive or not sys.stdin.isatty() else "Interactive terminal required.")


def main() -> None:
    args = parse_args()
    ensure_interactive_terminal(args.non_interactive)
    print_header()
    selection = choose_run_mode()
    if selection == "4":
        print("Exiting.")
        return
    if selection == "1":
        run_sequence([1, 2, 3, 4])
    elif selection == "2":
        step = choose_step("Step to run [1-4]: ")
        run_sequence([step])
    else:
        step = choose_step("Start from step [1-4]: ")
        run_sequence(list(range(step, 5)))
    print()
    print(f"Pipeline finished. Database path: {DEFAULT_DB_PATH}")


if __name__ == "__main__":
    main()
