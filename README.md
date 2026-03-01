# Material Flow Pipeline Container

This container runs the material-flow pipeline with editable prompts/input files and persistent output folders.

## Requirements

- Docker or a compatible container runtime
- A Gemini API key in an env file, for example:

```text
GEMINI_API_KEY=your_key_here
```

## Run

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force -Path .\runtime\context | Out-Null
New-Item -ItemType Directory -Force -Path .\runtime\output | Out-Null

docker run --rm -it `
  --env-file .env `
  -v "${PWD}\runtime\context:/data/context" `
  -v "${PWD}\runtime\output:/data/output" `
  hansweytjens/circular_match:20260301
```

Linux or WSL:

```bash
mkdir -p ./runtime/context ./runtime/output

docker run --rm -it \
  --env-file .env \
  -v "$(pwd)/runtime/context:/data/context" \
  -v "$(pwd)/runtime/output:/data/output" \
  hansweytjens/circular_match:20260301
```

## Folders

- `/data/context`: editable prompts, schemas, and source CSV
- `/data/output`: generated JSON files and SQLite database

On first start, the container copies the default context files into `/data/context` only if that mounted folder is empty.

If you already have files in the mounted context folder, they are kept as-is and are not overwritten by newer container versions.

If you want the defaults from a newer image, start again with an empty mounted context folder.

## Customization

To change prompts or input data, edit the files in the mounted context folder on your machine:

- `Master Prompt.txt`
- `material_match_prompt.txt`
- JSON schema files
- `Database Aalst - Sheet1.csv`

## Output

The container writes results under the mounted output folder:

- `gemini_profiles`
- `material_matches`
- `databases`
