# Pharmacogenomics Analysis Tool

OpenCGA pharmacogenomics (PGx) analysis tool that infers star alleles from genotyping data and annotates them with CPIC drug recommendations. Supports two technologies under a unified pipeline:

- **OpenArray**: ThermoFisher OpenArray TrueMark genotyping data (SNV + CNV)
- **NGS**: VCF files processed through PharmCAT

Both pipelines produce the same `AlleleTyperResult` JSON output format, enabling consistent downstream annotation and reporting.

## Installation

```bash
cd opencga-app/app/analysis/pharmacogenomics
uv sync
```

## Usage

### OpenArray

Infer star alleles from ThermoFisher OpenArray genotyping files:

```bash
source .venv/bin/activate

python3 ./src/pharmacogenomics/cli.py openarray \
  --snv-file /home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark\ \(con\ CNV\)/FQU71/20260318_FQU71_DO_20260318_FQU71_DO_Genotyping_18-03-2026-122836.txt \
  --cnv-file /home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark\ \(con\ CNV\)/FQU71/20260318_FQU71_DO_20260318_FQU71_DO_Copy_Number_Variation_Result_multi_plate_18-03-2026-122836.txt \
  --translation-file /home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark\ \(con\ CNV\)/Pharmacogenetics/PGX_SNP_CNV_128_OA_translation_RevC\ tab.txt \
  --rename-file /home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark\ \(con\ CNV\)/aux_files/Nomenclatura_variante_personalizada_XettaBase.txt \
  --outdir /tmp/pgx_result
```

#### OpenArray parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--snv-file` | Yes | SNV genotyping file (ThermoFisher QuantStudio export) |
| `--translation-file` | Yes | Translation table mapping assays to star alleles |
| `--cnv-file` | No | CNV results file for copy number-aware diplotype calling |
| `--rename-file` | No | Allele rename file to map rs-based names to HGVS nomenclature |
| `--compare-to` | No | TrueMark detailed results file for comparison benchmark |
| `--annotate` | No | Run CPIC annotation on results |

### NGS

Infer star alleles from VCF files using PharmCAT:

```bash
source .venv/bin/activate

python3 ./src/pharmacogenomics/cli.py ngs \
  --vcf-file sample.vcf \
  --outdir /tmp/pgx_result
```

#### NGS parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--vcf-file` | Yes | Input VCF file (GRCh38 assembly required) |
| `--pharmcat` | No | Path to PharmCAT JAR file. Uses Docker image `pgkb/pharmcat` if not provided |
| `--annotate` | No | Run CPIC annotation on results |

### Shared parameters

| Parameter | Description |
|-----------|-------------|
| `--outdir` | Output directory (default: current directory) |
| `--config` | Path to config.json override |
| `--log-level` | Logging level: DEBUG, INFO, WARNING, ERROR (default: INFO) |

## Output

Each sample produces a JSON file (`<sample_id>.json`) containing:

- `sampleId`: sample identifier
- `source`: `"openarray"` or `"ngs"`
- `alleleTyperResults`: list of per-gene star allele results with diplotype calls
- `genotypes`: per-variant genotype data
- `additionalInformation`: technology-specific metadata (e.g., PharmCAT version for NGS)

When `--annotate` is used, each gene result includes CPIC diplotype annotation with phenotype classification and drug recommendations.

When `--compare-to` is used (OpenArray only), a `comparison_report.txt` is generated showing per-gene accuracy against TrueMark official results.

## Testing

```bash
uv run pytest -v
uv run ruff check src/
```
