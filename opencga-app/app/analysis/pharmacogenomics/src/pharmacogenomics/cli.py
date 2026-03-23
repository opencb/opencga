"""Command-line interface for pharmacogenomics analysis tool."""

import argparse
import json
import logging
from pathlib import Path

from pharmacogenomics import __version__


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="pharmacogenomics",
        description="Pharmacogenomics analysis tool supporting OpenArray and NGS pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command", help="Analysis pipeline to run")

    # Shared arguments for both commands
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument(
        "--outdir", type=str, default=".", help="Output directory for results (default: current directory)",
    )
    shared.add_argument("--config", type=str, default=None, help="Path to config.json override")
    shared.add_argument("--annotate", action="store_true", help="Run CPIC annotation on results")
    shared.add_argument(
        "--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level",
    )

    # --- openarray command ---
    oa_parser = subparsers.add_parser(
        "openarray", parents=[shared], help="Run PGx analysis from ThermoFisher OpenArray genotyping data",
    )
    oa_parser.add_argument("--snv-file", type=str, required=True, help="SNV genotyping file (ThermoFisher export)")
    oa_parser.add_argument("--cnv-file", type=str, default=None, help="CNV results file (optional)")
    oa_parser.add_argument("--translation-file", type=str, required=True, help="Translation table file")
    oa_parser.add_argument("--rename-file", type=str, default=None, help="Allele rename file (optional)")
    oa_parser.add_argument(
        "--compare-to", type=str, default=None,
        help="Path to TrueMark detailed results file for comparison benchmark",
    )

    # --- ngs command ---
    ngs_parser = subparsers.add_parser(
        "ngs", parents=[shared], help="Run PGx analysis from NGS VCF data using PharmCAT",
    )
    ngs_parser.add_argument("--vcf-file", type=str, required=True, help="Input VCF file (GRCh38)")
    ngs_parser.add_argument(
        "--pharmcat", type=str, default=None, help="Path to PharmCAT JAR (uses Docker if not provided)",
    )

    return parser.parse_args(argv)


def configure_logger(outdir: Path, log_level: str) -> logging.Logger:
    """Configure logger with console and file handlers.

    Console handler uses a simple format at the requested log level.
    File handler logs DEBUG and above to <outdir>/app.log with timestamps.
    """
    logger = logging.getLogger("pharmacogenomics")
    logger.setLevel(logging.DEBUG)

    # File handler — verbose, logs everything to outdir/app.log
    file_handler = logging.FileHandler(str(outdir / "app.log"))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    # Console handler — simple format at the requested level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def run_openarray(args, config, logger) -> None:
    """Run OpenArray PGx pipeline."""
    outdir = Path(args.outdir)

    from pharmacogenomics.core.allele_typer import AlleleTyper

    typer = AlleleTyper()

    # Parse translation file
    logger.info("Parsing translation file: %s", args.translation_file)
    typer.parse_translation_file(Path(args.translation_file))

    # Parse CNV file if provided
    if args.cnv_file:
        logger.info("Parsing CNV file: %s", args.cnv_file)
        typer.parse_cnv_file(Path(args.cnv_file))

    # Parse rename file if provided
    if args.rename_file:
        logger.info("Parsing rename file: %s", args.rename_file)
        typer.parse_rename_file(Path(args.rename_file))

    # Run allele typing
    logger.info("Running allele typing on: %s", args.snv_file)
    results = typer.build_allele_typer_results(Path(args.snv_file))

    # Apply renames if loaded
    typer.apply_renames(results)

    logger.info("Allele typing produced %d sample results", len(results))

    # Annotate if requested
    if args.annotate:
        logger.info("Running CPIC annotation...")
        from pharmacogenomics.core.annotator import CpicAnnotator

        annotator = CpicAnnotator(config.cpic_base_url)
        annotator.annotate_results(results)

    # Export results and summaries
    from pharmacogenomics.core.models import build_summary

    for result in results:
        output_file = outdir / f"{result.sample_id}.json"
        with open(output_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)

        summary = build_summary(result)
        summary_file = outdir / f"{result.sample_id}_summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary.to_dict(), f, indent=2)

        logger.info("Written: %s, %s", output_file, summary_file)

    # Run comparison benchmark if requested
    if args.compare_to:
        from pharmacogenomics.core.comparator import ComparisonReport

        report = ComparisonReport()
        report_file = outdir / "comparison_report.txt"
        report.run(results, Path(args.compare_to), report_file)
        logger.info("Comparison report written to: %s", report_file)

    logger.info("OpenArray pipeline complete: %d samples", len(results))


def run_ngs(args, config, logger) -> None:
    """Run NGS PGx pipeline using PharmCAT."""
    outdir = Path(args.outdir)

    from pharmacogenomics.core.pharmcat_runner import PharmcatRunner

    runner = PharmcatRunner(
        pharmcat_path=args.pharmcat,
        docker_image=config.pharmcat_docker_image,
        timeout=config.pharmcat_timeout,
    )

    # Run PharmCAT
    logger.info("Running PharmCAT on: %s", args.vcf_file)
    pharmcat_output = runner.run(Path(args.vcf_file), outdir)

    # Parse PharmCAT results into AlleleTyperResult
    from pharmacogenomics.core.pharmcat_parser import PharmcatParser

    parser = PharmcatParser()
    results = parser.parse(pharmcat_output)

    logger.info("PharmCAT produced %d sample results", len(results))

    # Annotate if requested
    if args.annotate:
        logger.info("Running CPIC annotation...")
        from pharmacogenomics.core.annotator import CpicAnnotator

        annotator = CpicAnnotator(config.cpic_base_url)
        annotator.annotate_results(results)

    # Export results and summaries
    from pharmacogenomics.core.models import build_summary

    for result in results:
        output_file = outdir / f"{result.sample_id}.json"
        with open(output_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)

        summary = build_summary(result)
        summary_file = outdir / f"{result.sample_id}_summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary.to_dict(), f, indent=2)

        logger.info("Written: %s, %s", output_file, summary_file)

    logger.info("NGS pipeline complete: %d samples", len(results))


def run(args) -> None:
    from pharmacogenomics.config import load_config

    outdir = Path(args.outdir) if hasattr(args, "outdir") else Path(".")
    outdir.mkdir(parents=True, exist_ok=True)

    logger = configure_logger(outdir, args.log_level)
    config = load_config(args.config if hasattr(args, "config") else None)

    if args.command == "openarray":
        run_openarray(args, config, logger)
    elif args.command == "ngs":
        run_ngs(args, config, logger)
    else:
        parse_args(["--help"])


def main(argv=None) -> None:
    args = parse_args(argv)
    run(args)


if __name__ == "__main__":
    main()
