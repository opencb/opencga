#!/usr/bin/env python3
import argparse
import json
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

from processing import QualityControl, Alignment, VariantCalling, PrepareReferenceIndexes, AffymetrixMicroarray

# Define global constants
VALID_STEPS = ["quality-control", "alignment", "variant-calling"]
SENTINEL = "DONE"

# Define global logger
outdir = None
logger = None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Pipeline runner", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", required=True)

    ## --- index command ---
    prepare_parser = subparsers.add_parser("prepare", help="Index the reference genome")
    prepare_parser.add_argument("-r", "--reference-genome", required=True, help="Path or URL to the reference genome in FASTA format")
    prepare_parser.add_argument("-i", "--indexes", default="bwa,bwa-mem2,minimap2,bowtie2,hisat2,affy", help="Comma-separated list of indexes to prepare (reference-genome,bwa,bwa-mem2,minimap2). Reference-genome is always executed.")
    prepare_parser.add_argument("-c", "--clean", action="store_true", help="Clean existing directory before running")
    prepare_parser.add_argument("-l", "--log-level", default="info", choices=["debug", "info", "warning", "error"], help="Set console logging level")
    prepare_parser.add_argument("-o", "--outdir", required=True, help="Base output directory, index subfolders will be created")

    ## --- genomics command ---
    run_parser = subparsers.add_parser("genomics", help="Align reads to reference genome and call variants")
    run_parser.add_argument("-p", "--pipeline", required=True, help="Pipeline JSON file to execute")
    run_parser.add_argument("-s", "--samples", help="Samples to be processed. Accepted format: sample_id::file1,file2::somatic(0/1)::role(F/M/C/U)")
    run_parser.add_argument("--samples-file", help="File containing samples to be processed, one per line. Accepted format: sample_id::file1,file2::somatic(0/1)::role(F/M/C/U)")
    run_parser.add_argument("-i", "--index-dir", help="Directory containing reference and aligner indexes")
    run_parser.add_argument("--steps", default="quality-control,alignment,variant-calling", help="Pipeline step to execute")
    run_parser.add_argument("--overwrite", action="store_true", help="Force re-run even if step previously completed")
    run_parser.add_argument("-c", "--clean", action="store_true", help="Clean existing directory before running")
    run_parser.add_argument("-l", "--log-level", default="INFO", choices=["debug", "info", "warning", "error"], help="Set console logging level")
    run_parser.add_argument("-o", "--outdir", required=True, help="Base output directory, step subfolders will be created")

    ## --- rna-seq command ---
    # run_parser = subparsers.add_parser("rna-seq", help="Align reads to reference genome")
    # run_parser.add_argument("-p", "--pipeline", help="Pipeline step to execute")
    # run_parser.add_argument("-s", "--samples", help="Input data file or directory")
    # run_parser.add_argument("-i", "--index-dir", help="Input data file or directory")
    # run_parser.add_argument("--steps", default="quality-control,alignment,variant-calling", help="Pipeline step to execute")
    # run_parser.add_argument("--overwrite", action="store_true", help="Force re-run even if step previously completed")
    # run_parser.add_argument("-c", "--clean", action="store_true", help="Clean existing directory before running")
    # run_parser.add_argument("-l", "--log-level", default="INFO", choices=["debug", "info", "warning", "error"], help="Set console logging level")
    # run_parser.add_argument("-o", "--outdir", required=True, help="Base output directory, step subfolders will be created")

    ## --- affy command ---
    run_parser = subparsers.add_parser("affy", help="Process Affymetrix microarray data")
    run_parser.add_argument("-p", "--pipeline", help="Pipeline JSON file to execute")
    run_parser.add_argument("-s", "--samples", help="Samples to be processed. Accepted format: sample_id::file1,file2::somatic(0/1)::role(F/M/C/U)")
    run_parser.add_argument("-d", "--data-dir", help="Input directory for data files. Accepted format: CEL")
    run_parser.add_argument("--chip-type", help="Affymetrix chip type (e.g., 'Axiom_KU8', 'HG-U133_Plus_2')")
    run_parser.add_argument("-i", "--index-dir", help="Directory containing reference index and APT configuration files")
    run_parser.add_argument("--steps", default="quality-control,genotype", help="Pipeline step to execute")
    run_parser.add_argument("--overwrite", action="store_true", help="Force re-run even if step previously completed")
    run_parser.add_argument("-c", "--clean", action="store_true", help="Clean existing directory before running")
    run_parser.add_argument("-l", "--log-level", default="INFO", choices=["debug", "info", "warning", "error"], help="Set console logging level")
    run_parser.add_argument("-o", "--outdir", required=True, help="Base output directory, step subfolders will be created")

    return parser.parse_args(argv)

def prepare_step_dir(base_outdir: Path, step: str):
    step_dir = base_outdir / step
    step_dir.mkdir(parents=True, exist_ok=True)
    return step_dir

def mark_completed(step_dir: Path):
    (step_dir / SENTINEL).write_text(f"Completed: {datetime.now().isoformat()}Z\n")

def is_completed(step_dir: Path):
    return (step_dir / SENTINEL).is_file()

def create_output_dir(args):
    global outdir

    ## Get absolute path of outdir and create if not exists
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def configure_logger(args):
    global outdir, logger

    ## Determine log level from args
    outdir = Path(args.outdir).resolve()
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)

    ## Create logger
    logger = logging.getLogger("ngs_pipeline")
    logger.setLevel(logging.DEBUG)  # master level, controls what gets through to handlers

    ## File handler
    file_handler = logging.FileHandler(str(outdir / "app.log"))
    file_handler.setLevel(logging.DEBUG)  # log everything (DEBUG+) to file
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    ## Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)  # log_level and above to console
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    ## Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def clean(args):
    ## 1. Check clean parameter and directory is not empty
    if args.clean and any(outdir.iterdir()):
        # logger.info(f"Cleaning existing output directory: {outdir}")
        ## removing all contents of outdir
        for item in outdir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            elif item.is_file():
                item.unlink()
        # logger.info("Output directory cleaned.")
        return 0
    return None


def prepare(args):
    ## 1. Validate the index types
    indexes = args.aligner_indexes.split(",")
    for idx in indexes:
        if idx not in ["reference-genome", "bwa", "bwa-mem2", "minimap2", "bowtie2", "hisat2"]:
            logger.error(f"ERROR: Unsupported index type specified: {idx}")
            return 1

    ## 2. Ensure reference genome is created. We MUST add it if not present
    if "reference-genome" not in indexes:
        indexes.insert(0, "reference-genome")

    ## 3. Calculate indexes and prepare reference
    prepare_reference = PrepareReferenceIndexes(reference_genome=args.reference_genome, indexes=indexes, output=outdir, logger=logger)
    return prepare_reference.execute()

def genomics(args):
    ## 1. Load pipeline configuration
    pipeline_path = Path(args.pipeline).resolve()
    if not pipeline_path.is_file():
        logger.error(f"ERROR: 'pipeline' configuration file not found: {pipeline_path}")
        return 1
    with pipeline_path.open("r", encoding="utf-8") as fh:
        pipeline = json.load(fh)
    if not isinstance(pipeline, dict):
        logger.error(f"ERROR: 'pipeline' configuration is not a JSON object: {pipeline_path}")
        return 1

    ## 2. Set reference in pipeline configuration if provided
    if args.index_dir:
        pipeline.get("input", {}).update({"indexDir": args.index_dir})

    ## 3. Set samples in pipeline configuration if provided
    samples = []
    # 3.1 Check if samples provided via --samples or --samples-file
    if args.samples:
        logger.debug(f"Loading samples from command line: {args.samples}")
        samples = args.samples.split(";")
    elif args.samples_file:
        samples_file_path = Path(args.samples_file).resolve()
        if not samples_file_path.is_file():
            logger.error(f"ERROR: 'samples-file' not found: {samples_file_path}")
            return 1
        logger.debug(f"Loading samples from file: {args.samples_file}")
        with samples_file_path.open("r", encoding="utf-8") as sfh:
            for line in sfh:
                line = line.strip()
                if line and not line.startswith("#"):
                    samples.append(line)

    # 3.2 If samples provided, parse and set in pipeline configuration object
    if samples:
        # Reset pipeline samples list
        pipeline.get("input", {}).update({"samples": []})
        for sample in samples:
            ## Parse sample string, format: sample_id::file1,file2::somatic(0/1)::role(T/N/U)
            parts = sample.split("::")
            parts += [""] * (4 - len(parts))  # complete 'parts' to length 4
            sample_id, files, somatic, role = parts[0], parts[1].split(","), parts[2] or 0, parts[3] or "U"
            logger.debug(f"Sample '{sample_id}' files='{files}' somatic='{somatic}' role='{role}'")
            ## Append to existing samples list
            sample_list = pipeline.get("input", {}).get("samples", []) + [{"id": sample_id, "files": files, "somatic": somatic, "role": role}]
            pipeline.get("input", {}).update({"samples": sample_list})
        logger.debug(f"Input set in pipeline configuration: {pipeline.get('input')}")

    # 3.3 Use those in pipeline configuration. If none provided, report error
    logger.debug("No samples provided via --samples or --samples-file; using pipeline configuration samples.")
    samples = pipeline.get("input", {}).get("samples", [])
    if not samples:
        logger.error("ERROR: No input files specified in pipeline configuration or via --samples")
        return 1

    ## 4. Check input files exist
    for sample in samples:
        for f in sample.get("files", []):
            fpath = Path(f).resolve()
            if not fpath.is_file():
                logger.error(f"ERROR: Input file for sample '{sample.get('id')}' not found: {fpath}")
                return 1
    logger.debug(f"All input files verified for {len(samples)} samples.")

    ## 5. Prepare first step execution
    steps = args.steps.split(",")
    ## Check that all steps are in VALID_STEPS
    for step in steps:
        if step not in VALID_STEPS:
            logger.error(f"ERROR: Invalid step specified: {step}. Valid steps are: {', '.join(VALID_STEPS)}")
            return 1

    ## 6. Execute steps in order
    for step in steps:
        ## 6.1. Prepare step directory and implementation
        step_dir = prepare_step_dir(outdir, step)
        impl = None
        logger.debug(f"Starting step='{step}' overwrite={args.overwrite}")
        match step:
            case "quality-control":
                impl = QualityControl(pipeline=pipeline, output=step_dir, logger=logger)
            case "alignment":
                impl = Alignment(pipeline=pipeline, output=step_dir, logger=logger)
            case "variant-calling":
                impl = VariantCalling(pipeline=pipeline, output=step_dir, logger=logger)
            case _:
                logger.debug("Done with all steps.")
                return 1

        ## 6.2. Execute step if implementation exists
        if impl:
            if not is_completed(step_dir):
                logger.debug(f"Executing step: '{step}'; No existing completion detected")
                impl.execute()
                mark_completed(step_dir)
            else:
                if args.overwrite:
                    logger.debug(
                        "Executing step: '{step}'; Existing completion detected, but overwriting as requested.")
                    impl.execute()
                    mark_completed(step_dir)
                else:
                    logger.warning(f"Step '{step}' already completed. Use --overwrite to force.")

def rna_seq(args):
    pass

def affy(args):
    ## 1. Load pipeline configuration
    pipeline_path = Path(args.pipeline).resolve()
    if not pipeline_path.is_file():
        logger.error(f"ERROR: 'pipeline' configuration file not found: {pipeline_path}")
        return 1
    with pipeline_path.open("r", encoding="utf-8") as fh:
        pipeline = json.load(fh)
    if not isinstance(pipeline, dict):
        logger.error(f"ERROR: 'pipeline' configuration is not a JSON object: {pipeline_path}")
        return 1

    ## 2. Set reference in pipeline configuration if provided
    if args.index_dir:
        pipeline.get("input", {}).update({"indexDir": args.index_dir})



    ## 3. Set input in pipeline configuration if provided
    if args.samples:
        samples = []
        # This can be a string or a directory path
        if Path(args.samples).is_dir():
            samples_path = Path(args.samples).resolve()
            logger.debug(f"Loading samples from file/directory: {args.samples}")
            # Sample are *.CEL file names in the directory
            cel_files = list(samples_path.glob("*.CEL"))
            for cel_file in cel_files:
                sample_id = cel_file.stem
                samples.append(f"{sample_id}::{str(cel_file)}::0::U")
            logger.debug(f"Parsed sample IDs: {samples}")
        else:
            samples = samples.split(";")

        ## Reset pipeline samples list
        pipeline.get("input", {}).update({"samples": []})
        # samples = args.samples.split(";")
        for sample in samples:
            ## Parse sample string, format: sample_id::file1,file2::somatic(0/1)::role(T/N/U)
            parts = sample.split("::")
            parts += [""] * (4 - len(parts))  # complete 'parts' to length 4
            sample_id, files, somatic, role = parts[0], parts[1].split(","), parts[2] or 0, parts[3] or "U"
            logger.debug(f"Sample '{sample_id}' files='{files}' somatic='{somatic}' role='{role}'")
            ## Append to existing samples list
            sample_list = pipeline.get("input", {}).get("samples", []) + [
                {"id": sample_id, "files": files, "somatic": somatic, "role": role}]
            pipeline.get("input", {}).update({"samples": sample_list})
        logger.info("Input set in pipeline configuration: %s", json.dumps(pipeline.get("input", {}), ensure_ascii=False))

    ## 4. Check input files exist
    samples = pipeline.get("input", {}).get("samples", [])
    if not samples:
        logger.error("ERROR: No input files specified in pipeline configuration or via --samples")
        return 1
    for sample in samples:
        for f in sample.get("files", []):
            fpath = Path(f).resolve()
            if not fpath.is_file():
                logger.error(f"ERROR: Input file for sample '{sample.get('id')}' not found: {fpath}")
                return 1
    logger.debug(f"All input files verified for {len(samples)} samples.")



    affy_impl = AffymetrixMicroarray(pipeline=pipeline, output=outdir, logger=logger)
    affy_impl.execute()
    return 0

def main(argv=None):
    args = parse_args(argv)

    ## 1. Get absolute path of outdir and create if not exists
    # global outdir
    # outdir = Path(args.outdir).resolve()
    # outdir.mkdir(parents=True, exist_ok=True)
    create_output_dir(args)

    ## 2. Handle --clean option
    clean(args)

    ## 3. Configure global logger
    configure_logger(args)

    # 4. Run the appropriate command
    logger.debug(f"Executing command '{args.command}' in output directory '{outdir}'")
    match args.command:
        case "prepare":
            return prepare(args)
        case "genomics":
            return genomics(args)
        case "rna-seq":
            return rna_seq(args)
        case "affy":
            return affy(args)
        case _:
            logger.error(f"Unknown command: {args.command}")
            return 1


if __name__ == "__main__":
    sys.exit(main())
